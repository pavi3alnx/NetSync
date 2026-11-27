package com.netsync.gui;

import com.netsync.app.DemoServerConfig;
import com.netsync.app.NetSyncServerApplication;
import com.netsync.common.PresenceState;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionView;
import com.netsync.security.CredentialStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Headless, thread-safe lifecycle and monitoring controller for the NetSync GUI server dashboard.
 * <p>
 * Composes the real {@link NetSyncServerApplication} without console prompts, executes all blocking
 * startup/shutdown work on a dedicated single-thread executor, prevents overlapping transitions,
 * and publishes read-only {@link ServerModels.ServerSnapshot} updates through {@link UiEventDispatcher}.
 */
public final class ServerDashboardController implements AutoCloseable {
    public static final int MAX_LOG_ENTRIES = 200;

    public record AccountInput(String username, char[] password) {
        public AccountInput {
            password = password == null ? null : Arrays.copyOf(password, password.length);
        }

        public void clearPassword() {
            if (password != null) {
                Arrays.fill(password, '\0');
            }
        }
    }

    public interface Listener {
        default void onSnapshot(ServerModels.ServerSnapshot snapshot) {}
        default void onLogEntry(ServerModels.ActivityLogEntry entry) {}
    }

    private final UiEventDispatcher dispatcher;
    private final BiFunction<DemoServerConfig, CredentialStore, NetSyncServerApplication> appFactory;
    private final ExecutorService lifecycleExecutor;
    private final Object stateLock = new Object();
    private final ArrayDeque<ServerModels.ActivityLogEntry> activityLog = new ArrayDeque<>(MAX_LOG_ENTRIES);
    private volatile Listener listener;

    private ServerModels.ServerState state = ServerModels.ServerState.STOPPED;
    private NetSyncServerApplication activeApplication;
    private Instant startedAt;
    private int lastTcpPort = ProtocolLimits.TCP_PORT;
    private int lastUdpPort = ProtocolLimits.UDP_PORT;
    private int lastAccountCount = 0;
    private String lastStatusMessage = "Server stopped.";
    private boolean closed;

    public ServerDashboardController(Consumer<Runnable> uiExecutor) {
        this(uiExecutor, NetSyncServerApplication::new);
    }

    public ServerDashboardController(
            Consumer<Runnable> uiExecutor,
            BiFunction<DemoServerConfig, CredentialStore, NetSyncServerApplication> appFactory) {
        this.dispatcher = new UiEventDispatcher(uiExecutor);
        this.appFactory = Objects.requireNonNull(appFactory, "appFactory");
        this.lifecycleExecutor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(8),
                task -> {
                    Thread thread = new Thread(task, "netsync-gui-server-lifecycle");
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
    }

    public void setListener(Listener listener) {
        this.listener = listener;
        emitSnapshot();
    }

    /**
     * Validates configuration, provisions disposable PBKDF2 verifiers in memory, and starts the
     * real {@link NetSyncServerApplication} off the JavaFX thread.
     * Caller password arrays are cleared regardless of outcome.
     */
    public CompletableFuture<ServerModels.ServerSnapshot> startServer(
            boolean insecureDemoAcknowledged,
            int tcpPort,
            int udpPort,
            List<AccountInput> accounts) {
        List<AccountInput> ownedAccounts = copyAndClearInputs(accounts);
        CompletableFuture<ServerModels.ServerSnapshot> result = new CompletableFuture<>();

        synchronized (stateLock) {
            if (closed) {
                clearAll(ownedAccounts);
                result.completeExceptionally(new IllegalStateException("Server controller is closed."));
                return result;
            }
            if (state == ServerModels.ServerState.STARTING
                    || state == ServerModels.ServerState.RUNNING
                    || state == ServerModels.ServerState.STOPPING) {
                clearAll(ownedAccounts);
                result.completeExceptionally(new IllegalStateException("Server is currently " + state + "."));
                return result;
            }
            state = ServerModels.ServerState.STARTING;
            lastTcpPort = tcpPort;
            lastUdpPort = udpPort;
            lastAccountCount = ownedAccounts.size();
            lastStatusMessage = "Starting server on 127.0.0.1...";
        }
        appendLog("INFO", "Starting server (TCP " + tcpPort + ", UDP " + udpPort + ", " + ownedAccounts.size() + " account(s)).");
        emitSnapshot();

        try {
            lifecycleExecutor.execute(() -> runStart(insecureDemoAcknowledged, tcpPort, udpPort, ownedAccounts, result));
        } catch (RuntimeException rejected) {
            clearAll(ownedAccounts);
            synchronized (stateLock) {
                state = ServerModels.ServerState.FAILED;
                lastStatusMessage = "Unable to schedule server startup.";
            }
            emitSnapshot();
            result.completeExceptionally(rejected);
        }
        return result;
    }

    private void runStart(
            boolean insecureDemoAcknowledged,
            int tcpPort,
            int udpPort,
            List<AccountInput> ownedAccounts,
            CompletableFuture<ServerModels.ServerSnapshot> result) {
        NetSyncServerApplication created = null;
        try {
            if (ownedAccounts.isEmpty()) {
                throw new IllegalArgumentException("Add at least one disposable demo account before starting.");
            }
            List<String> usernames = new ArrayList<>(ownedAccounts.size());
            List<CredentialStore.DemoAccountSpec> specs = new ArrayList<>(ownedAccounts.size());
            for (AccountInput acc : ownedAccounts) {
                if (acc.username() == null || acc.username().isBlank()) {
                    throw new IllegalArgumentException("Username cannot be blank.");
                }
                if (acc.password() == null || acc.password().length == 0) {
                    throw new IllegalArgumentException("Password is required for account '" + acc.username() + "'.");
                }
                usernames.add(acc.username());
                specs.add(new CredentialStore.DemoAccountSpec(acc.username(), acc.password()));
            }

            DemoServerConfig config = new DemoServerConfig(insecureDemoAcknowledged, tcpPort, usernames, udpPort);
            CredentialStore credentials = new CredentialStore(specs);
            created = appFactory.apply(config, credentials);
            created.start();

            boolean shouldAbort;
            synchronized (stateLock) {
                shouldAbort = closed || state != ServerModels.ServerState.STARTING;
                if (!shouldAbort) {
                    activeApplication = created;
                    startedAt = Instant.now();
                    lastTcpPort = created.port();
                    lastUdpPort = created.udpPort();
                    lastAccountCount = created.configuredAccountCount();
                    state = ServerModels.ServerState.RUNNING;
                    lastStatusMessage = "Listening on 127.0.0.1 TCP " + lastTcpPort + ", UDP " + lastUdpPort + ".";
                }
            }
            if (shouldAbort) {
                created.close();
                result.complete(snapshot());
                return;
            }
            appendLog("INFO", "Server running on 127.0.0.1 TCP " + created.port() + ", UDP " + created.udpPort() + ".");
            ServerModels.ServerSnapshot snap = snapshot();
            emitSnapshot(snap);
            result.complete(snap);
        } catch (Exception failure) {
            if (created != null) {
                try {
                    created.close();
                } catch (RuntimeException ignored) {
                }
            }
            String safeMessage = sanitizeStartupError(failure);
            synchronized (stateLock) {
                if (!closed) {
                    state = ServerModels.ServerState.FAILED;
                    lastStatusMessage = safeMessage;
                }
            }
            appendLog("ERROR", safeMessage);
            emitSnapshot();
            result.completeExceptionally(new IllegalStateException(safeMessage));
        } finally {
            clearAll(ownedAccounts);
        }
    }

    /** Stops the active server cleanly off the JavaFX thread. */
    public CompletableFuture<ServerModels.ServerSnapshot> stopServer() {
        CompletableFuture<ServerModels.ServerSnapshot> result = new CompletableFuture<>();
        NetSyncServerApplication toClose;
        synchronized (stateLock) {
            if (state == ServerModels.ServerState.STOPPED) {
                result.complete(snapshot());
                return result;
            }
            if (state == ServerModels.ServerState.STOPPING) {
                result.completeExceptionally(new IllegalStateException("Server is already stopping."));
                return result;
            }
            state = ServerModels.ServerState.STOPPING;
            lastStatusMessage = "Stopping server...";
            toClose = activeApplication;
            activeApplication = null;
            startedAt = null;
        }
        appendLog("INFO", "Stopping server...");
        emitSnapshot();

        try {
            lifecycleExecutor.execute(() -> {
                try {
                    if (toClose != null) {
                        toClose.close();
                    }
                    synchronized (stateLock) {
                        state = ServerModels.ServerState.STOPPED;
                        lastStatusMessage = "Server stopped.";
                    }
                    appendLog("INFO", "Server stopped cleanly.");
                    ServerModels.ServerSnapshot snap = snapshot();
                    emitSnapshot(snap);
                    result.complete(snap);
                } catch (RuntimeException failure) {
                    synchronized (stateLock) {
                        state = ServerModels.ServerState.FAILED;
                        lastStatusMessage = "Server shutdown encountered an error.";
                    }
                    appendLog("ERROR", "Server shutdown encountered an error.");
                    emitSnapshot();
                    result.completeExceptionally(failure);
                }
            });
        } catch (RuntimeException rejected) {
            if (toClose != null) {
                try { toClose.close(); } catch (RuntimeException ignored) {}
            }
            synchronized (stateLock) {
                state = ServerModels.ServerState.STOPPED;
                lastStatusMessage = "Server stopped.";
            }
            result.complete(snapshot());
        }
        return result;
    }

    /** Performs a safe sequential stop followed by start on the configured ports. */
    public CompletableFuture<ServerModels.ServerSnapshot> restartServer(
            boolean insecureDemoAcknowledged,
            int tcpPort,
            int udpPort,
            List<AccountInput> accounts) {
        List<AccountInput> owned = copyAndClearInputs(accounts);
        return stopServer().handle((snap, err) -> null).thenCompose(ignored ->
                startServer(insecureDemoAcknowledged, tcpPort, udpPort, owned));
    }

    /** Captures a thread-safe read-only monitoring snapshot without touching Selector-owned mutable internals. */
    public ServerModels.ServerSnapshot snapshot() {
        synchronized (stateLock) {
            NetSyncServerApplication app = activeApplication;
            if (app != null && state == ServerModels.ServerState.RUNNING) {
                if (!app.isRunning()) {
                    state = app.failure() != null ? ServerModels.ServerState.FAILED : ServerModels.ServerState.STOPPED;
                    lastStatusMessage = app.failure() != null
                            ? "Server stopped unexpectedly."
                            : "Server stopped.";
                    activeApplication = null;
                    startedAt = null;
                } else {
                    List<SessionView> active = app.sessions().activeSessions();
                    List<ServerModels.MonitoredSession> monitored = new ArrayList<>(active.size());
                    for (SessionView view : active) {
                        PresenceState presence = app.presenceState(view.clientId()).orElse(PresenceState.OFFLINE);
                        monitored.add(new ServerModels.MonitoredSession(
                                view.clientId(), view.username(), view.connectionId(), presence));
                    }
                    long uptime = startedAt == null ? 0L : Math.max(0L, Duration.between(startedAt, Instant.now()).toSeconds());
                    return new ServerModels.ServerSnapshot(
                            ServerModels.ServerState.RUNNING,
                            ProtocolLimits.BIND_ADDRESS,
                            app.port(),
                            app.udpPort(),
                            app.configuredAccountCount(),
                            uptime,
                            monitored,
                            app.activeTransferCount(),
                            lastStatusMessage);
                }
            }
            return new ServerModels.ServerSnapshot(
                    state,
                    ProtocolLimits.BIND_ADDRESS,
                    lastTcpPort,
                    lastUdpPort,
                    lastAccountCount,
                    0L,
                    List.of(),
                    0,
                    lastStatusMessage);
        }
    }

    /** Refreshes the listener with the latest monitoring snapshot (safe to call from a periodic UI timer). */
    public void refreshMonitoring() {
        emitSnapshot();
    }

    public List<ServerModels.ActivityLogEntry> activityLogSnapshot() {
        synchronized (stateLock) {
            return List.copyOf(activityLog);
        }
    }

    public ServerModels.ServerState state() {
        synchronized (stateLock) {
            return state;
        }
    }

    private void emitSnapshot() {
        emitSnapshot(snapshot());
    }

    private void emitSnapshot(ServerModels.ServerSnapshot snap) {
        Listener current = this.listener;
        if (current != null) {
            dispatcher.postImportant(null, () -> current.onSnapshot(snap));
        }
    }

    private void appendLog(String level, String message) {
        ServerModels.ActivityLogEntry entry = new ServerModels.ActivityLogEntry(Instant.now(), level, message);
        synchronized (stateLock) {
            while (activityLog.size() >= MAX_LOG_ENTRIES) {
                activityLog.removeFirst();
            }
            activityLog.addLast(entry);
        }
        Listener current = this.listener;
        if (current != null) {
            dispatcher.postImportant(null, () -> current.onLogEntry(entry));
        }
    }

    private static List<AccountInput> copyAndClearInputs(List<AccountInput> accounts) {
        if (accounts == null) {
            return List.of();
        }
        List<AccountInput> copies = new ArrayList<>(accounts.size());
        for (AccountInput acc : accounts) {
            if (acc != null) {
                copies.add(new AccountInput(acc.username(), acc.password()));
                acc.clearPassword();
            }
        }
        return copies;
    }

    private static void clearAll(List<AccountInput> accounts) {
        for (AccountInput acc : accounts) {
            if (acc != null) {
                acc.clearPassword();
            }
        }
    }

    private static String sanitizeStartupError(Exception failure) {
        if (failure instanceof IllegalArgumentException) {
            return failure.getMessage() != null ? failure.getMessage() : "Invalid server configuration.";
        }
        String className = failure.getClass().getSimpleName();
        if (className.contains("BindException")) {
            return "Port bind failed: TCP or UDP port is already in use on 127.0.0.1.";
        }
        return "Server startup failed (" + className + "). Verify ports, storage permissions, and configuration.";
    }

    @Override
    public void close() {
        NetSyncServerApplication toClose;
        synchronized (stateLock) {
            if (closed) {
                return;
            }
            closed = true;
            state = ServerModels.ServerState.STOPPED;
            toClose = activeApplication;
            activeApplication = null;
            startedAt = null;
        }
        dispatcher.close();
        if (toClose != null) {
            try {
                toClose.close();
            } catch (RuntimeException ignored) {
            }
        }
        lifecycleExecutor.shutdownNow();
        try {
            lifecycleExecutor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
