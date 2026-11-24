package com.netsync.client;

import com.netsync.common.*;
import java.io.Console;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Nonblocking response handling; password input runs only on the command caller's thread. */
public final class ClientAuthenticationAdapter implements AutoCloseable {
    @FunctionalInterface
    public interface PasswordInput {
        /** Returns an owned buffer which the adapter clears; null cancels input. */
        char[] readPassword();

        static PasswordInput console() {
            return () -> {
                Console console = System.console();
                if (console == null) throw new CommandRegistry.InputException(
                        "Secure password input requires an interactive terminal; passwords cannot be command arguments.");
                return console.readPassword("Disposable password: ");
            };
        }
    }

    /** Immutable secret snapshot for later composition. Never log presenceToken(). */
    public record PresenceCredentials(SessionView session, String presenceToken,
                                      int heartbeatIntervalSeconds, int presenceTimeoutSeconds) {
        public PresenceCredentials {
            Objects.requireNonNull(session); Objects.requireNonNull(presenceToken);
        }
        @Override public String toString() { return "PresenceCredentials[presenceToken=REDACTED]"; }
    }

    private static final class Pending {
        final UUID id = UUID.randomUUID();
        final UUID connection;
        final MessageType operation;
        final String username;
        final long started;
        boolean sent;
        Pending(UUID connection, MessageType operation, String username, long started) {
            this.connection = connection; this.operation = operation;
            this.username = username; this.started = started;
        }
    }

    private final TcpClient client;
    private final CommandRegistry commands;
    private final Consumer<String> output;
    private final ClientOutputQueue outputQueue;
    private final PasswordInput passwords;
    private final LongSupplier nanoTime;
    private final long timeoutNanos;
    private final ScheduledExecutorService timer;
    private final List<ClientDispatcher.Registration> registrations = new ArrayList<>();
    private final CommandRegistry.Command loginCommand = this::loginCommand;
    private final CommandRegistry.Command logoutCommand = this::logoutCommand;
    // All mutable authentication state is guarded by the transport monitor, including generation checks.
    private Pending pending;
    private PresenceCredentials credentials;
    private boolean closed;
    private final Set<Runnable> stateListeners = new LinkedHashSet<>();

    /** Internal composition notifications only enqueue bounded work; never perform I/O here. */
    ClientDispatcher.Registration onStateChanged(Runnable signal) {
        synchronized (client) { stateListeners.add(signal); }
        return () -> { synchronized (client) { stateListeners.remove(signal); } };
    }
    private void stateChanged() { for (Runnable signal : List.copyOf(stateListeners)) signal.run(); }
    public boolean isLoggingOut() {
        synchronized (client) { return pending != null && pending.operation == MessageType.LOGOUT_REQUEST; }
    }

    public ClientAuthenticationAdapter(TcpClient client, CommandRegistry commands,
                                        Consumer<String> output, PasswordInput passwords) {
        this(client, commands, output, passwords, System::nanoTime,
                Duration.ofSeconds(ProtocolLimits.LOGIN_TIMEOUT_SECONDS));
    }

    ClientAuthenticationAdapter(TcpClient client, CommandRegistry commands, Consumer<String> output,
                                 PasswordInput passwords, LongSupplier nanoTime, Duration timeout) {
        this(client, commands, output, null, passwords, nanoTime, timeout);
    }

    ClientAuthenticationAdapter(TcpClient client, CommandRegistry commands, ClientOutputQueue outputQueue,
                                 PasswordInput passwords) {
        this(client, commands, null, Objects.requireNonNull(outputQueue), passwords, System::nanoTime,
                Duration.ofSeconds(ProtocolLimits.LOGIN_TIMEOUT_SECONDS));
    }

    private ClientAuthenticationAdapter(TcpClient client, CommandRegistry commands, Consumer<String> output,
                                        ClientOutputQueue outputQueue, PasswordInput passwords,
                                        LongSupplier nanoTime, Duration timeout) {
        this.client = Objects.requireNonNull(client);
        this.commands = Objects.requireNonNull(commands);
        this.output = output;
        this.outputQueue = outputQueue;
        if (output == null && outputQueue == null) throw new NullPointerException("output");
        this.passwords = Objects.requireNonNull(passwords);
        this.nanoTime = Objects.requireNonNull(nanoTime);
        timeoutNanos = Objects.requireNonNull(timeout).toNanos();
        if (timeoutNanos <= 0 || timeoutNanos > TimeUnit.SECONDS.toNanos(ProtocolLimits.LOGIN_TIMEOUT_SECONDS))
            throw new IllegalArgumentException("Authentication timeout must be positive and at most ten seconds");
        timer = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "netsync-client-auth-timeout");
            thread.setDaemon(true); return thread;
        });
        try {
            commands.register("/login", "/login <username>", "Log in with a hidden disposable password", loginCommand);
            commands.register("/logout", "/logout", "Log out and close this connection", logoutCommand);
            registrations.add(client.dispatcher().register(MessageType.LOGIN_RESPONSE, this::loginResponse));
            registrations.add(client.dispatcher().register(MessageType.ERROR, this::errorResponse));
            registrations.add(client.dispatcher().register(MessageType.ACK, this::acknowledgement));
            registrations.add(client.onDisconnect(this::disconnected));
            timer.scheduleWithFixedDelay(this::checkTimeouts, 100, 100, TimeUnit.MILLISECONDS);
        } catch (RuntimeException failure) { close(); throw failure; }
    }

    private void loginCommand(List<String> arguments) {
        if (arguments.size() != 1) throw new CommandRegistry.InputException("Usage: /login <username>; enter the password at the hidden prompt.");
        login(arguments.getFirst());
    }
    private void logoutCommand(List<String> arguments) {
        if (!arguments.isEmpty()) throw new CommandRegistry.InputException("Usage: /logout");
        logout();
    }

    public void login(String username) {
        if (client.isDispatchThread()) throw new CommandRegistry.InputException("Run /login from the terminal thread.");
        if (username == null || !username.matches("[A-Za-z0-9_]{1,32}"))
            throw new CommandRegistry.InputException("Username must contain 1-32 ASCII letters, digits or underscores.");
        Pending operation;
        synchronized (client) {
            requireReady();
            if (!client.isLoopbackConnection()) throw new CommandRegistry.InputException("Demo login requires a loopback TCP connection.");
            if (client.session().isPresent()) throw new CommandRegistry.InputException("Already authenticated; log out first.");
            operation = new Pending(client.connectionId().orElseThrow(), MessageType.LOGIN_REQUEST, username, nanoTime.getAsLong());
            pending = operation;
        }
        char[] password = null;
        try {
            try { password = passwords.readPassword(); }
            catch (Exception failure) {
                throw new CommandRegistry.InputException("Secure password input failed; use an interactive terminal with hidden input.");
            }
            if (password == null) throw new CommandRegistry.InputException("Login cancelled.");
            if (password.length == 0 || password.length > 128)
                throw new CommandRegistry.InputException("Disposable password must contain 1-128 UTF-16 code units.");
            synchronized (client) {
                if (closed || pending != operation || !current(operation.connection)
                        || nanoTime.getAsLong() - operation.started >= timeoutNanos)
                    throw new CommandRegistry.InputException("Login expired or connection changed; reconnect and try again.");
                MessageEnvelope request = new MessageEnvelope(ProtocolLimits.VERSION, MessageType.LOGIN_REQUEST,
                        operation.id, null, new MessageBody.LoginRequest(username, new String(password)));
                // The immutable wire DTO/String cannot be wiped. Neither is retained by this adapter.
                if (client.sendJson(request) != EnqueueResult.ACCEPTED)
                    throw new CommandRegistry.InputException("Login request could not be queued.");
                operation.sent = true;
            }
            emit("Login requested.");
        } catch (Exception failure) {
            synchronized (client) { if (pending == operation) pending = null; }
            if (nanoTime.getAsLong() - operation.started >= timeoutNanos) client.disconnect(operation.connection);
            if (failure instanceof CommandRegistry.InputException safe) throw safe;
            throw new CommandRegistry.InputException("Secure password input or login request failed.");
        } finally { if (password != null) Arrays.fill(password, '\0'); }
    }

    public void logout() {
        synchronized (client) {
            requireReady();
            if (client.session().isEmpty()) throw new CommandRegistry.InputException("Not authenticated.");
            Pending operation = new Pending(client.connectionId().orElseThrow(), MessageType.LOGOUT_REQUEST, null, nanoTime.getAsLong());
            try {
                if (client.sendJson(new MessageEnvelope(ProtocolLimits.VERSION, MessageType.LOGOUT_REQUEST,
                        operation.id, null, new MessageBody.Empty())) != EnqueueResult.ACCEPTED)
                    throw new CommandRegistry.InputException("Logout request could not be queued.");
                operation.sent = true; pending = operation; stateChanged();
            } catch (ProtocolException invalid) { throw new CommandRegistry.InputException("Logout request failed."); }
        }
        emit("Logout requested.");
    }

    private void requireReady() {
        if (closed) throw new CommandRegistry.InputException("Authentication adapter is closed.");
        refresh();
        if (!client.isConnected()) throw new CommandRegistry.InputException("Connect before authenticating.");
        if (pending != null && !current(pending.connection)) pending = null;
        if (pending != null) throw new CommandRegistry.InputException("An authentication command is already pending.");
    }

    private boolean current(UUID connection) { return client.connectionId().filter(connection::equals).isPresent(); }
    private void refresh() {
        if (credentials != null && (!current(credentials.session().connectionId())
                || client.session().filter(credentials.session()::equals).isEmpty())) { credentials = null; stateChanged(); }
    }

    private void disconnected(UUID connection, TcpClient.Failure reason) {
        // Transport already holds its monitor. No I/O or callbacks here.
        if (credentials != null && credentials.session().connectionId().equals(connection)) credentials = null;
        // Preserve only request metadata across orderly EOF so queued ERROR/ACK controls
        // can still be correlated. It expires on the existing bounded request deadline.
        if (pending != null && pending.connection.equals(connection)
                && (reason != TcpClient.Failure.REMOTE_CLOSED || !pending.sent)) pending = null;
        stateChanged();
    }

    /** Returns only current credentials; disconnect/reconnect/close immediately makes access empty. */
    public Optional<PresenceCredentials> presenceCredentials() {
        synchronized (client) { refresh(); return closed ? Optional.empty() : Optional.ofNullable(credentials); }
    }
    public boolean isCurrent(PresenceCredentials snapshot) {
        synchronized (client) { refresh(); return !closed && snapshot != null && snapshot == credentials; }
    }

    private Pending matching(MessageEnvelope message) {
        if (closed || pending == null || !pending.sent || !pending.id.equals(message.replyTo())) return null;
        UUID connection = client.connectionId().orElse(null);
        if (connection != null && !connection.equals(pending.connection)) { pending = null; return null; }
        return pending;
    }

    private void loginResponse(MessageEnvelope message) {
        checkTimeouts();
        UUID disconnect = null;
        String notice;
        Pending operation;
        synchronized (client) {
            operation = matching(message);
            if (operation == null || operation.operation != MessageType.LOGIN_REQUEST
                    || !(message.body() instanceof MessageBody.LoginResponse response)) return;
            pending = null;
            if (!current(operation.connection)) notice = "Login did not complete: connection closed.";
            else if (!operation.username.equals(response.username())) {
                clearIdentity(operation.connection); disconnect = operation.connection;
                notice = "Login response did not match the requested identity; reconnect.";
            } else {
                SessionView session = new SessionView(response.clientId(), response.username(), operation.connection);
                client.supplySession(operation.connection, session);
                credentials = new PresenceCredentials(session, response.presenceToken(),
                        response.heartbeatIntervalSeconds(), response.presenceTimeoutSeconds());
                stateChanged();
                notice = "Logged in as " + response.username() + " (client ID: " + response.clientId() + ").";
            }
        }
        if (disconnect != null) client.disconnect(disconnect);
        emit(notice, operation.connection);
    }

    private void errorResponse(MessageEnvelope message) {
        checkTimeouts();
        UUID disconnect = null;
        String notice;
        Pending operation;
        synchronized (client) {
            operation = matching(message);
            if (operation == null || !(message.body() instanceof MessageBody.Error error)) return;
            pending = null;
            if (operation.operation == MessageType.LOGIN_REQUEST) {
                clearIdentity(operation.connection);
                // No retry of an uncertain login on this connection.
                disconnect = operation.connection;
                notice = "Login failed (" + error.code() + "). Reconnect before trying again.";
            } else {
                if (error.code() == ErrorCode.UNAUTHENTICATED) {
                    clearIdentity(operation.connection); disconnect = operation.connection;
                }
                notice = "Logout failed (" + error.code() + ").";
                stateChanged();
            }
        }
        if (disconnect != null) client.disconnect(disconnect);
        emit(notice, operation.connection); // Never echo server-supplied error text.
    }

    private void acknowledgement(MessageEnvelope message) {
        checkTimeouts();
        UUID disconnect;
        synchronized (client) {
            Pending operation = matching(message);
            if (operation == null || operation.operation != MessageType.LOGOUT_REQUEST
                    || !(message.body() instanceof MessageBody.Ack ack)
                    || ack.operation() != MessageType.LOGOUT_REQUEST || ack.status() != AckStatus.ACCEPTED) return;
            pending = null; disconnect = operation.connection;
            clearIdentity(disconnect);
        }
        client.disconnect(disconnect);
        emit("Logged out.", disconnect);
    }

    private void clearIdentity(UUID connection) {
        if (credentials != null && credentials.session().connectionId().equals(connection)) credentials = null;
        if (current(connection)) client.clearSession();
        stateChanged();
    }

    /** One periodic task, one pending operation; virtual monotonic time is injectable in tests. */
    void checkTimeouts() {
        UUID disconnect = null;
        synchronized (client) {
            if (closed) return;
            refresh();
            if (pending != null && client.isConnected() && !current(pending.connection)) pending = null;
            if (pending != null && nanoTime.getAsLong() - pending.started >= timeoutNanos) {
                disconnect = pending.connection; pending = null; clearIdentity(disconnect);
            }
        }
        if (disconnect != null) {
            client.disconnect(disconnect);
            emit("Authentication response timed out; original connection closed. Reconnect before trying again.", disconnect);
        }
    }

    private void emit(String text) {
        if (outputQueue != null) outputQueue.offer(text);
        else try { output.accept(text); } catch (RuntimeException ignored) { }
    }
    private void emit(String text, UUID generation) {
        if (outputQueue != null) outputQueue.offerImportant(text, generation);
        else emit(text);
    }

    @Override public void close() {
        UUID disconnect;
        synchronized (client) {
            if (closed) return;
            closed = true;
            disconnect = pending != null ? pending.connection : credentials != null ? credentials.session().connectionId() : null;
            pending = null;
            if (disconnect != null) clearIdentity(disconnect);
            credentials = null;
            stateChanged(); stateListeners.clear();
        }
        timer.shutdownNow();
        registrations.forEach(ClientDispatcher.Registration::close);
        commands.unregister("/login", loginCommand); commands.unregister("/logout", logoutCommand);
        if (disconnect != null) client.disconnect(disconnect);
    }
}
