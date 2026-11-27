package com.netsync.gui;

import com.netsync.chat.ChatClientAdapter;
import com.netsync.client.ClientAuthenticationAdapter;
import com.netsync.client.ClientDispatcher;
import com.netsync.client.ClientPresenceAdapter;
import com.netsync.client.CommandRegistry;
import com.netsync.client.TcpClient;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.PresenceState;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionView;
import com.netsync.transfer.FileTransferClientAdapter;
import com.netsync.transfer.FileTransferProgress;
import com.netsync.transfer.FileTransferResult;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Headless, thread-safe client session controller bridging the JavaFX UI and the real NetSync
 * TCP/UDP production adapters without CLI text parsing or protocol duplication.
 */
public final class ClientSessionController implements AutoCloseable {
    public static final int MAX_MESSAGES_PER_CONVERSATION = 200;
    public static final int MAX_CONVERSATIONS = 100;
    public static final int MAX_TRANSFER_HISTORY = 100;

    public interface Listener {
        default void onConnectionPhaseChanged(ClientModels.ConnectionPhase phase, SessionView session, String statusMessage) {}
        default void onUsersChanged(List<ClientModels.UserItem> users) {}
        default void onConversationUpdated(UUID peerClientId, List<ClientModels.ChatMessageItem> messages) {}
        default void onIncomingMessage(UUID peerClientId, ClientModels.ChatMessageItem message) {}
        default void onTransfersChanged(List<ClientModels.TransferItem> transfers) {}
        default void onNotice(String severity, String message) {}
    }

    private final TcpClient transport;
    private final CommandRegistry commands;
    private final ClientAuthenticationAdapter authentication;
    private final ClientPresenceAdapter presence;
    private final ChatClientAdapter chat;
    private final FileTransferClientAdapter fileTransfer;
    private final UiEventDispatcher dispatcher;
    private final ExecutorService backgroundOps;
    private final java.util.function.LongSupplier nanoTime;
    private final List<ClientDispatcher.Registration> registrations = new ArrayList<>();

    private final Object lock = new Object();
    private final Map<UUID, ClientModels.UserItem> usersByClientId = new LinkedHashMap<>();
    private final Map<UUID, ArrayDeque<ClientModels.ChatMessageItem>> conversations = new LinkedHashMap<>();
    private final Map<UUID, UUID> requestToPeer = new LinkedHashMap<>();
    private final Map<UUID, ClientModels.TransferItem> transfersByKey = new LinkedHashMap<>();
    private final Map<UUID, UUID> transferIdToKey = new LinkedHashMap<>();
    private UUID pendingOutgoingTransferKey;

    private volatile Listener listener;
    private ClientModels.ConnectionPhase phase = ClientModels.ConnectionPhase.DISCONNECTED;
    private SessionView currentSession;
    private UUID currentGeneration;
    private String currentHost = ProtocolLimits.BIND_ADDRESS;
    private int currentTcpPort = ProtocolLimits.TCP_PORT;
    private int currentUdpPort = ProtocolLimits.UDP_PORT;
    private int heartbeatIntervalSeconds = ProtocolLimits.HEARTBEAT_INTERVAL_SECONDS;
    private int presenceTimeoutSeconds = ProtocolLimits.PRESENCE_TIMEOUT_SECONDS;
    private CompletableFuture<SessionView> pendingLoginFuture;
    private CompletableFuture<Void> pendingLogoutFuture;
    private boolean closed;

    public ClientSessionController(Consumer<Runnable> uiExecutor) {
        this(uiExecutor, Path.of(ProtocolLimits.STORAGE_ROOT, "clients"), new TcpClient());
    }

    public ClientSessionController(Consumer<Runnable> uiExecutor, Path clientsRoot) {
        this(uiExecutor, clientsRoot, new TcpClient());
    }

    public ClientSessionController(Consumer<Runnable> uiExecutor, Path clientsRoot, TcpClient transport) {
        this(uiExecutor, clientsRoot, transport, System::nanoTime);
    }

    public ClientSessionController(Consumer<Runnable> uiExecutor, Path clientsRoot, TcpClient transport,
                                   java.util.function.LongSupplier nanoTime) {
        Objects.requireNonNull(uiExecutor, "uiExecutor");
        Objects.requireNonNull(clientsRoot, "clientsRoot");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.commands = new CommandRegistry();
        this.dispatcher = new UiEventDispatcher(uiExecutor, UiEventDispatcher.DEFAULT_CAPACITY,
                dropped -> notifyNotice("WARNING", "UI event queue dropped " + dropped + " update(s); click Refresh Users if needed."));
        this.backgroundOps = new ThreadPoolExecutor(
                2, 2, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(32),
                task -> {
                    Thread thread = new Thread(task, "netsync-gui-client-ops");
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());

        this.authentication = new ClientAuthenticationAdapter(transport, commands, ignored -> {},
                () -> { throw new CommandRegistry.InputException("Use the GUI login form for password entry."); });
        this.presence = new ClientPresenceAdapter(transport, authentication, commands, ProtocolLimits.UDP_PORT);
        this.chat = new ChatClientAdapter(transport, authentication, commands, (text, gen, activeOnly) -> {});
        this.fileTransfer = new FileTransferClientAdapter(
                transport, commands, (text, gen) -> {},
                clientsRoot,
                () -> transport.session().map(session -> clientsRoot.resolve(session.clientId().toString())).orElse(null),
                null);

        registrations.add(authentication.onAuthEvent(this::handleAuthEvent));
        registrations.add(presence.onPresenceEvent(this::handlePresenceEvent));
        registrations.add(chat.onChatEvent(this::handleChatEvent));
        registrations.add(fileTransfer.onTransferEvent(this::handleTransferEvent));
    }

    public void setListener(Listener listener) {
        this.listener = listener;
        SessionView snapSession;
        ClientModels.ConnectionPhase snapPhase;
        List<ClientModels.UserItem> snapUsers;
        List<ClientModels.TransferItem> snapTransfers;
        synchronized (lock) {
            snapSession = currentSession;
            snapPhase = phase;
            snapUsers = sortedUsersLocked();
            snapTransfers = transferHistoryLocked();
        }
        if (listener != null) {
            dispatcher.postImportant(null, () -> {
                listener.onConnectionPhaseChanged(snapPhase, snapSession, "");
                listener.onUsersChanged(snapUsers);
                listener.onTransfersChanged(snapTransfers);
            });
        }
    }

    /**
     * Combined connect-and-login executed off the JavaFX thread.
     * Collects credentials upfront so the server's 10-second login deadline is respected.
     * Clears the caller's password buffer in all cases.
     */
    public CompletableFuture<SessionView> connectAndLogin(
            String host, int tcpPort, int udpPort, String username, char[] password) {
        char[] ownedPassword = password == null ? null : Arrays.copyOf(password, password.length);
        if (password != null) {
            Arrays.fill(password, '\0');
        }
        CompletableFuture<SessionView> future = new CompletableFuture<>();
        try {
            String trimmedHost = host == null ? "" : host.trim();
            if (trimmedHost.isEmpty()) {
                throw new IllegalArgumentException("Server host is required.");
            }
            if (tcpPort < 1 || tcpPort > 65535) {
                throw new IllegalArgumentException("TCP port must be between 1 and 65535.");
            }
            if (udpPort < 1 || udpPort > 65535) {
                throw new IllegalArgumentException("UDP port must be between 1 and 65535.");
            }
            String trimmedUser = username == null ? "" : username.trim();
            if (!trimmedUser.matches("[A-Za-z0-9_]{1,32}")) {
                throw new IllegalArgumentException("Username must contain 1-32 ASCII letters, digits, or underscores.");
            }
            if (ownedPassword == null || ownedPassword.length == 0 || ownedPassword.length > 128) {
                throw new IllegalArgumentException("Password must contain 1-128 characters.");
            }

            synchronized (lock) {
                if (closed) {
                    throw new IllegalStateException("Client controller is closed.");
                }
                if (phase == ClientModels.ConnectionPhase.CONNECTING || pendingLoginFuture != null) {
                    throw new IllegalStateException("A login request is already in progress.");
                }
                if (phase == ClientModels.ConnectionPhase.AUTHENTICATED) {
                    throw new IllegalStateException("Already signed in. Log out first.");
                }
                if (phase == ClientModels.ConnectionPhase.LOGGING_OUT) {
                    throw new IllegalStateException("Previous session is still logging out; wait a moment and try again.");
                }
                phase = ClientModels.ConnectionPhase.CONNECTING;
                currentHost = trimmedHost;
                currentTcpPort = tcpPort;
                currentUdpPort = udpPort;
                pendingLoginFuture = future;
            }
            emitPhase(ClientModels.ConnectionPhase.CONNECTING, null, "Connecting to " + trimmedHost + ":" + tcpPort + "...");

            backgroundOps.execute(() -> {
                try {
                    if (transport.state() != TcpClient.State.DISCONNECTED) {
                        transport.disconnect();
                    }
                    presence.setUdpPort(udpPort);
                    transport.connect(trimmedHost, tcpPort);
                    UUID generation = transport.connectionId().orElseThrow(() ->
                            new IOException("Connection closed before authentication could begin."));
                    synchronized (lock) {
                        currentGeneration = generation;
                    }
                    dispatcher.setActiveGeneration(generation);
                    authentication.login(trimmedUser, ownedPassword);
                } catch (Exception failure) {
                    String safeMsg = mapConnectLoginFailure(failure);
                    try {
                        transport.disconnect();
                    } catch (RuntimeException ignored) {
                    }
                    failPendingLogin(safeMsg);
                } finally {
                    if (ownedPassword != null) {
                        Arrays.fill(ownedPassword, '\0');
                    }
                }
            });
            return future;
        } catch (RuntimeException validationError) {
            if (ownedPassword != null) {
                Arrays.fill(ownedPassword, '\0');
            }
            future.completeExceptionally(validationError);
            return future;
        }
    }

    /** Sends a private TCP chat message to the specified recipient client ID. */
    public UUID sendChatMessage(UUID recipientClientId, String text) {
        return chat.sendMessage(recipientClientId, text);
    }

    /** Requests a fresh presence snapshot via the existing TCP PRESENCE_QUERY operation. */
    public void refreshUsers() {
        backgroundOps.execute(() -> {
            try {
                presence.query();
            } catch (RuntimeException failure) {
                notifyNotice("WARNING", failure.getMessage() != null ? failure.getMessage() : "Unable to refresh users.");
            }
        });
    }

    /** Initiates a consent-based file transfer offer to the selected recipient. */
    public void sendFile(UUID recipientClientId, Path sourceFile) {
        fileTransfer.sendFile(recipientClientId, sourceFile);
    }

    /** Accepts an incoming file offer by transfer ID. */
    public void acceptFileOffer(UUID transferId) {
        fileTransfer.acceptOffer(transferId);
    }

    /** Rejects an incoming file offer by transfer ID. */
    public void rejectFileOffer(UUID transferId) {
        fileTransfer.rejectOffer(transferId);
    }

    /** Cancels a specific active file transfer. */
    public void cancelFileTransfer(UUID transferId) {
        if (transferId != null) {
            fileTransfer.cancelTransfer(transferId);
        } else {
            fileTransfer.cancelActiveTransfer();
        }
    }

    /**
     * Exports a completed, SHA-256-verified incoming transfer to a user-chosen destination off the
     * JavaFX application thread without modifying or deleting the original safe {@code .bin} file.
     */
    public CompletableFuture<SafeFileExportService.ExportResult> exportReceivedFile(UUID transferId, Path destinationFile) {
        Objects.requireNonNull(transferId, "transferId");
        Objects.requireNonNull(destinationFile, "destinationFile");

        ClientModels.TransferItem item;
        synchronized (lock) {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("Client controller is closed."));
            }
            UUID key = resolveTransferKeyLocked(transferId);
            item = key == null ? null : transfersByKey.get(key);
        }
        if (!TransferPresentation.canSaveAs(item)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Save As is only available for completed, verified incoming transfers."));
        }

        Path sourcePath = item.localFile();
        String expectedSha = item.sha256();
        CompletableFuture<SafeFileExportService.ExportResult> future = new CompletableFuture<>();
        try {
            backgroundOps.execute(() -> {
                try {
                    SafeFileExportService.ExportResult exported =
                            SafeFileExportService.exportVerifiedFile(sourcePath, expectedSha, destinationFile);
                    future.complete(exported);
                } catch (Exception failure) {
                    future.completeExceptionally(failure);
                }
            });
        } catch (RuntimeException rejected) {
            future.completeExceptionally(rejected);
        }
        return future;
    }

    /** Returns the monotonic clock reading used for advisory incoming-offer countdown estimates. */
    public long nanoTime() {
        return nanoTime.getAsLong();
    }

    /** Performs correlated protocol logout off the JavaFX thread. */
    public CompletableFuture<Void> logout() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        synchronized (lock) {
            if (closed || phase == ClientModels.ConnectionPhase.DISCONNECTED) {
                future.complete(null);
                return future;
            }
            if (phase == ClientModels.ConnectionPhase.LOGGING_OUT && pendingLogoutFuture != null) {
                return pendingLogoutFuture;
            }
            phase = ClientModels.ConnectionPhase.LOGGING_OUT;
            pendingLogoutFuture = future;
        }
        emitPhase(ClientModels.ConnectionPhase.LOGGING_OUT, currentSessionSnapshot(), "Logging out...");
        backgroundOps.execute(() -> {
            try {
                authentication.logout();
            } catch (RuntimeException failure) {
                transport.disconnect();
                completeDisconnectState("Disconnected.");
                future.complete(null);
            }
        });
        return future;
    }

    /** Disconnects the current TCP session immediately off the JavaFX thread. */
    public CompletableFuture<Void> disconnect() {
        return CompletableFuture.runAsync(() -> {
            transport.disconnect();
            completeDisconnectState("Disconnected.");
        }, backgroundOps);
    }

    // --- Event handlers from production adapters ---

    private void handleAuthEvent(ClientAuthenticationAdapter.AuthEvent event) {
        if (event instanceof ClientAuthenticationAdapter.AuthEvent.LoginSucceeded ok) {
            CompletableFuture<SessionView> loginFuture;
            synchronized (lock) {
                if (closed) return;
                clearSessionDataLocked();
                currentSession = ok.session();
                currentGeneration = ok.connectionId();
                heartbeatIntervalSeconds = ok.heartbeatIntervalSeconds();
                presenceTimeoutSeconds = ok.presenceTimeoutSeconds();
                phase = ClientModels.ConnectionPhase.AUTHENTICATED;
                loginFuture = pendingLoginFuture;
                pendingLoginFuture = null;
            }
            dispatcher.setActiveGeneration(ok.connectionId());
            emitPhase(ClientModels.ConnectionPhase.AUTHENTICATED, ok.session(),
                    "Signed in as " + ok.session().username());
            emitUsers();
            emitTransfers();
            if (loginFuture != null) {
                loginFuture.complete(ok.session());
            }
            refreshUsers();
        } else if (event instanceof ClientAuthenticationAdapter.AuthEvent.LoginFailed failed) {
            boolean matches;
            synchronized (lock) { matches = Objects.equals(currentGeneration, failed.connectionId()); }
            if (matches) {
                String msg = describeLoginError(failed.code(), failed.message());
                failPendingLogin(msg);
            }
        } else if (event instanceof ClientAuthenticationAdapter.AuthEvent.LoginTimedOut timedOut) {
            boolean matches;
            synchronized (lock) { matches = Objects.equals(currentGeneration, timedOut.connectionId()); }
            if (matches) {
                failPendingLogin(timedOut.message());
            }
        } else if (event instanceof ClientAuthenticationAdapter.AuthEvent.LogoutSucceeded logoutOk) {
            boolean matches;
            synchronized (lock) { matches = Objects.equals(currentGeneration, logoutOk.connectionId()); }
            if (matches) {
                completeDisconnectState("Logged out cleanly.");
            }
        } else if (event instanceof ClientAuthenticationAdapter.AuthEvent.LogoutFailed failed) {
            boolean matches;
            synchronized (lock) { matches = Objects.equals(currentGeneration, failed.connectionId()); }
            if (!matches) return;
            notifyNotice("WARNING", "Logout failed (" + failed.code() + ").");
            if (failed.code() == ErrorCode.UNAUTHENTICATED) {
                completeDisconnectState("Session ended.");
            } else {
                synchronized (lock) {
                    if (currentSession != null) {
                        phase = ClientModels.ConnectionPhase.AUTHENTICATED;
                    }
                    if (pendingLogoutFuture != null) {
                        pendingLogoutFuture.completeExceptionally(new IllegalStateException(failed.message()));
                        pendingLogoutFuture = null;
                    }
                }
                emitPhase(phase, currentSessionSnapshot(), "Logout failed (" + failed.code() + ").");
            }
        } else if (event instanceof ClientAuthenticationAdapter.AuthEvent.Disconnected disc) {
            boolean matchesCurrent;
            boolean wasPendingLogin;
            synchronized (lock) {
                matchesCurrent = Objects.equals(currentGeneration, disc.connectionId());
                wasPendingLogin = matchesCurrent && pendingLoginFuture != null;
            }
            if (!matchesCurrent) {
                return;
            }
            if (wasPendingLogin) {
                // Keep pendingLoginFuture alive briefly if REMOTE_CLOSED so a queued ERROR frame can explain why.
                if (disc.reason() != TcpClient.Failure.REMOTE_CLOSED) {
                    failPendingLogin("Disconnected during login (" + disc.reason() + ").");
                }
            } else {
                completeDisconnectState("Disconnected (" + disc.reason() + ").");
            }
        }
    }

    private void handlePresenceEvent(ClientPresenceAdapter.PresenceEvent event) {
        if (event instanceof ClientPresenceAdapter.PresenceEvent.SnapshotReceived snap) {
            List<ClientModels.UserItem> sorted;
            synchronized (lock) {
                if (!isCurrentGenerationLocked(snap.connectionId())) return;
                usersByClientId.clear();
                for (MessageBody.PresenceEntry entry : snap.entries()) {
                    usersByClientId.put(entry.clientId(),
                            new ClientModels.UserItem(entry.clientId(), entry.username(), entry.state()));
                }
                sorted = sortedUsersLocked();
            }
            Listener current = this.listener;
            if (current != null) {
                dispatcher.postImportant(snap.connectionId(), () -> current.onUsersChanged(sorted));
            }
        } else if (event instanceof ClientPresenceAdapter.PresenceEvent.UserUpdated upd) {
            List<ClientModels.UserItem> sorted;
            MessageBody.PresenceEntry entry = upd.entry();
            synchronized (lock) {
                if (!isCurrentGenerationLocked(upd.connectionId())) return;
                // If the same username reconnected with a new clientId, remove stale OFFLINE entry for the old clientId.
                if (entry.state() == PresenceState.ONLINE) {
                    usersByClientId.entrySet().removeIf(e ->
                            !e.getKey().equals(entry.clientId())
                                    && e.getValue().username().equals(entry.username())
                                    && e.getValue().state() == PresenceState.OFFLINE);
                }
                usersByClientId.put(entry.clientId(),
                        new ClientModels.UserItem(entry.clientId(), entry.username(), entry.state()));
                sorted = sortedUsersLocked();
            }
            Listener current = this.listener;
            if (current != null) {
                dispatcher.postPresence(upd.connectionId(), entry.clientId(), () -> current.onUsersChanged(sorted));
            }
        } else if (event instanceof ClientPresenceAdapter.PresenceEvent.QueryFailed failed) {
            notifyNotice("WARNING", "Presence refresh failed (" + failed.code() + ").");
        } else if (event instanceof ClientPresenceAdapter.PresenceEvent.HeartbeatStartFailed failed) {
            notifyNotice("WARNING", failed.reason());
        }
    }

    private void handleChatEvent(ChatClientAdapter.ChatEvent event) {
        if (event instanceof ChatClientAdapter.ChatEvent.MessageSent sent) {
            List<ClientModels.ChatMessageItem> updated;
            synchronized (lock) {
                if (!isCurrentGenerationLocked(sent.connectionId())) return;
                UUID myId = currentSession != null ? currentSession.clientId() : null;
                String myName = currentSession != null ? currentSession.username() : "Me";
                ClientModels.ChatMessageItem item = new ClientModels.ChatMessageItem(
                        sent.requestId(), sent.recipientId(), myId, myName, sent.text(),
                        sent.timestamp(), true, ClientModels.DeliveryStatus.SENDING, "Sending...");
                appendMessageLocked(sent.recipientId(), item);
                requestToPeer.put(sent.requestId(), sent.recipientId());
                updated = conversationSnapshotLocked(sent.recipientId());
            }
            emitConversation(sent.connectionId(), sent.recipientId(), updated);
        } else if (event instanceof ChatClientAdapter.ChatEvent.MessageReceived recv) {
            ClientModels.ChatMessageItem item;
            List<ClientModels.ChatMessageItem> updated;
            synchronized (lock) {
                if (!isCurrentGenerationLocked(recv.connectionId())) return;
                String senderLabel = resolveUsernameLocked(recv.senderClientId());
                item = new ClientModels.ChatMessageItem(
                        recv.sourceMessageId(), recv.senderClientId(), recv.senderClientId(), senderLabel,
                        recv.text(), recv.timestamp(), false, ClientModels.DeliveryStatus.RECEIVED, "");
                appendMessageLocked(recv.senderClientId(), item);
                updated = conversationSnapshotLocked(recv.senderClientId());
            }
            Listener current = this.listener;
            if (current != null) {
                dispatcher.postImportant(recv.connectionId(), () -> {
                    current.onConversationUpdated(recv.senderClientId(), updated);
                    current.onIncomingMessage(recv.senderClientId(), item);
                });
            }
        } else if (event instanceof ChatClientAdapter.ChatEvent.MessageQueued queued) {
            updateOutgoingStatus(queued.connectionId(), queued.requestId(), queued.recipientId(),
                    ClientModels.DeliveryStatus.QUEUED, "Queued by server");
        } else if (event instanceof ChatClientAdapter.ChatEvent.MessageFailed failed) {
            updateOutgoingStatus(failed.connectionId(), failed.requestId(), failed.recipientId(),
                    ClientModels.DeliveryStatus.FAILED, "Failed (" + failed.code() + ")");
        } else if (event instanceof ChatClientAdapter.ChatEvent.MessageTimedOut timedOut) {
            updateOutgoingStatus(timedOut.connectionId(), timedOut.requestId(), timedOut.recipientId(),
                    ClientModels.DeliveryStatus.UNKNOWN_OUTCOME, "No response (30s); outcome unknown");
        }
    }

    private void handleTransferEvent(FileTransferClientAdapter.TransferEvent event) {
        if (event instanceof FileTransferClientAdapter.TransferEvent.OfferSent sent) {
            List<ClientModels.TransferItem> list;
            synchronized (lock) {
                if (!isCurrentGenerationLocked(sent.connectionId())) return;
                UUID itemKey = UUID.randomUUID();
                pendingOutgoingTransferKey = itemKey;
                ClientModels.TransferItem item = new ClientModels.TransferItem(
                        itemKey, null, ClientModels.TransferDirection.OUTGOING,
                        sent.recipientClientId(), resolveUsernameLocked(sent.recipientClientId()),
                        sent.fileName(), null, 0L, sent.sizeBytes(),
                        ClientModels.TransferStatus.OFFERING, "Waiting for recipient consent...", null, false);
                putTransferLocked(itemKey, item);
                list = transferHistoryLocked();
            }
            emitTransfers(sent.connectionId(), list, false, null);
        } else if (event instanceof FileTransferClientAdapter.TransferEvent.OfferAssigned assigned) {
            List<ClientModels.TransferItem> list;
            synchronized (lock) {
                if (!isCurrentGenerationLocked(assigned.connectionId())) return;
                UUID key = pendingOutgoingTransferKey;
                if (key != null && transfersByKey.containsKey(key)) {
                    ClientModels.TransferItem prev = transfersByKey.get(key);
                    ClientModels.TransferItem updated = new ClientModels.TransferItem(
                            prev.itemKey(), assigned.transferId(), prev.direction(),
                            prev.peerClientId(), prev.peerLabel(), prev.fileName(), prev.localFile(),
                            prev.bytesTransferred(), prev.totalBytes(), prev.status(), prev.statusDetail(),
                            prev.sha256(), prev.sha256Verified());
                    transfersByKey.put(key, updated);
                    transferIdToKey.put(assigned.transferId(), key);
                    pendingOutgoingTransferKey = null;
                }
                list = transferHistoryLocked();
            }
            emitTransfers(assigned.connectionId(), list, false, null);
        } else if (event instanceof FileTransferClientAdapter.TransferEvent.OfferReceived offer) {
            List<ClientModels.TransferItem> list;
            long nowNanos = nanoTime.getAsLong();
            long stampedNanos = nowNanos == 0L ? 1L : nowNanos;
            synchronized (lock) {
                if (!isCurrentGenerationLocked(offer.connectionId())) return;
                ClientModels.TransferItem item = new ClientModels.TransferItem(
                        offer.transferId(), offer.transferId(), ClientModels.TransferDirection.INCOMING,
                        offer.senderClientId(), resolveUsernameLocked(offer.senderClientId()),
                        offer.fileName(), null, 0L, offer.sizeBytes(),
                        ClientModels.TransferStatus.OFFERED, "Incoming offer — awaiting your decision",
                        null, false, stampedNanos);
                putTransferLocked(offer.transferId(), item);
                transferIdToKey.put(offer.transferId(), offer.transferId());
                list = transferHistoryLocked();
            }
            emitTransfers(offer.connectionId(), list, false, null);
        } else if (event instanceof FileTransferClientAdapter.TransferEvent.OfferAccepted accepted) {
            updateTransferState(accepted.connectionId(), accepted.transferId(),
                    ClientModels.TransferStatus.TRANSFERRING, "Accepted — receiving chunks...", null, null, false, false);
        } else if (event instanceof FileTransferClientAdapter.TransferEvent.Progress prog) {
            FileTransferProgress p = prog.progress();
            List<ClientModels.TransferItem> list;
            synchronized (lock) {
                if (!isCurrentGenerationLocked(prog.connectionId())) return;
                UUID key = resolveTransferKeyLocked(p.transferId());
                if (key != null) {
                    ClientModels.TransferItem prev = transfersByKey.get(key);
                    if (prev != null && !isTerminal(prev.status())) {
                        ClientModels.TransferItem updated = new ClientModels.TransferItem(
                                prev.itemKey(), p.transferId(), prev.direction(),
                                prev.peerClientId(), prev.peerLabel(), prev.fileName(), prev.localFile(),
                                p.bytesTransferred(), p.totalBytes(), ClientModels.TransferStatus.TRANSFERRING,
                                String.format(java.util.Locale.ROOT, "%.1f%% (%d / %d B)", p.percentage(), p.bytesTransferred(), p.totalBytes()),
                                prev.sha256(), prev.sha256Verified(), 0L);
                        transfersByKey.put(key, updated);
                    }
                }
                list = transferHistoryLocked();
            }
            emitTransfers(prog.connectionId(), list, true, p.transferId());
        } else if (event instanceof FileTransferClientAdapter.TransferEvent.Completed completed) {
            FileTransferResult r = completed.result();
            boolean verified = r.success() && r.sha256() != null && !r.sha256().isBlank();
            List<ClientModels.TransferItem> list;
            synchronized (lock) {
                if (!isCurrentGenerationLocked(completed.connectionId())) return;
                UUID key = resolveTransferKeyLocked(r.transferId());
                if (key != null) {
                    ClientModels.TransferItem prev = transfersByKey.get(key);
                    if (prev != null) {
                        boolean isIncoming = prev.direction() == ClientModels.TransferDirection.INCOMING;
                        String detail = isIncoming
                                ? (verified ? "Saved and SHA-256 verified" : "Saved")
                                : (verified ? "Recipient confirmed successful delivery and SHA-256 verification"
                                        : "Recipient confirmed successful completion");
                        ClientModels.TransferItem updated = new ClientModels.TransferItem(
                                prev.itemKey(), r.transferId(), prev.direction(),
                                prev.peerClientId(), prev.peerLabel(), prev.fileName(),
                                r.localFile() != null ? r.localFile() : prev.localFile(),
                                r.bytesTransferred(), Math.max(prev.totalBytes(), r.bytesTransferred()),
                                ClientModels.TransferStatus.COMPLETED, detail, r.sha256(), verified, 0L);
                        transfersByKey.put(key, updated);
                    }
                }
                list = transferHistoryLocked();
            }
            emitTransfers(completed.connectionId(), list, false, null);
        } else if (event instanceof FileTransferClientAdapter.TransferEvent.Rejected rejected) {
            updateTransferState(rejected.connectionId(), rejected.transferId(),
                    ClientModels.TransferStatus.REJECTED, safeReason(rejected.reason(), "Rejected"), null, null, false, true);
        } else if (event instanceof FileTransferClientAdapter.TransferEvent.Cancelled cancelled) {
            updateTransferState(cancelled.connectionId(), cancelled.transferId(),
                    ClientModels.TransferStatus.CANCELLED, safeReason(cancelled.reason(), "Cancelled"), null, null, false, true);
        } else if (event instanceof FileTransferClientAdapter.TransferEvent.Failed failed) {
            updateTransferState(failed.connectionId(), failed.transferId(),
                    ClientModels.TransferStatus.FAILED, safeReason(failed.reason(), "Transfer failed"), null, null, false, true);
        } else if (event instanceof FileTransferClientAdapter.TransferEvent.Disconnected disc) {
            if (disc.hadActiveTransfer()) {
                notifyNotice("WARNING", "Active file transfer was cancelled due to disconnect.");
            }
        }
    }

    // --- State helpers ---

    private void updateOutgoingStatus(UUID generation, UUID requestId, UUID recipientId,
                                      ClientModels.DeliveryStatus status, String detail) {
        List<ClientModels.ChatMessageItem> updated;
        synchronized (lock) {
            if (!isCurrentGenerationLocked(generation)) return;
            UUID peerId = requestToPeer.remove(requestId);
            if (peerId == null) peerId = recipientId;
            ArrayDeque<ClientModels.ChatMessageItem> deque = conversations.get(peerId);
            if (deque == null) return;
            ArrayDeque<ClientModels.ChatMessageItem> rebuilt = new ArrayDeque<>(deque.size());
            for (ClientModels.ChatMessageItem item : deque) {
                if (requestId.equals(item.requestId())) {
                    rebuilt.addLast(item.withStatus(status, detail));
                } else {
                    rebuilt.addLast(item);
                }
            }
            conversations.put(peerId, rebuilt);
            updated = List.copyOf(rebuilt);
        }
        emitConversation(generation, recipientId, updated);
    }

    private void updateTransferState(UUID generation, UUID transferId, ClientModels.TransferStatus newStatus,
                                     String detail, Path localFile, String sha256, boolean verified, boolean clearPendingKey) {
        List<ClientModels.TransferItem> list;
        synchronized (lock) {
            if (!isCurrentGenerationLocked(generation)) return;
            UUID key = resolveTransferKeyLocked(transferId);
            if (key != null) {
                ClientModels.TransferItem prev = transfersByKey.get(key);
                if (prev != null) {
                    ClientModels.TransferItem updated = new ClientModels.TransferItem(
                            prev.itemKey(), transferId != null ? transferId : prev.transferId(), prev.direction(),
                            prev.peerClientId(), prev.peerLabel(), prev.fileName(),
                            localFile != null ? localFile : prev.localFile(),
                            prev.bytesTransferred(), prev.totalBytes(),
                            newStatus, detail, sha256 != null ? sha256 : prev.sha256(), verified);
                    transfersByKey.put(key, updated);
                }
                if (clearPendingKey && key.equals(pendingOutgoingTransferKey)) {
                    pendingOutgoingTransferKey = null;
                }
            }
            list = transferHistoryLocked();
        }
        emitTransfers(generation, list, false, null);
    }

    private UUID resolveTransferKeyLocked(UUID transferId) {
        if (transferId != null) {
            UUID mapped = transferIdToKey.get(transferId);
            if (mapped != null) return mapped;
            if (transfersByKey.containsKey(transferId)) return transferId;
        }
        return pendingOutgoingTransferKey;
    }

    private void putTransferLocked(UUID key, ClientModels.TransferItem item) {
        while (transfersByKey.size() >= MAX_TRANSFER_HISTORY) {
            UUID oldest = transfersByKey.keySet().iterator().next();
            ClientModels.TransferItem removed = transfersByKey.remove(oldest);
            if (removed != null && removed.transferId() != null) {
                transferIdToKey.remove(removed.transferId());
            }
        }
        transfersByKey.put(key, item);
    }

    private void appendMessageLocked(UUID peerClientId, ClientModels.ChatMessageItem item) {
        ArrayDeque<ClientModels.ChatMessageItem> deque = conversations.get(peerClientId);
        if (deque == null) {
            while (conversations.size() >= MAX_CONVERSATIONS) {
                UUID oldestPeer = conversations.keySet().iterator().next();
                conversations.remove(oldestPeer);
            }
            deque = new ArrayDeque<>(MAX_MESSAGES_PER_CONVERSATION);
            conversations.put(peerClientId, deque);
        }
        while (deque.size() >= MAX_MESSAGES_PER_CONVERSATION) {
            deque.removeFirst();
        }
        deque.addLast(item);
    }

    private String resolveUsernameLocked(UUID clientId) {
        ClientModels.UserItem user = usersByClientId.get(clientId);
        if (user != null) {
            return user.username();
        }
        String raw = clientId.toString();
        return "User " + raw.substring(0, Math.min(8, raw.length()));
    }

    private void failPendingLogin(String message) {
        CompletableFuture<SessionView> loginFuture;
        synchronized (lock) {
            loginFuture = pendingLoginFuture;
            pendingLoginFuture = null;
            currentSession = null;
            currentGeneration = null;
            phase = ClientModels.ConnectionPhase.DISCONNECTED;
        }
        dispatcher.setActiveGeneration(null);
        emitPhase(ClientModels.ConnectionPhase.DISCONNECTED, null, message);
        if (loginFuture != null) {
            loginFuture.completeExceptionally(new IllegalStateException(message));
        }
    }

    private void completeDisconnectState(String statusMessage) {
        CompletableFuture<SessionView> loginFuture;
        CompletableFuture<Void> logoutFuture;
        synchronized (lock) {
            clearSessionDataLocked();
            currentSession = null;
            currentGeneration = null;
            phase = ClientModels.ConnectionPhase.DISCONNECTED;
            loginFuture = pendingLoginFuture;
            pendingLoginFuture = null;
            logoutFuture = pendingLogoutFuture;
            pendingLogoutFuture = null;
        }
        dispatcher.setActiveGeneration(null);
        emitPhase(ClientModels.ConnectionPhase.DISCONNECTED, null, statusMessage);
        emitUsers();
        emitTransfers();
        if (loginFuture != null) {
            loginFuture.completeExceptionally(new IllegalStateException(statusMessage));
        }
        if (logoutFuture != null) {
            logoutFuture.complete(null);
        }
    }

    private void clearSessionDataLocked() {
        usersByClientId.clear();
        conversations.clear();
        requestToPeer.clear();
        transfersByKey.clear();
        transferIdToKey.clear();
        pendingOutgoingTransferKey = null;
    }

    private boolean isCurrentGenerationLocked(UUID generation) {
        return !closed && currentGeneration != null && currentGeneration.equals(generation);
    }

    private List<ClientModels.UserItem> sortedUsersLocked() {
        List<ClientModels.UserItem> list = new ArrayList<>(usersByClientId.values());
        list.sort(Comparator.comparing((ClientModels.UserItem u) -> u.state() == PresenceState.ONLINE ? 0 : 1)
                .thenComparing(ClientModels.UserItem::username, String.CASE_INSENSITIVE_ORDER));
        return List.copyOf(list);
    }

    private List<ClientModels.ChatMessageItem> conversationSnapshotLocked(UUID peerId) {
        ArrayDeque<ClientModels.ChatMessageItem> deque = conversations.get(peerId);
        return deque == null ? List.of() : List.copyOf(deque);
    }

    private List<ClientModels.TransferItem> transferHistoryLocked() {
        List<ClientModels.TransferItem> list = new ArrayList<>(transfersByKey.values());
        java.util.Collections.reverse(list);
        return List.copyOf(list);
    }

    private void emitPhase(ClientModels.ConnectionPhase newPhase, SessionView session, String status) {
        Listener current = this.listener;
        if (current != null) {
            dispatcher.postImportant(null, () -> current.onConnectionPhaseChanged(newPhase, session, status));
        }
    }

    private void emitUsers() {
        Listener current = this.listener;
        if (current != null) {
            List<ClientModels.UserItem> snapshot;
            synchronized (lock) { snapshot = sortedUsersLocked(); }
            dispatcher.postImportant(null, () -> current.onUsersChanged(snapshot));
        }
    }

    private void emitTransfers() {
        Listener current = this.listener;
        if (current != null) {
            List<ClientModels.TransferItem> snapshot;
            synchronized (lock) { snapshot = transferHistoryLocked(); }
            dispatcher.postImportant(null, () -> current.onTransfersChanged(snapshot));
        }
    }

    private void emitTransfers(UUID generation, List<ClientModels.TransferItem> snapshot, boolean coalescable, UUID transferId) {
        Listener current = this.listener;
        if (current != null) {
            if (coalescable && transferId != null) {
                dispatcher.postTransferProgress(generation, transferId, () -> current.onTransfersChanged(snapshot));
            } else {
                dispatcher.postImportant(generation, () -> current.onTransfersChanged(snapshot));
            }
        }
    }

    private void emitConversation(UUID generation, UUID peerId, List<ClientModels.ChatMessageItem> snapshot) {
        Listener current = this.listener;
        if (current != null) {
            dispatcher.postImportant(generation, () -> current.onConversationUpdated(peerId, snapshot));
        }
    }

    private void notifyNotice(String severity, String message) {
        Listener current = this.listener;
        if (current != null) {
            dispatcher.postImportant(null, () -> current.onNotice(severity, message));
        }
    }

    private static boolean isTerminal(ClientModels.TransferStatus status) {
        return status == ClientModels.TransferStatus.COMPLETED
                || status == ClientModels.TransferStatus.REJECTED
                || status == ClientModels.TransferStatus.CANCELLED
                || status == ClientModels.TransferStatus.FAILED;
    }

    private static String safeReason(String reason, String fallback) {
        return reason == null || reason.isBlank() ? fallback : reason;
    }

    private static String describeLoginError(ErrorCode code, String fallback) {
        if (code == null) return fallback;
        return switch (code) {
            case AUTH_FAILED -> "Invalid username or password, or account already active.";
            case INVALID_STATE -> "This account or connection is already signed in.";
            case BUSY -> "Server authentication workers are busy; retry shortly.";
            default -> "Sign-in failed (" + code + ").";
        };
    }

    private static String mapConnectLoginFailure(Exception failure) {
        if (failure instanceof CommandRegistry.InputException input) {
            return input.getMessage();
        }
        if (failure instanceof IOException) {
            return "Could not connect to NetSync server. Verify that the server is running on the configured host and TCP port.";
        }
        return failure.getMessage() != null ? failure.getMessage() : "Connection or sign-in failed.";
    }

    // --- Public read-only accessors for views & tests ---

    public ClientModels.ConnectionPhase phase() {
        synchronized (lock) { return phase; }
    }

    public Optional<SessionView> currentSession() {
        synchronized (lock) { return Optional.ofNullable(currentSession); }
    }

    private SessionView currentSessionSnapshot() {
        synchronized (lock) { return currentSession; }
    }

    public List<ClientModels.UserItem> usersSnapshot() {
        synchronized (lock) { return sortedUsersLocked(); }
    }

    public List<ClientModels.ChatMessageItem> conversationSnapshot(UUID peerClientId) {
        synchronized (lock) { return conversationSnapshotLocked(peerClientId); }
    }

    public List<ClientModels.TransferItem> transfersSnapshot() {
        synchronized (lock) { return transferHistoryLocked(); }
    }

    public String currentHost() { synchronized (lock) { return currentHost; } }
    public int currentTcpPort() { synchronized (lock) { return currentTcpPort; } }
    public int currentUdpPort() { synchronized (lock) { return currentUdpPort; } }
    public int heartbeatIntervalSeconds() { synchronized (lock) { return heartbeatIntervalSeconds; } }
    public int presenceTimeoutSeconds() { synchronized (lock) { return presenceTimeoutSeconds; } }
    public boolean isSendingHeartbeats() { return presence.isSendingHeartbeats(); }
    public TcpClient transport() { return transport; }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) return;
            closed = true;
            clearSessionDataLocked();
            currentSession = null;
            currentGeneration = null;
            phase = ClientModels.ConnectionPhase.DISCONNECTED;
            if (pendingLoginFuture != null) {
                pendingLoginFuture.completeExceptionally(new IllegalStateException("Client closed."));
                pendingLoginFuture = null;
            }
            if (pendingLogoutFuture != null) {
                pendingLogoutFuture.complete(null);
                pendingLogoutFuture = null;
            }
        }
        dispatcher.close();
        for (ClientDispatcher.Registration reg : registrations) {
            try { reg.close(); } catch (RuntimeException ignored) {}
        }
        registrations.clear();
        fileTransfer.close();
        chat.close();
        presence.close();
        authentication.close();
        transport.close();
        backgroundOps.shutdownNow();
        try {
            backgroundOps.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
