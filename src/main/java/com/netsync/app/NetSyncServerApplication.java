package com.netsync.app;

import com.netsync.common.*;
import com.netsync.security.*;
import com.netsync.presence.*;
import com.netsync.chat.ChatHandler;
import com.netsync.server.MessageDispatcher;
import com.netsync.server.NioServer;
import com.netsync.transfer.TransferWorkerPool;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/** Phase 5A application owner: real authentication over the single loopback TCP transport. */
public final class NetSyncServerApplication implements AutoCloseable {
    static final String WARNING = "INSECURE DEMO: loopback only; use disposable credentials. "
            + "No transport confidentiality; unsuitable for untrusted networks or deployment.";
    private static final System.Logger LOG = System.getLogger(NetSyncServerApplication.class.getName());
    private enum State { NEW, RUNNING, CLOSED }

    @FunctionalInterface
    interface ServerFactory {
        NioServer create(int port, NioServer.HandlerFactory handlers,
                         BiConsumer<ConnectionContext, FileChunk> chunks);
    }

    private final DemoServerConfig config;
    private final SessionRegistryImpl sessions = new SessionRegistryImpl();
    private final AuthService authentication;
    private final AuthorizationServiceImpl authorization;
    private final ServerFactory serverFactory;
    private final Supplier<TransferWorkerPool> workerFactory;
    private TransferWorkerPool workers;
    private NioServer server;
    private State state = State.NEW;
    private HeartbeatTracker presence;
    private PresenceEventPublisher publisher;
    private UdpPresenceService udp;
    private final HeartbeatTracker.TimeSource presenceClock;
    private int boundUdpPort = -1;

    public NetSyncServerApplication(DemoServerConfig config, CredentialStore credentials) {
        this(config, credentials, NioServer::new, TransferWorkerPool::new);
    }

    /** Package-private construction seams retain production handlers/services in integration tests. */
    NetSyncServerApplication(DemoServerConfig config, CredentialStore credentials,
                             ServerFactory serverFactory, Supplier<TransferWorkerPool> workerFactory) {
        this(config, credentials, serverFactory, workerFactory, System::nanoTime);
    }

    NetSyncServerApplication(DemoServerConfig config, CredentialStore credentials,
                             ServerFactory serverFactory, Supplier<TransferWorkerPool> workerFactory,
                             HeartbeatTracker.TimeSource presenceClock) {
        this.config = Objects.requireNonNull(config);
        Objects.requireNonNull(credentials);
        if (credentials.accountCount() == 0 || credentials.accountCount() > ProtocolLimits.MAX_CONNECTIONS)
            throw new IllegalArgumentException("Configure disposable demo accounts before startup");
        this.authentication = new AuthService(credentials, sessions, new SessionTokenGenerator());
        this.authorization = new AuthorizationServiceImpl(sessions);
        this.serverFactory = Objects.requireNonNull(serverFactory);
        this.workerFactory = Objects.requireNonNull(workerFactory);
        this.presenceClock = Objects.requireNonNull(presenceClock);
    }

    /** Single-use owner; create another instance to restart. Returns after a successful TCP bind. */
    public synchronized void start() throws IOException {
        if (state != State.NEW) throw new IllegalStateException("Application already started or closed");
        LOG.log(System.Logger.Level.WARNING, WARNING);
        try {
            workers = Objects.requireNonNull(workerFactory.get());
            server = Objects.requireNonNull(serverFactory.create(config.port(), nio -> {
                nio.withSessionRegistry(sessions);
                publisher = new PresenceEventPublisher(nio.selectorTasks(), sessions, nio);
                presence = new HeartbeatTracker(presenceClock, publisher, sessions, authentication::isPresenceTokenValid);
                java.util.function.Consumer<SessionView> revoke = session -> {
                    presence.revokeSession(session.clientId());
                    publisher.forgetSession(session.clientId());
                };
                LoginHandler login = new LoginHandler(authentication, workers::submit,
                        nio.selectorTasks(), nio, nio, result -> {
                            if (!publisher.registerSession(result.session())
                                    || !presence.registerSession(result.session().clientId(), result.presenceToken())) {
                                publisher.forgetSession(result.session().clientId());
                                throw new IllegalStateException("Presence admission unavailable");
                            }
                        }, revoke);
                LogoutHandler logout = new LogoutHandler(authentication, authorization, nio, nio, revoke);
                nio.addDisconnectListener(login::onDisconnected);
                MessageDispatcher dispatcher = MessageDispatcher.builder()
                        .register(MessageType.LOGIN_REQUEST, login)
                        .register(MessageType.LOGOUT_REQUEST, logout)
                        .register(MessageType.PRESENCE_QUERY, new PresenceHandler(authorization, sessions, presence, nio))
                        .register(MessageType.CHAT_SEND, new ChatHandler(sessions, authorization, nio))
                        .build(nio);
                return (context, message) -> {
                    if (message.type() != MessageType.LOGIN_REQUEST
                            && authorization.authenticatedSession(context).isEmpty()) {
                        reject(nio, context, message.messageId(), null, ErrorCode.UNAUTHENTICATED);
                    } else dispatcher.handle(context, message);
                };
            }, (context, chunk) -> reject(server, context, chunk.messageId(), chunk.transferId(),
                    authorization.authenticatedSession(context).isEmpty()
                            ? ErrorCode.UNAUTHENTICATED : ErrorCode.INVALID_REQUEST)));
            udp = new UdpPresenceService(new InetSocketAddress(ProtocolLimits.BIND_ADDRESS, config.udpPort()),
                    presence, publisher::retryPending);
            // Reserve UDP without loops first; TCP only starts once both sockets can bind.
            udp.bind();
            boundUdpPort = udp.getLocalAddress().getPort();
            server.start();
            udp.start();
            state = State.RUNNING;
        } catch (IOException | RuntimeException failure) {
            close();
            throw failure;
        }
    }

    private static void reject(NioServer nio, ConnectionContext context, UUID replyTo,
                               UUID transferId, ErrorCode code) {
        nio.send(context.connectionId(), new MessageEnvelope(ProtocolLimits.VERSION, MessageType.ERROR,
                UUID.randomUUID(), replyTo, new MessageBody.Error(code,
                code == ErrorCode.UNAUTHENTICATED ? "Authentication required" : "Operation is not supported",
                transferId)));
    }

    public synchronized int port() { return server == null ? -1 : server.port(); }
    public synchronized boolean isRunning() { return state == State.RUNNING && server.isRunning() && udp.isRunning(); }
    public synchronized int udpPort() { return boundUdpPort; }
    HeartbeatTracker presenceTracker() { return presence; }
    long decodedHeartbeatsForTest() { return udp == null ? 0 : udp.decodedHeartbeatCountForTesting(); }
    public synchronized IOException failure() { return server == null ? null : server.failure(); }
    /** The one authoritative registry, exposed through its read-only shared contract. */
    public SessionRegistry sessions() { return sessions; }
    AuthService authentication() { return authentication; }
    TransferWorkerPool workers() { return workers; }

    /** Transport first cancels pending logins/revokes sessions; then stop bounded credential work. */
    @Override public synchronized void close() {
        if (state == State.CLOSED) return;
        state = State.CLOSED;
        try {
            if (udp != null) udp.stop();
            if (server != null) server.close();
        } finally {
            if (publisher != null) publisher.close();
            try {
                if (workers != null) {
                    workers.close();
                    try {
                        if (!workers.awaitTermination(1, TimeUnit.SECONDS))
                            LOG.log(System.Logger.Level.WARNING, "Credential workers have not terminated");
                    } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                }
            } finally {
                for (SessionView session : sessions.activeSessions())
                    authentication.disconnect(session.connectionId());
            }
        }
    }

    public static void main(String[] args) {
        try {
            DemoServerConfig config = DemoServerConfig.parse(args);
            System.err.println(WARNING);
            CredentialStore credentials = DemoAccounts.read(config, System.console());
            try (NetSyncServerApplication application = new NetSyncServerApplication(config, credentials)) {
                Thread hook = new Thread(application::close, "netsync-app-shutdown");
                Runtime.getRuntime().addShutdownHook(hook);
                try {
                    application.start();
                    System.out.println("NetSync server listening on 127.0.0.1 TCP " + application.port()
                            + ", UDP " + application.udpPort());
                    while (application.isRunning()) Thread.sleep(250);
                    if (application.failure() != null) throw new IOException("TCP server stopped unexpectedly");
                } finally {
                    application.close();
                    try { Runtime.getRuntime().removeShutdownHook(hook); }
                    catch (IllegalStateException shuttingDown) { /* JVM is already running the hook. */ }
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException failure) {
            // Never print arbitrary exception messages, causes, arguments or credential material.
            System.err.println("Server startup/runtime failed. Check demo options, console and TCP port.");
            System.exit(1);
        }
    }
}
