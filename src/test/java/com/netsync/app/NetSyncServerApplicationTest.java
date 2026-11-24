package com.netsync.app;

import com.netsync.common.*;
import com.netsync.security.CredentialStore;
import com.netsync.server.MonotonicServerTestSupport;
import com.netsync.server.NioServer;
import com.netsync.transfer.TransferWorkerPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.io.DataInputStream;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class NetSyncServerApplicationTest {
    private static final FrameCodec CODEC = new FrameCodec(new MessageCodec());

    private static DemoServerConfig config(int port) {
        return new DemoServerConfig(true, port, List.of("alice", "bob"));
    }

    private static CredentialStore accounts() {
        return DemoAccounts.provision(config(0), user -> ("disposable-" + user).toCharArray());
    }

    private static NetSyncServerApplication application() throws Exception {
        NetSyncServerApplication app = new NetSyncServerApplication(config(0), accounts());
        app.start();
        return app;
    }

    private static Socket connect(NetSyncServerApplication app) throws Exception {
        Socket socket = new Socket("127.0.0.1", app.port());
        socket.setSoTimeout(4000);
        assertTrue(socket.getInetAddress().isLoopbackAddress());
        return socket;
    }

    private static MessageEnvelope loginRequest(String user, String password) {
        return new MessageEnvelope(1, MessageType.LOGIN_REQUEST, UUID.randomUUID(), null,
                new MessageBody.LoginRequest(user, password));
    }

    private static MessageEnvelope empty(MessageType type) {
        return new MessageEnvelope(1, type, UUID.randomUUID(), null, new MessageBody.Empty());
    }

    private static void write(Socket socket, OutboundFrame frame) throws Exception {
        ByteBuffer buffer = CODEC.encode(frame);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        socket.getOutputStream().write(bytes);
    }

    private static MessageEnvelope read(Socket socket) throws Exception {
        MessageEnvelope message;
        do { message = readRaw(socket); }
        while (message.type() == MessageType.PRESENCE_UPDATE);
        return message;
    }

    private static MessageEnvelope readRaw(Socket socket) throws Exception {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int length = CODEC.decodeLength(ByteBuffer.wrap(input.readNBytes(4)));
        byte[] payload = input.readNBytes(length);
        assertEquals(length, payload.length);
        return assertInstanceOf(MessageEnvelope.class, CODEC.decodePayload(ByteBuffer.wrap(payload)));
    }

    private static void drainUntilEof(Socket socket) throws Exception {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        while (input.read() >= 0) { /* shutdown may flush queued presence frames first */ }
    }

    private static MessageBody.LoginResponse login(Socket socket, String user) throws Exception {
        MessageEnvelope request = loginRequest(user, "disposable-" + user);
        write(socket, request);
        MessageEnvelope response = read(socket);
        assertEquals(MessageType.LOGIN_RESPONSE, response.type());
        assertEquals(request.messageId(), response.replyTo());
        return assertInstanceOf(MessageBody.LoginResponse.class, response.body());
    }

    private static void error(MessageEnvelope response, UUID replyTo, ErrorCode code) {
        assertEquals(MessageType.ERROR, response.type());
        assertEquals(replyTo, response.replyTo());
        assertEquals(code, assertInstanceOf(MessageBody.Error.class, response.body()).code());
    }

    private static void eventually(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(5);
        assertTrue(condition.getAsBoolean());
    }

    @Test void validLoginCreatesAuthoritativeSessionAndToken() throws Exception {
        try (var app = application(); Socket socket = connect(app)) {
            var response = login(socket, "alice");
            SessionView session = app.sessions().findByClientId(response.clientId()).orElseThrow();
            assertEquals("alice", session.username());
            assertEquals(Optional.of(session), app.sessions().findByConnectionId(session.connectionId()));
            assertTrue(app.authentication().isPresenceTokenValid(response.clientId(), response.presenceToken()));
            assertEquals(1, app.sessions().activeSessions().size());
        }
    }

    @Test void invalidLoginReturnsGenericFailureThenCloses() throws Exception {
        try (var app = application(); Socket socket = connect(app)) {
            MessageEnvelope request = loginRequest("alice", "incorrect-disposable");
            write(socket, request);
            MessageEnvelope response = read(socket);
            error(response, request.messageId(), ErrorCode.AUTH_FAILED);
            assertEquals("Authentication failed", ((MessageBody.Error) response.body()).message());
            drainUntilEof(socket);
            assertTrue(app.sessions().activeSessions().isEmpty());
        }
    }

    @Test void duplicateActiveUsernameIsRejectedWithoutDisturbingOriginalSession() throws Exception {
        try (var app = application(); Socket first = connect(app); Socket second = connect(app)) {
            var original = login(first, "alice");
            MessageEnvelope request = loginRequest("alice", "disposable-alice");
            write(second, request);
            error(read(second), request.messageId(), ErrorCode.AUTH_FAILED);
            assertEquals(-1, second.getInputStream().read());
            assertTrue(app.sessions().findByClientId(original.clientId()).isPresent());
            assertEquals(1, app.sessions().activeSessions().size());
        }
    }

    @Test void logoutDrainsAckThenClosesAndRevokesToken() throws Exception {
        try (var app = application(); Socket socket = connect(app)) {
            var identity = login(socket, "alice");
            MessageEnvelope logout = empty(MessageType.LOGOUT_REQUEST);
            write(socket, logout);
            MessageEnvelope response = read(socket);
            assertEquals(MessageType.ACK, response.type());
            assertEquals(logout.messageId(), response.replyTo());
            var ack = assertInstanceOf(MessageBody.Ack.class, response.body());
            assertEquals(MessageType.LOGOUT_REQUEST, ack.operation());
            assertEquals(AckStatus.ACCEPTED, ack.status());
            drainUntilEof(socket);
            assertTrue(app.sessions().findByClientId(identity.clientId()).isEmpty());
            assertFalse(app.authentication().isPresenceTokenValid(identity.clientId(), identity.presenceToken()));
        }
    }

    @Test void abruptTcpResetRevokesSessionAndToken() throws Exception {
        try (var app = application()) {
            MessageBody.LoginResponse identity;
            try (Socket socket = connect(app)) {
                identity = login(socket, "alice");
                socket.setSoLinger(true, 0);
            }
            eventually(() -> app.sessions().findByClientId(identity.clientId()).isEmpty());
            assertFalse(app.authentication().isPresenceTokenValid(identity.clientId(), identity.presenceToken()));
        }
    }

    @Test void liveConnectionLookupMatchesTwoRealAuthenticatedSockets() throws Exception {
        Map<UUID, ConnectionContext> contexts = new ConcurrentHashMap<>();
        NetSyncServerApplication.ServerFactory factory = (port, handlers, chunks) ->
                new NioServer(port, nio -> {
                    MessageHandler production = handlers.create(nio);
                    return (context, message) -> {
                        contexts.put(message.messageId(), context);
                        production.handle(context, message);
                    };
                }, chunks);
        try (var app = new NetSyncServerApplication(config(0), accounts(), factory, TransferWorkerPool::new)) {
            app.start();
            try (Socket first = connect(app); Socket second = connect(app)) {
                var alice = login(first, "alice");
                var bob = login(second, "bob");
                MessageEnvelope a = empty(MessageType.PRESENCE_QUERY), b = empty(MessageType.PRESENCE_QUERY);
                write(first, a); write(second, b);
                MessageEnvelope snapshotA = readUntilReply(first, a.messageId(), true);
                MessageEnvelope snapshotB = readUntilReply(second, b.messageId(), true);
                assertEquals(MessageType.PRESENCE_SNAPSHOT, snapshotA.type());
                assertEquals(MessageType.PRESENCE_SNAPSHOT, snapshotB.type());
                assertEquals(2, ((MessageBody.PresenceSnapshot) snapshotA.body()).clients().size());
                assertEquals(2, ((MessageBody.PresenceSnapshot) snapshotB.body()).clients().size());
                ConnectionContext ca = contexts.get(a.messageId()), cb = contexts.get(b.messageId());
                assertEquals(alice.clientId(), ca.session().orElseThrow().clientId());
                assertEquals(bob.clientId(), cb.session().orElseThrow().clientId());
                assertNotEquals(ca.connectionId(), cb.connectionId());
                assertEquals(ca.session(), app.sessions().findByConnectionId(ca.connectionId()));
                assertEquals(cb.session(), app.sessions().findByConnectionId(cb.connectionId()));
            }
        }
    }

    private static MessageEnvelope readUntilReply(Socket socket, UUID replyTo, boolean expectReply) throws Exception {
        MessageEnvelope message;
        do { message = readRaw(socket); }
        while (message.type() == MessageType.PRESENCE_UPDATE
                || (expectReply && !replyTo.equals(message.replyTo())));
        return message;
    }

    @Test void unauthenticatedConnectionExpiresWithProductionHandlersAndVirtualClock() throws Exception {
        AtomicLong clock = new AtomicLong();
        try (var app = new NetSyncServerApplication(config(0), accounts(),
                (port, handlers, chunks) -> MonotonicServerTestSupport.create(port, handlers, chunks, clock::get),
                TransferWorkerPool::new)) {
            app.start();
            try (Socket idle = connect(app); Socket authenticated = connect(app)) {
                var identity = login(authenticated, "alice");
                // Response guarantees the first socket was already accepted before advancing time.
                MessageEnvelope query = empty(MessageType.PRESENCE_QUERY);
                write(idle, query);
                error(read(idle), query.messageId(), ErrorCode.UNAUTHENTICATED);
                clock.addAndGet(TimeUnit.SECONDS.toNanos(10));
                assertEquals(-1, idle.getInputStream().read());
                MessageEnvelope logout = empty(MessageType.LOGOUT_REQUEST);
                write(authenticated, logout);
                assertEquals(MessageType.ACK, read(authenticated).type());
                assertTrue(app.sessions().findByClientId(identity.clientId()).isEmpty());
            }
        }
    }

    @Test void duplicateLoginIdIsRejectedByCoreBeforeAuthenticationHandler() throws Exception {
        try (var app = application(); Socket socket = connect(app)) {
            MessageEnvelope request = loginRequest("alice", "disposable-alice");
            write(socket, request);
            assertEquals(MessageType.LOGIN_RESPONSE, read(socket).type());
            write(socket, request);
            error(read(socket), request.messageId(), ErrorCode.DUPLICATE_MESSAGE);
            assertEquals(1, app.sessions().activeSessions().size());
            MessageEnvelope second = loginRequest("alice", "disposable-alice");
            write(socket, second);
            error(read(socket), second.messageId(), ErrorCode.INVALID_STATE);
        }
    }

    @Test void blockedCredentialWorkersDoNotBlockUnrelatedConnections() throws Exception {
        CountDownLatch release = new CountDownLatch(1), entered = new CountDownLatch(2);
        try (var app = application(); Socket loginSocket = connect(app); Socket other = connect(app)) {
            try {
                for (int i = 0; i < 2; i++) app.workers().submit(() -> {
                    entered.countDown();
                    try { release.await(); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                });
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                write(loginSocket, loginRequest("alice", "disposable-alice"));
                eventually(() -> app.workers().queueSize() == 1);
                MessageEnvelope query = empty(MessageType.PRESENCE_QUERY);
                write(other, query);
                error(read(other), query.messageId(), ErrorCode.UNAUTHENTICATED);
                assertTrue(app.sessions().activeSessions().isEmpty());
            } finally { release.countDown(); }
            assertEquals(MessageType.LOGIN_RESPONSE, read(loginSocket).type());
        }
    }

    @Test void shutdownClosesSocketsRevokesSessionsAndTerminatesWorkers() throws Exception {
        try (var app = application(); Socket socket = connect(app)) {
            var identity = login(socket, "alice");
            int port = app.port();
            app.close(); app.close();
            assertFalse(app.isRunning());
            assertTrue(app.workers().isTerminated());
            drainUntilEof(socket);
            assertTrue(app.sessions().activeSessions().isEmpty());
            assertFalse(app.authentication().isPresenceTokenValid(identity.clientId(), identity.presenceToken()));
            try (ServerSocket replacement = new ServerSocket(port, 1, InetAddress.getByName("127.0.0.1"))) {
                assertEquals(port, replacement.getLocalPort());
            }
        }
    }

    @Test void expiredPendingLoginCannotPublishSessionAfterWorkersResume() throws Exception {
        AtomicLong clock = new AtomicLong();
        CountDownLatch release = new CountDownLatch(1), entered = new CountDownLatch(2);
        try (var app = new NetSyncServerApplication(config(0), accounts(),
                (port, handlers, chunks) -> MonotonicServerTestSupport.create(port, handlers, chunks, clock::get),
                TransferWorkerPool::new)) {
            app.start();
            try (Socket socket = connect(app)) {
                try {
                    for (int i = 0; i < 2; i++) app.workers().submit(() -> {
                        entered.countDown();
                        try { release.await(); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    });
                    assertTrue(entered.await(3, TimeUnit.SECONDS));
                    write(socket, loginRequest("alice", "disposable-alice"));
                    eventually(() -> app.workers().queueSize() == 1);
                    clock.addAndGet(TimeUnit.SECONDS.toNanos(10));
                    assertEquals(-1, socket.getInputStream().read());
                } finally { release.countDown(); }
                eventually(() -> app.workers().queueSize() == 0 && app.workers().activeThreadCount() == 0);
                assertTrue(app.sessions().activeSessions().isEmpty());
            }
        }
    }

    @Test void shutdownCancelsQueuedAuthenticationBeforeWorkersResume() throws Exception {
        CountDownLatch release = new CountDownLatch(1), entered = new CountDownLatch(2);
        try (var app = application(); Socket socket = connect(app)) {
            CompletableFuture<Void> closing = null;
            try {
                for (int i = 0; i < 2; i++) app.workers().submit(() -> {
                    entered.countDown();
                    try { release.await(); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                });
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                write(socket, loginRequest("alice", "disposable-alice"));
                eventually(() -> app.workers().queueSize() == 1);
                closing = CompletableFuture.runAsync(app::close);
                assertEquals(-1, socket.getInputStream().read());
            } finally {
                release.countDown();
                if (closing != null) closing.get(4, TimeUnit.SECONDS);
            }
            assertTrue(app.workers().isTerminated());
            assertTrue(app.sessions().activeSessions().isEmpty());
        }
    }

    @Test void bindFailureRollsBackWorkerPoolAndCanBeClosedAgain() throws Exception {
        try (ServerSocket occupied = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            AtomicReference<TransferWorkerPool> pool = new AtomicReference<>();
            try (var app = new NetSyncServerApplication(config(occupied.getLocalPort()), accounts(), NioServer::new,
                    () -> { var workers = new TransferWorkerPool(); pool.set(workers); return workers; })) {
                assertThrows(java.io.IOException.class, app::start);
                assertFalse(app.isRunning());
                assertTrue(pool.get().isTerminated());
                assertTrue(app.sessions().activeSessions().isEmpty());
                assertThrows(IllegalStateException.class, app::start);
                app.close();
            }
        }
    }

    @Test void udpBindFailureDoesNotLeaveTcpServerOrWorkersRunning() throws Exception {
        try (DatagramSocket occupied = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0))) {
            int udpPort = occupied.getLocalPort();
            AtomicReference<TransferWorkerPool> pool = new AtomicReference<>();
            DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob"), udpPort);
            try (var app = new NetSyncServerApplication(config, accounts(), NioServer::new,
                    () -> { var workers = new TransferWorkerPool(); pool.set(workers); return workers; })) {
                assertThrows(java.io.IOException.class, app::start);
                assertFalse(app.isRunning());
                assertEquals(-1, app.port());
                assertTrue(pool.get().isTerminated());
                assertTrue(app.sessions().activeSessions().isEmpty());
                app.close();
            }
        }
    }

    @Test void constructionFailureAfterWorkerCreationRollsBackWorkers() throws Exception {
        AtomicReference<TransferWorkerPool> pool = new AtomicReference<>();
        try (var app = new NetSyncServerApplication(config(0), accounts(),
                (port, handlers, chunks) -> { throw new IllegalStateException("Injected construction failure"); },
                () -> { var workers = new TransferWorkerPool(); pool.set(workers); return workers; })) {
            assertThrows(IllegalStateException.class, app::start);
            assertTrue(pool.get().isTerminated());
            assertFalse(app.isRunning());
        }
    }

    @Test void repeatedApplicationInstancesReusePortWithoutLeavingWorkers() throws Exception {
        int port = 0;
        for (int i = 0; i < 3; i++) {
            try (var app = new NetSyncServerApplication(config(port), accounts())) {
                app.start(); port = app.port();
                try (Socket socket = connect(app)) { login(socket, "alice"); }
                app.close();
                assertTrue(app.workers().isTerminated());
                assertTrue(app.sessions().activeSessions().isEmpty());
                assertNull(app.failure());
            }
        }
    }

    @Test void closeBeforeStartIsTerminalAndAllocatesNoWorkers() {
        try (var app = new NetSyncServerApplication(config(0), accounts())) {
            app.close();
            assertNull(app.workers());
            assertFalse(app.isRunning());
            assertThrows(IllegalStateException.class, app::start);
        }
    }

    @Test void unintegratedFeaturesAreRejectedRatherThanSilentlyAccepted() throws Exception {
        try (var app = application(); Socket socket = connect(app)) {
            FileChunk chunk = new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0, new byte[]{1});
            write(socket, chunk);
            error(read(socket), chunk.messageId(), ErrorCode.UNAUTHENTICATED);
            login(socket, "alice");
            write(socket, chunk);
            error(read(socket), chunk.messageId(), ErrorCode.INVALID_REQUEST);
            MessageEnvelope query = empty(MessageType.PRESENCE_QUERY);
            write(socket, query);
            MessageEnvelope snapshot = readUntilReply(socket, query.messageId(), true);
            assertEquals(MessageType.PRESENCE_SNAPSHOT, snapshot.type());
            assertEquals(query.messageId(), snapshot.replyTo());
        }
    }
}
