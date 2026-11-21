package com.netsync.server;

import com.netsync.common.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** Real loopback transport with a virtual monotonic clock; no ten-second sleeps. */
@Timeout(10)
class ProtocolGuardsTest {
    private static final FrameCodec CODEC = new FrameCodec(new MessageCodec());

    private static MessageEnvelope request(UUID id, MessageType type) {
        return new MessageEnvelope(1, type, id, null, new MessageBody.Empty());
    }

    private static MessageEnvelope request() {
        return request(UUID.randomUUID(), MessageType.PRESENCE_QUERY);
    }

    private static byte[] encode(OutboundFrame frame) throws Exception {
        ByteBuffer buffer = CODEC.encode(frame);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static void write(Socket socket, OutboundFrame... frames) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (OutboundFrame frame : frames) bytes.write(encode(frame));
        socket.getOutputStream().write(bytes.toByteArray());
    }

    private static void json(Socket socket, String text) throws Exception {
        byte[] body = text.getBytes(StandardCharsets.UTF_8);
        socket.getOutputStream().write(ByteBuffer.allocate(5 + body.length)
                .putInt(1 + body.length).put(ProtocolLimits.JSON_KIND).put(body).array());
    }

    private static MessageEnvelope read(Socket socket) throws Exception {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int length = input.readInt();
        byte[] payload = input.readNBytes(length);
        assertEquals(length, payload.length);
        return assertInstanceOf(MessageEnvelope.class, CODEC.decodePayload(ByteBuffer.wrap(payload)));
    }

    private static void error(Socket socket, UUID id, ErrorCode code) throws Exception {
        MessageEnvelope response = read(socket);
        assertEquals(MessageType.ERROR, response.type());
        assertEquals(id, response.replyTo());
        assertNotEquals(id, response.messageId());
        assertEquals(code, assertInstanceOf(MessageBody.Error.class, response.body()).code());
    }

    private static final class Registry implements SessionRegistry {
        final Map<UUID, SessionView> entries = new ConcurrentHashMap<>();
        public Optional<SessionView> findByConnectionId(UUID id) { return Optional.ofNullable(entries.get(id)); }
        public Optional<SessionView> findByClientId(UUID id) {
            return entries.values().stream().filter(s -> s.clientId().equals(id)).findFirst();
        }
        public List<SessionView> activeSessions() { return List.copyOf(entries.values()); }
    }

    private static final class Fixture implements AutoCloseable {
        final AtomicLong clock = new AtomicLong(1234);
        final Registry registry = new Registry();
        final BlockingQueue<MessageEnvelope> handled = new LinkedBlockingQueue<>();
        final BlockingQueue<FileChunk> chunks = new LinkedBlockingQueue<>();
        final BlockingQueue<UUID> disconnected = new LinkedBlockingQueue<>();
        final NioServer server;

        Fixture() throws Exception { this(false, false); }

        Fixture(boolean applicationError, boolean throwHandler) throws Exception {
            server = new NioServer(0, nio -> (context, message) -> {
                handled.add(message);
                if (throwHandler) throw new IllegalStateException("Fixture failure");
                if (applicationError) {
                    nio.send(context.connectionId(), new MessageEnvelope(1, MessageType.ERROR,
                            UUID.randomUUID(), message.messageId(),
                            new MessageBody.Error(ErrorCode.UNAUTHENTICATED, "Authentication required", null)));
                } else nio.send(context.connectionId(), message);
            }, (context, chunk) -> chunks.add(chunk), clock::get);
            server.withSessionRegistry(registry);
            server.addDisconnectListener(id -> {
                disconnected.add(id);
                registry.entries.remove(id);
            });
            server.start();
        }

        <T> T task(Supplier<T> action) throws Exception {
            CompletableFuture<T> result = new CompletableFuture<>();
            assertTrue(server.selectorTasks().submit(() -> {
                try { result.complete(action.get()); }
                catch (Throwable failure) { result.completeExceptionally(failure); }
            }));
            return result.get(3, TimeUnit.SECONDS);
        }

        // Inspect only on the owning Selector, also synchronizing acceptance of idle sockets.
        @SuppressWarnings("unchecked")
        List<ConnectionState> states() throws Exception {
            return task(() -> {
                try {
                    var field = NioServer.class.getDeclaredField("connections");
                    field.setAccessible(true);
                    var states = (Map<UUID, ConnectionState>) field.get(server);
                    return List.copyOf(states.values());
                } catch (ReflectiveOperationException e) { throw new AssertionError(e); }
            });
        }

        Socket connect() throws Exception {
            Set<UUID> before = new HashSet<>();
            for (ConnectionState state : states()) before.add(state.connectionId());
            Socket socket = new Socket("127.0.0.1", server.port());
            socket.setSoTimeout(3000);
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            try {
                while (System.nanoTime() < until) {
                    if (states().stream().anyMatch(s -> !before.contains(s.connectionId()))) return socket;
                    Thread.sleep(1);
                }
                throw new AssertionError("Connection not accepted");
            } catch (Throwable failure) { socket.close(); throw failure; }
        }

        ConnectionState onlyState() throws Exception { return states().getFirst(); }

        void authenticate(ConnectionState state) {
            registry.entries.put(state.connectionId(), new SessionView(UUID.randomUUID(), "alice", state.connectionId()));
        }

        void advance(long seconds) throws Exception {
            clock.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
            task(() -> null); // wakes an idle Selector; timeout check runs before this task
        }

        void awaitState(BooleanSupplier condition) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < until) {
                if (task(condition::getAsBoolean)) return;
                Thread.sleep(1);
            }
            fail("Selector state did not reach expected condition");
        }

        void echo(Socket socket) throws Exception {
            MessageEnvelope message = request();
            write(socket, message);
            assertEquals(message, read(socket));
        }

        public void close() { server.close(); assertNull(server.failure()); }
    }

    @Test void idleUnauthenticatedConnectionExpiresExactlyAtDeadline() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            ConnectionState state = f.onlyState();
            assertEquals(1234, state.acceptedAt);
            f.advance(9);
            assertFalse(f.task(() -> state.closed));
            // No socket traffic or task wakeup: the periodic select timeout must enforce this.
            f.clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
            assertEquals(-1, socket.getInputStream().read());
            assertEquals(state.connectionId(), f.disconnected.poll(3, TimeUnit.SECONDS));
        }
    }

    @Test void authenticatedConnectionSurvivesLoginDeadline() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            f.authenticate(f.onlyState());
            f.advance(11);
            f.echo(socket);
            assertTrue(f.disconnected.isEmpty());
        }
    }

    @Test void partialJsonProgressDoesNotExtendLoginDeadline() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            ConnectionState state = f.onlyState();
            byte[] frame = encode(request());
            socket.getOutputStream().write(frame, 0, 6);
            f.awaitState(() -> state.payload != null && state.payload.position() == 2);
            f.advance(9);
            socket.getOutputStream().write(frame, 6, 1);
            f.awaitState(() -> state.payload != null && state.payload.position() == 3);
            f.advance(1);
            assertEquals(-1, socket.getInputStream().read());
            assertTrue(f.handled.isEmpty());
        }
    }

    @Test void repeatedUnauthenticatedTrafficDoesNotExtendDeadline() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            for (int i = 0; i < 3; i++) { f.advance(3); f.echo(socket); }
            assertEquals(3, f.handled.size());
            f.advance(1);
            assertEquals(-1, socket.getInputStream().read());
        }
    }

    @Test void timeoutCleansStateAndInvokesDisconnectListenerOnce() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            f.echo(socket);
            ConnectionState state = f.onlyState();
            f.advance(10);
            assertEquals(-1, socket.getInputStream().read());
            f.task(() -> {
                assertTrue(state.closed);
                assertTrue(state.recentJsonIds.isEmpty());
                assertTrue(state.output.isEmpty());
                assertNull(state.payload);
                assertNull(state.key.attachment());
                return null;
            });
            f.advance(20);
            assertTrue(f.states().isEmpty());
            assertEquals(List.of(state.connectionId()), List.copyOf(f.disconnected));
            assertFalse(f.task(() -> f.server.closeConnection(state.connectionId())));
        }
    }

    @Test void expiryDoesNotCloseAnotherAuthenticatedConnection() throws Exception {
        try (Fixture f = new Fixture(); Socket idle = f.connect()) {
            ConnectionState first = f.onlyState();
            try (Socket healthy = f.connect()) {
                ConnectionState second = f.states().stream().filter(s -> s != first).findFirst().orElseThrow();
                f.authenticate(second);
                f.advance(10);
                assertEquals(-1, idle.getInputStream().read());
                f.echo(healthy);
                assertEquals(List.of(first.connectionId()), List.copyOf(f.disconnected));
            }
        }
    }

    @Test void pendingLoginExpiresAndLateCompletionCannotReviveTransport() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            MessageEnvelope login = new MessageEnvelope(1, MessageType.LOGIN_REQUEST, UUID.randomUUID(), null,
                    new MessageBody.LoginRequest("alice", "disposable"));
            write(socket, login);
            assertEquals(login, read(socket)); // fixture accepts request without establishing a session
            ConnectionState state = f.onlyState();
            f.advance(10);
            assertEquals(-1, socket.getInputStream().read());
            f.authenticate(state); // model late external session publication
            assertEquals(EnqueueResult.REJECTED,
                    f.task(() -> f.server.send(state.connectionId(), request())));
            assertTrue(state.session().isEmpty());
            assertTrue(f.states().isEmpty());
        }
    }

    @Test void sessionVisibleAtDeadlineCheckWinsBoundaryRace() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            ConnectionState state = f.onlyState();
            f.task(() -> { f.authenticate(state); f.clock.addAndGet(TimeUnit.SECONDS.toNanos(10)); return null; });
            f.echo(socket);
            assertTrue(f.disconnected.isEmpty());
        }
    }

    @Test void mismatchedSessionSnapshotDoesNotPreventExpiry() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            ConnectionState state = f.onlyState();
            f.registry.entries.put(state.connectionId(), new SessionView(UUID.randomUUID(), "alice", UUID.randomUUID()));
            f.advance(10);
            assertEquals(-1, socket.getInputStream().read());
        }
    }

    @Test void shutdownDiscardsPendingDeadlinesAndIdHistory() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            f.echo(socket);
            ConnectionState state = f.onlyState();
            f.server.close();
            assertEquals(-1, socket.getInputStream().read());
            assertTrue(state.closed);
            assertTrue(state.recentJsonIds.isEmpty());
            assertEquals(1, f.disconnected.size());
        }
    }

    @Test void duplicateIsCorrelatedRejectedAndNeverDispatchedTwice() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            MessageEnvelope message = request();
            write(socket, message, message);
            assertEquals(message, read(socket));
            error(socket, message.messageId(), ErrorCode.DUPLICATE_MESSAGE);
            assertEquals(List.of(message), List.copyOf(f.handled));
            f.echo(socket); // duplicate is recoverable
        }
    }

    @Test void sameIdOnDifferentConnectionsIsIndependent() throws Exception {
        try (Fixture f = new Fixture(); Socket first = f.connect(); Socket second = f.connect()) {
            MessageEnvelope message = request();
            write(first, message); write(second, message);
            assertEquals(message, read(first)); assertEquals(message, read(second));
            assertEquals(2, f.handled.size());
        }
    }

    @Test void differentJsonTypesShareHistory() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            UUID id = UUID.randomUUID();
            MessageEnvelope first = request(id, MessageType.PRESENCE_QUERY);
            write(socket, first, request(id, MessageType.LOGOUT_REQUEST));
            assertEquals(first, read(socket));
            error(socket, id, ErrorCode.DUPLICATE_MESSAGE);
            assertEquals(1, f.handled.size());
        }
    }

    @Test void reconnectStartsFreshAndDisconnectReleasesHistory() throws Exception {
        try (Fixture f = new Fixture()) {
            MessageEnvelope message = request();
            ConnectionState old;
            try (Socket first = f.connect()) {
                write(first, message); assertEquals(message, read(first)); old = f.onlyState();
            }
            assertEquals(old.connectionId(), f.disconnected.poll(3, TimeUnit.SECONDS));
            f.task(() -> { assertTrue(old.recentJsonIds.isEmpty()); return null; });
            try (Socket second = f.connect()) {
                write(second, message); assertEquals(message, read(second));
                assertNotEquals(old.connectionId(), f.onlyState().connectionId());
                assertEquals(2, f.handled.size());
            }
        }
    }

    @Test void fullHistoryEvictsOldestOnWire() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            ConnectionState state = f.onlyState();
            f.authenticate(state);
            List<MessageEnvelope> messages = new ArrayList<>();
            for (int i = 0; i < ConnectionState.RECENT_JSON_IDS; i++) messages.add(request());
            // Read each response to avoid making this an output-saturation test.
            for (MessageEnvelope message : messages) { write(socket, message); assertEquals(message, read(socket)); }
            write(socket, messages.getFirst());
            error(socket, messages.getFirst().messageId(), ErrorCode.DUPLICATE_MESSAGE);
            f.echo(socket); // consumes the 1,025th distinct ID
            write(socket, messages.getFirst());
            assertEquals(messages.getFirst(), read(socket));
            assertEquals(1026, f.handled.size());
            assertEquals(1024, f.task(() -> state.recentJsonIds.size()));
        }
    }

    @Test void invalidBodyDoesNotConsumeDecodedEnvelopeId() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            UUID id = UUID.randomUUID();
            json(socket, "{\"version\":1,\"type\":\"PRESENCE_QUERY\",\"messageId\":\"" + id
                    + "\",\"body\":{\"unexpected\":true}}");
            error(socket, id, ErrorCode.INVALID_REQUEST);
            MessageEnvelope valid = request(id, MessageType.PRESENCE_QUERY);
            write(socket, valid); assertEquals(valid, read(socket));
            assertEquals(1, f.handled.size());
        }
    }

    @Test void malformedJsonClosesWithoutConsumingId() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            ConnectionState state = f.onlyState();
            json(socket, "{\"messageId\":\"" + UUID.randomUUID() + "\",");
            MessageEnvelope response = read(socket);
            assertEquals(ErrorCode.MALFORMED_MESSAGE, ((MessageBody.Error) response.body()).code());
            assertEquals(-1, socket.getInputStream().read());
            assertTrue(f.handled.isEmpty());
            assertTrue(state.recentJsonIds.isEmpty());
        }
    }

    @Test void binaryChunksNeitherConsumeNorConsultJsonHistory() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            UUID id = UUID.randomUUID();
            FileChunk chunk = new FileChunk(id, UUID.randomUUID(), 0, new byte[]{1});
            write(socket, chunk, request(id, MessageType.PRESENCE_QUERY), chunk);
            assertEquals(id, read(socket).messageId());
            assertNotNull(f.chunks.poll(3, TimeUnit.SECONDS));
            assertNotNull(f.chunks.poll(3, TimeUnit.SECONDS));
            assertEquals(1, f.handled.size());
        }
    }

    @Test void applicationErrorStillConsumesId() throws Exception {
        try (Fixture f = new Fixture(true, false); Socket socket = f.connect()) {
            MessageEnvelope message = request();
            write(socket, message, message);
            error(socket, message.messageId(), ErrorCode.UNAUTHENTICATED);
            error(socket, message.messageId(), ErrorCode.DUPLICATE_MESSAGE);
            assertEquals(1, f.handled.size());
        }
    }

    @Test void thrownHandlerClosesConnectionWithoutRunningDuplicate() throws Exception {
        try (Fixture f = new Fixture(false, true); Socket socket = f.connect()) {
            MessageEnvelope message = request();
            write(socket, message, message);
            assertEquals(-1, socket.getInputStream().read());
            assertEquals(1, f.handled.size());
            assertNotNull(f.disconnected.poll(3, TimeUnit.SECONDS));
        }
    }

    @Test void mixedValidAndDuplicateFramesPreserveDispatchAndResponseOrder() throws Exception {
        try (Fixture f = new Fixture(); Socket socket = f.connect()) {
            MessageEnvelope a = request(), b = request(), c = request();
            write(socket, a, a, b, a, c);
            assertEquals(a, read(socket)); error(socket, a.messageId(), ErrorCode.DUPLICATE_MESSAGE);
            assertEquals(b, read(socket)); error(socket, a.messageId(), ErrorCode.DUPLICATE_MESSAGE);
            assertEquals(c, read(socket));
            assertEquals(List.of(a, b, c), List.copyOf(f.handled));
        }
    }

    @Test void fifoWindowIsBoundedAndDuplicatesDoNotRefreshRetention() {
        ConnectionState state = new ConnectionState(null, null, 0);
        UUID first = UUID.randomUUID();
        assertTrue(state.acceptJsonId(first));
        for (int i = 1; i < 1024; i++) assertTrue(state.acceptJsonId(UUID.randomUUID()));
        assertFalse(state.acceptJsonId(first));
        assertTrue(state.acceptJsonId(UUID.randomUUID()));
        assertEquals(1024, state.recentJsonIds.size());
        assertFalse(state.recentJsonIds.contains(first));
        assertTrue(state.acceptJsonId(first));
        assertEquals(1024, state.recentJsonIds.size());
    }
}
