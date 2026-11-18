package com.netsync.server;

import com.netsync.common.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import static org.junit.jupiter.api.Assertions.*;

/** Socket fixtures exercise the actual decoder -> dispatcher -> registered handler path. */
@Timeout(15)
class MessageDispatcherTest {
    private final FrameCodec codec = new FrameCodec(new MessageCodec());
    private record Delivery(String route, ConnectionContext context, OutboundFrame frame, Thread thread) {}

    private NioServer start(MessageDispatcher.Builder handlers,
                            BiConsumer<ConnectionContext, FileChunk> chunks) throws IOException {
        NioServer server = new NioServer(0, handlers, chunks);
        server.start();
        return server;
    }

    private Socket connect(NioServer server) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
        socket.setSoTimeout(3000);
        return socket;
    }

    private MessageEnvelope message(MessageType type) {
        return new MessageEnvelope(1, type, UUID.randomUUID(), null, new MessageBody.Empty());
    }

    private MessageHandler record(String route, LinkedBlockingQueue<Delivery> deliveries) {
        return (context, message) -> deliveries.add(new Delivery(route, context, message, Thread.currentThread()));
    }

    private <T> T next(LinkedBlockingQueue<T> queue) throws InterruptedException {
        T result = queue.poll(3, TimeUnit.SECONDS);
        assertNotNull(result, "Expected selector callback");
        return result;
    }

    private void write(Socket socket, OutboundFrame... frames) throws Exception {
        var bytes = new ByteArrayOutputStream();
        for (OutboundFrame frame : frames) {
            ByteBuffer encoded = codec.encode(frame);
            byte[] data = new byte[encoded.remaining()];
            encoded.get(data);
            bytes.write(data);
        }
        socket.getOutputStream().write(bytes.toByteArray());
    }

    private void writeJson(Socket socket, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        socket.getOutputStream().write(ByteBuffer.allocate(5 + body.length).putInt(1 + body.length)
                .put(ProtocolLimits.JSON_KIND).put(body).array());
    }

    private MessageEnvelope read(Socket socket) throws Exception {
        var input = new DataInputStream(socket.getInputStream());
        byte[] prefix = input.readNBytes(4);
        assertEquals(4, prefix.length);
        int length = codec.decodeLength(ByteBuffer.wrap(prefix));
        byte[] payload = input.readNBytes(length);
        assertEquals(length, payload.length);
        return assertInstanceOf(MessageEnvelope.class, codec.decodePayload(ByteBuffer.wrap(payload)));
    }

    private void assertError(MessageEnvelope response, UUID requestId, ErrorCode code) {
        assertEquals(MessageType.ERROR, response.type());
        assertEquals(requestId, response.replyTo());
        assertNotEquals(requestId, response.messageId());
        assertEquals(code, assertInstanceOf(MessageBody.Error.class, response.body()).code());
    }

    @Test void routesTypesInOrderWithOriginalContextAndSelectorOwnedResponses() throws Exception {
        var deliveries = new LinkedBlockingQueue<Delivery>();
        var enqueues = new LinkedBlockingQueue<EnqueueResult>();
        var owner = new AtomicReference<NioServer>();
        MessageHandler presence = (context, message) -> {
            record("presence", deliveries).handle(context, message);
            enqueues.add(owner.get().send(context.connectionId(), message)); // Transport-only echo fixture.
        };
        MessageHandler logout = (context, message) -> {
            record("logout", deliveries).handle(context, message);
            enqueues.add(owner.get().send(context.connectionId(), message));
        };
        var handlers = MessageDispatcher.builder().register(MessageType.PRESENCE_QUERY, presence)
                .register(MessageType.LOGOUT_REQUEST, logout);
        try (NioServer server = start(handlers, (context, chunk) -> {})) {
            owner.set(server);
            try (Socket client = connect(server)) {
                var requests = List.of(message(MessageType.PRESENCE_QUERY), message(MessageType.LOGOUT_REQUEST),
                        message(MessageType.PRESENCE_QUERY));
                write(client, requests.toArray(OutboundFrame[]::new));
                ConnectionContext original = null;
                Thread selectorThread = null;
                for (int i = 0; i < requests.size(); i++) {
                    Delivery result = next(deliveries);
                    assertEquals(i == 1 ? "logout" : "presence", result.route());
                    assertEquals(requests.get(i), result.frame());
                    if (original == null) { original = result.context(); selectorThread = result.thread(); }
                    assertSame(original, result.context());
                    assertSame(selectorThread, result.thread());
                    assertNotSame(Thread.currentThread(), result.thread());
                    assertTrue(result.context().session().isEmpty());
                    assertEquals(EnqueueResult.ACCEPTED, next(enqueues));
                    assertEquals(requests.get(i), read(client));
                }
                assertNull(server.failure());
            }
        }
    }

    @Test void clientsHaveIndependentContextsAndDisconnectDoesNotChangeRouting() throws Exception {
        var deliveries = new LinkedBlockingQueue<Delivery>();
        var handlers = MessageDispatcher.builder().register(MessageType.PRESENCE_QUERY, record("presence", deliveries));
        try (NioServer server = start(handlers, (context, chunk) -> {});
             Socket first = connect(server); Socket second = connect(server)) {
            MessageEnvelope a = message(MessageType.PRESENCE_QUERY), b = message(MessageType.PRESENCE_QUERY);
            write(first, a);
            Delivery firstResult = next(deliveries);
            write(second, b);
            Delivery secondResult = next(deliveries);
            assertEquals(a, firstResult.frame());
            assertEquals(b, secondResult.frame());
            assertNotEquals(firstResult.context().connectionId(), secondResult.context().connectionId());
            assertSame(firstResult.thread(), secondResult.thread());
            first.shutdownOutput();
            assertEquals(-1, first.getInputStream().read());
            MessageEnvelope subsequent = message(MessageType.PRESENCE_QUERY);
            write(second, subsequent);
            Delivery afterDisconnect = next(deliveries);
            assertEquals(subsequent, afterDisconnect.frame());
            assertSame(secondResult.context(), afterDisconnect.context());
            assertNull(server.failure());
        }
    }

    @Test void defaultServerDistinguishesUnregisteredOperationsFromUnknownWireTypes() throws Exception {
        try (NioServer server = new NioServer(0)) {
            server.start();
            try (Socket client = connect(server)) {
                MessageEnvelope request = message(MessageType.PRESENCE_QUERY);
                write(client, request);
                assertError(read(client), request.messageId(), ErrorCode.INVALID_REQUEST);
                UUID unknownId = UUID.randomUUID();
                writeJson(client, "{\"version\":1,\"type\":\"FUTURE_OPERATION\",\"messageId\":\"" + unknownId + "\",\"body\":{}}");
                assertError(read(client), unknownId, ErrorCode.UNKNOWN_TYPE);
                MessageEnvelope next = message(MessageType.PRESENCE_QUERY);
                write(client, next);
                assertError(read(client), next.messageId(), ErrorCode.INVALID_REQUEST);
                assertTrue(server.isRunning());
                assertNull(server.failure());
            }
        }
    }

    @Test void duplicateRegistrationFailsAndBuiltHandlerTableIsAnImmutableSnapshot() throws Exception {
        var deliveries = new LinkedBlockingQueue<Delivery>();
        var handlers = MessageDispatcher.builder().register(MessageType.PRESENCE_QUERY, record("original", deliveries));
        assertThrows(IllegalArgumentException.class,
                () -> handlers.register(MessageType.PRESENCE_QUERY, record("replacement", deliveries)));
        assertThrows(IllegalArgumentException.class,
                () -> handlers.register(MessageType.FILE_CHUNK, record("binary", deliveries)));
        assertThrows(NullPointerException.class, () -> handlers.register(null, record("null", deliveries)));
        assertThrows(NullPointerException.class, () -> handlers.register(MessageType.LOGOUT_REQUEST, null));
        try (NioServer server = start(handlers, (context, chunk) -> {}); Socket client = connect(server)) {
            handlers.register(MessageType.LOGOUT_REQUEST, record("added later", deliveries));
            MessageEnvelope presence = message(MessageType.PRESENCE_QUERY), logout = message(MessageType.LOGOUT_REQUEST);
            write(client, presence, logout);
            Delivery routed = next(deliveries);
            assertEquals("original", routed.route());
            assertEquals(presence, routed.frame());
            assertError(read(client), logout.messageId(), ErrorCode.INVALID_REQUEST);
            assertTrue(deliveries.isEmpty());
        }
    }

    @Test void malformedJsonAndEnvelopeNeverReachRegisteredHandlers() throws Exception {
        var deliveries = new LinkedBlockingQueue<Delivery>();
        var handlers = MessageDispatcher.builder().register(MessageType.PRESENCE_QUERY, record("presence", deliveries));
        try (NioServer server = start(handlers, (context, chunk) -> {}); Socket healthy = connect(server)) {
            for (String malformed : List.of("{", "{\"version\":1,\"type\":\"PRESENCE_QUERY\",\"body\":{}}")) {
                try (Socket bad = connect(server)) {
                    writeJson(bad, malformed);
                    assertError(read(bad), null, ErrorCode.MALFORMED_MESSAGE);
                    assertEquals(-1, bad.getInputStream().read());
                }
                MessageEnvelope valid = message(MessageType.PRESENCE_QUERY);
                write(healthy, valid);
                assertEquals(valid, next(deliveries).frame());
                assertTrue(deliveries.isEmpty());
            }
            assertNull(server.failure());
        }
    }

    @Test void registeredHandlerExceptionClosesOnlyItsOriginatingConnection() throws Exception {
        var deliveries = new LinkedBlockingQueue<Delivery>();
        var handlers = MessageDispatcher.builder()
                .register(MessageType.PRESENCE_QUERY, record("presence", deliveries))
                .register(MessageType.LOGOUT_REQUEST, (context, message) -> {
                    throw new IllegalStateException("sensitive handler details must not be logged");
                });
        try (NioServer server = start(handlers, (context, chunk) -> {});
             Socket failing = connect(server); Socket healthy = connect(server)) {
            write(failing, message(MessageType.LOGOUT_REQUEST));
            assertEquals(-1, failing.getInputStream().read());
            MessageEnvelope valid = message(MessageType.PRESENCE_QUERY);
            write(healthy, valid);
            assertEquals(valid, next(deliveries).frame());
            assertTrue(server.isRunning());
            assertNull(server.failure());
        }
    }

    @Test void binaryFramesBypassJsonDispatcherAndPreserveInterleavedOrder() throws Exception {
        var deliveries = new LinkedBlockingQueue<Delivery>();
        var handlers = MessageDispatcher.builder().register(MessageType.PRESENCE_QUERY, record("json", deliveries));
        try (NioServer server = start(handlers,
                (context, chunk) -> deliveries.add(new Delivery("binary", context, chunk, Thread.currentThread())));
             Socket client = connect(server)) {
            MessageEnvelope first = message(MessageType.PRESENCE_QUERY), last = message(MessageType.PRESENCE_QUERY);
            FileChunk binary = new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0,
                    new byte[]{'{', '}', 0, (byte) 255});
            write(client, first, binary, last);
            Delivery a = next(deliveries), b = next(deliveries), c = next(deliveries);
            assertEquals(List.of("json", "binary", "json"), List.of(a.route(), b.route(), c.route()));
            assertEquals(List.of(first, binary, last), List.of(a.frame(), b.frame(), c.frame()));
            assertSame(a.context(), b.context());
            assertSame(a.context(), c.context());
            assertSame(a.thread(), b.thread());
            assertNull(server.failure());
        }
    }
}
