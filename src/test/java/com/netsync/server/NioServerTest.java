package com.netsync.server;

import com.netsync.common.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class NioServerTest {
    private final FrameCodec codec = new FrameCodec(new MessageCodec());
    private record Received(UUID connectionId, OutboundFrame frame) {}

    private NioServer server(LinkedBlockingQueue<Received> received) throws IOException {
        NioServer server = new NioServer(0,
                (context, message) -> received.add(new Received(context.connectionId(), message)),
                (context, chunk) -> received.add(new Received(context.connectionId(), chunk)));
        server.start();
        return server;
    }

    private Socket connect(NioServer server) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
        socket.setSoTimeout(3000);
        socket.setTcpNoDelay(true);
        return socket;
    }

    private MessageEnvelope message() {
        return new MessageEnvelope(1, MessageType.PRESENCE_QUERY, UUID.randomUUID(), null, new MessageBody.Empty());
    }

    private byte[] encode(OutboundFrame frame) throws ProtocolException {
        ByteBuffer buffer = codec.encode(frame);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private Received next(LinkedBlockingQueue<Received> queue) throws InterruptedException {
        Received received = queue.poll(3, TimeUnit.SECONDS);
        assertNotNull(received, "Selector did not dispatch a frame");
        return received;
    }

    private OutboundFrame readFrame(Socket socket) throws Exception {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int length = input.readInt();
        byte[] payload = input.readNBytes(length);
        assertEquals(length, payload.length);
        return codec.decodePayload(ByteBuffer.wrap(payload));
    }

    @Test void startsAndAcceptsConnection() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket client = connect(server)) {
            assertTrue(server.isRunning());
            assertTrue(server.port() > 0);
            MessageEnvelope request = message();
            client.getOutputStream().write(encode(request));
            assertEquals(request, next(queue).frame());
            assertNull(server.failure());
        }
    }

    @Test void twoClientsHaveIndependentPartialFrameState() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket first = connect(server); Socket second = connect(server)) {
            MessageEnvelope a = message(), b = message();
            byte[] bytes = encode(a);
            first.getOutputStream().write(bytes, 0, 2);
            second.getOutputStream().write(encode(b));
            Received secondResult = next(queue);
            assertEquals(b, secondResult.frame());
            first.getOutputStream().write(bytes, 2, bytes.length - 2);
            Received firstResult = next(queue);
            assertEquals(a, firstResult.frame());
            assertNotEquals(firstResult.connectionId(), secondResult.connectionId());
        }
    }

    @Test void fragmentedPrefixKindAndPayloadAreRetained() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket client = connect(server)) {
            MessageEnvelope request = message();
            byte[] bytes = encode(request);
            int offset = 0;
            for (int end : new int[]{1, 3, 4, 5, 12, bytes.length - 1}) {
                client.getOutputStream().write(bytes, offset, end - offset);
                assertNull(queue.poll(40, TimeUnit.MILLISECONDS), "Incomplete frame was dispatched");
                offset = end;
            }
            client.getOutputStream().write(bytes, offset, bytes.length - offset);
            assertEquals(request, next(queue).frame());
        }
    }

    @Test void coalescedJsonAndBinaryFramesPreserveOrder() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket client = connect(server)) {
            MessageEnvelope first = message(), last = message();
            FileChunk chunk = new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0, new byte[]{1, 2, 3});
            var output = new java.io.ByteArrayOutputStream();
            output.write(encode(first)); output.write(encode(chunk)); output.write(encode(last));
            client.getOutputStream().write(output.toByteArray());
            assertEquals(first, next(queue).frame());
            FileChunk decoded = assertInstanceOf(FileChunk.class, next(queue).frame());
            assertEquals(chunk.messageId(), decoded.messageId());
            assertEquals(chunk.transferId(), decoded.transferId());
            assertArrayEquals(chunk.data(), decoded.data());
            assertEquals(last, next(queue).frame());
        }
    }

    @Test void invalidLengthsAndKindsOnlyCloseOffendingClient() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket healthy = connect(server)) {
            for (int length : new int[]{0, -1, 1, ProtocolLimits.MAX_FRAME_LENGTH + 1}) {
                try (Socket bad = connect(server)) {
                    bad.getOutputStream().write(ByteBuffer.allocate(4).putInt(length).array());
                    assertEquals(-1, bad.getInputStream().read());
                }
            }
            for (byte kind : new byte[]{99, ProtocolLimits.CHUNK_KIND}) {
                try (Socket bad = connect(server)) {
                    // No remaining payload: kind-specific validation must happen immediately.
                    bad.getOutputStream().write(ByteBuffer.allocate(5).putInt(2).put(kind).array());
                    assertEquals(-1, bad.getInputStream().read());
                }
            }
            MessageEnvelope request = message();
            healthy.getOutputStream().write(encode(request));
            assertEquals(request, next(queue).frame());
            assertTrue(server.isRunning());
            assertNull(server.failure());
        }
    }

    @Test void malformedJsonGetsErrorThenClosure() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket bad = connect(server); Socket healthy = connect(server)) {
            bad.getOutputStream().write(new byte[]{0, 0, 0, 2, 1, '{'});
            MessageEnvelope error = assertInstanceOf(MessageEnvelope.class, readFrame(bad));
            assertEquals(ErrorCode.MALFORMED_MESSAGE, ((MessageBody.Error) error.body()).code());
            assertEquals(-1, bad.getInputStream().read());
            healthy.getOutputStream().write(encode(message()));
            next(queue);
        }
    }

    @Test void recoverableCodecErrorAllowsNextFrame() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket client = connect(server)) {
            byte[] encoded = encode(message());
            String json = new String(encoded, 5, encoded.length - 5,
                    java.nio.charset.StandardCharsets.UTF_8).replace("PRESENCE_QUERY", "UNKNOWN_REQUEST");
            byte[] body = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            client.getOutputStream().write(ByteBuffer.allocate(5 + body.length).putInt(body.length + 1).put((byte) 1).put(body).array());
            MessageEnvelope error = assertInstanceOf(MessageEnvelope.class, readFrame(client));
            assertEquals(ErrorCode.UNKNOWN_TYPE, ((MessageBody.Error) error.body()).code());
            client.getOutputStream().write(encode(message()));
            next(queue);
        }
    }

    @Test void disconnectDuringPayloadDoesNotAffectOtherClient() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket healthy = connect(server)) {
            try (Socket departing = connect(server)) {
                departing.getOutputStream().write(new byte[]{0, 0, 0, 20, 1, '{'});
            }
            MessageEnvelope request = message();
            healthy.getOutputStream().write(encode(request));
            assertEquals(request, next(queue).frame());
            assertNull(queue.poll(100, TimeUnit.MILLISECONDS));
        }
    }

    @Test void outboundSinkQueuesFramesInOrder() throws Exception {
        AtomicReference<NioServer> reference = new AtomicReference<>();
        try (NioServer server = new NioServer(0, (context, message) -> {
            assertEquals(EnqueueResult.ACCEPTED, reference.get().send(context.connectionId(), message));
        }, (context, chunk) -> {
            assertEquals(EnqueueResult.ACCEPTED, reference.get().send(context.connectionId(), chunk));
        })) {
            reference.set(server);
            server.start();
            try (Socket client = connect(server)) {
                for (int i = 0; i < 10; i++) {
                    MessageEnvelope request = message();
                    client.getOutputStream().write(encode(request));
                    assertEquals(request, readFrame(client));
                }
            }
        }
    }

    @Test void shutdownClosesClientsAndReleasesPort() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        NioServer server = server(queue);
        try (server; Socket client = connect(server)) {
            client.getOutputStream().write(encode(message()));
            next(queue);
            int port = server.port();
            server.close();
            server.close();
            assertFalse(server.isRunning());
            assertNull(server.failure());
            assertEquals(-1, client.getInputStream().read());
            try (ServerSocket rebound = new ServerSocket()) {
                rebound.bind(new InetSocketAddress("127.0.0.1", port));
            }
        }
    }

    @Test void outputQueueOverflowDisconnectsOnlyThatClient() throws Exception {
        AtomicReference<NioServer> reference = new AtomicReference<>();
        var results = new LinkedBlockingQueue<EnqueueResult>();
        try (NioServer server = new NioServer(0, (context, message) -> {
            EnqueueResult result = EnqueueResult.ACCEPTED;
            if (message.type() == MessageType.LOGOUT_REQUEST) {
                for (int i = 0; i <= ProtocolLimits.MAX_OUTPUT_FRAMES; i++) {
                    result = reference.get().send(context.connectionId(), message);
                }
            } else {
                result = reference.get().send(context.connectionId(), message);
            }
            results.add(result);
        }, (context, chunk) -> {})) {
            reference.set(server);
            server.start();
            try (Socket slow = connect(server); Socket healthy = connect(server)) {
                slow.getOutputStream().write(encode(new MessageEnvelope(1, MessageType.LOGOUT_REQUEST,
                        UUID.randomUUID(), null, new MessageBody.Empty())));
                assertEquals(EnqueueResult.REJECTED, results.poll(3, TimeUnit.SECONDS));
                assertEquals(-1, slow.getInputStream().read());
                MessageEnvelope request = message();
                healthy.getOutputStream().write(encode(request));
                assertEquals(EnqueueResult.ACCEPTED, results.poll(3, TimeUnit.SECONDS));
                assertEquals(request, readFrame(healthy));
            }
        }
    }

    @Test void coalescedFramesBeyondReadBudgetAreEventuallyDispatched() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket client = connect(server)) {
            MessageEnvelope request = message();
            byte[] encoded = encode(request);
            var bytes = new java.io.ByteArrayOutputStream();
            for (int i = 0; i < 100; i++) bytes.write(encoded);
            client.getOutputStream().write(bytes.toByteArray());
            for (int i = 0; i < 100; i++) assertEquals(request, next(queue).frame());
        }
    }

    @Test void bindFailureReleasesResourcesAndCloseBeforeStartIsSafe() throws Exception {
        try (ServerSocket occupied = new ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"));
             NioServer server = new NioServer(occupied.getLocalPort());
             NioServer unused = new NioServer(0)) {
            unused.close();
            assertThrows(IOException.class, server::start);
            assertFalse(server.isRunning());
        }
    }

    private void assertReleased(ConnectionState state) {
        assertTrue(state.closed);
        assertFalse(state.key.isValid());
        assertFalse(state.key.channel().isOpen());
        assertNull(state.key.attachment());
        assertNull(state.payload);
        assertTrue(state.output.isEmpty());
        assertEquals(0, state.queuedBytes);
        assertEquals(0, state.prefix.position());
    }

    @Test void explicitCloseReleasesStateAndRejectsFurtherSends() throws Exception {
        var reference = new AtomicReference<NioServer>();
        var states = new LinkedBlockingQueue<ConnectionState>();
        var results = new LinkedBlockingQueue<Boolean>();
        try (NioServer server = new NioServer(0, (context, request) -> {
            NioServer owner = reference.get();
            boolean queued = owner.send(context.connectionId(), request) == EnqueueResult.ACCEPTED;
            boolean closed = owner.closeConnection(context.connectionId());
            results.add(queued && closed && !owner.closeConnection(context.connectionId())
                    && owner.send(context.connectionId(), request) == EnqueueResult.REJECTED
                    && context.session().isEmpty());
            states.add((ConnectionState) context);
        }, (context, chunk) -> {})) {
            reference.set(server);
            server.start();
            try (Socket client = connect(server)) {
                client.getOutputStream().write(encode(message()));
                assertEquals(Boolean.TRUE, results.poll(3, TimeUnit.SECONDS));
                ConnectionState state = states.poll(3, TimeUnit.SECONDS);
                assertNotNull(state);
                assertReleased(state);
                assertEquals(-1, client.getInputStream().read());
            }
        }
    }

    @Test void abruptResetIsCleanedUpAndHealthyClientContinues() throws Exception {
        var reference = new AtomicReference<NioServer>();
        var departed = new AtomicReference<ConnectionState>();
        var registered = new LinkedBlockingQueue<Boolean>();
        var results = new LinkedBlockingQueue<EnqueueResult>();
        try (NioServer server = new NioServer(0, (context, request) -> {
            if (departed.compareAndSet(null, (ConnectionState) context)) registered.add(true);
            else results.add(reference.get().send(departed.get().connectionId(), request));
        }, (context, chunk) -> {})) {
            reference.set(server);
            server.start();
            try (Socket reset = connect(server); Socket healthy = connect(server)) {
                reset.getOutputStream().write(encode(message()));
                assertEquals(Boolean.TRUE, registered.poll(3, TimeUnit.SECONDS));
                reset.setSoLinger(true, 0); // Close with TCP RST rather than orderly EOF.
                reset.close();
                // Each request is a selector-thread barrier; reset and healthy readiness can arrive in either order.
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                EnqueueResult result;
                do {
                    healthy.getOutputStream().write(encode(message()));
                    result = results.poll(3, TimeUnit.SECONDS);
                    assertNotNull(result);
                } while (result != EnqueueResult.REJECTED && System.nanoTime() < deadline);
                assertEquals(EnqueueResult.REJECTED, result);
                assertReleased(departed.get());
                assertTrue(server.isRunning());
                assertNull(server.failure());
            }
        }
    }

    @Test void invalidChannelIsCleanedUpWithoutStoppingOtherClients() throws Exception {
        var states = new LinkedBlockingQueue<ConnectionState>();
        var healthy = new LinkedBlockingQueue<Boolean>();
        try (NioServer server = new NioServer(0, (context, request) -> {
            if (request.type() == MessageType.LOGOUT_REQUEST) {
                ConnectionState state = (ConnectionState) context;
                try { state.key.channel().close(); }
                catch (IOException e) { throw new java.io.UncheckedIOException(e); }
                states.add(state);
            } else healthy.add(true);
        }, (context, chunk) -> {})) {
            server.start();
            try (Socket invalid = connect(server); Socket other = connect(server)) {
                invalid.getOutputStream().write(encode(new MessageEnvelope(1, MessageType.LOGOUT_REQUEST,
                        UUID.randomUUID(), null, new MessageBody.Empty())));
                ConnectionState state = states.poll(3, TimeUnit.SECONDS);
                assertNotNull(state);
                other.getOutputStream().write(encode(message()));
                assertEquals(Boolean.TRUE, healthy.poll(3, TimeUnit.SECONDS));
                assertReleased(state); // The healthy callback runs after the invalid callback's cleanup.
                assertNull(server.failure());
            }
        }
    }

    @Test void handlerFailureOnlyClosesThatConnection() throws Exception {
        var received = new LinkedBlockingQueue<Boolean>();
        try (NioServer server = new NioServer(0, (context, request) -> {
            if (request.type() == MessageType.LOGOUT_REQUEST) throw new IllegalStateException("private handler data");
            received.add(true);
        }, (context, chunk) -> {})) {
            server.start();
            try (Socket failing = connect(server); Socket healthy = connect(server)) {
                failing.getOutputStream().write(encode(new MessageEnvelope(1, MessageType.LOGOUT_REQUEST,
                        UUID.randomUUID(), null, new MessageBody.Empty())));
                assertEquals(-1, failing.getInputStream().read());
                healthy.getOutputStream().write(encode(message()));
                assertEquals(Boolean.TRUE, received.poll(3, TimeUnit.SECONDS));
                assertTrue(server.isRunning());
                assertNull(server.failure());
            }
        }
    }

    @Test void queuedOutputAcrossWriteBudgetsDrainsAndDisablesWriteInterest() throws Exception {
        var reference = new AtomicReference<NioServer>();
        var checks = new LinkedBlockingQueue<Boolean>();
        FileChunk chunk = new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0,
                new byte[ProtocolLimits.MAX_CHUNK_BYTES]);
        try (NioServer server = new NioServer(0, (context, request) -> {
            ConnectionState state = (ConnectionState) context;
            if (request.type() == MessageType.LOGOUT_REQUEST) {
                checks.add(state.output.isEmpty() && state.queuedBytes == 0
                        && state.key.interestOps() == java.nio.channels.SelectionKey.OP_READ);
                return;
            }
            boolean accepted = true;
            for (int i = 0; i < 40; i++) {
                accepted &= reference.get().send(context.connectionId(), chunk) == EnqueueResult.ACCEPTED;
            }
            checks.add(accepted);
        }, (context, frame) -> {})) {
            reference.set(server);
            server.start();
            try (Socket client = connect(server)) {
                client.getOutputStream().write(encode(message()));
                assertEquals(Boolean.TRUE, checks.poll(3, TimeUnit.SECONDS));
                for (int i = 0; i < 40; i++) {
                    FileChunk decoded = assertInstanceOf(FileChunk.class, readFrame(client));
                    assertEquals(chunk.messageId(), decoded.messageId());
                    assertArrayEquals(chunk.data(), decoded.data());
                }
                client.getOutputStream().write(encode(new MessageEnvelope(1, MessageType.LOGOUT_REQUEST,
                        UUID.randomUUID(), null, new MessageBody.Empty())));
                assertEquals(Boolean.TRUE, checks.poll(3, TimeUnit.SECONDS));
            }
        }
    }

    @Test void connectionMutationsRejectCallsOutsideSelectorThread() throws Exception {
        try (NioServer server = new NioServer(0)) {
            server.start();
            assertThrows(IllegalStateException.class, () -> server.send(UUID.randomUUID(), message()));
            assertThrows(IllegalStateException.class, () -> server.closeConnection(UUID.randomUUID()));
            assertTrue(server.isRunning());
        }
    }

    @Test void completeFramesThenPartialFrameRetainOrderAndAllowOtherClients() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket busy = connect(server); Socket other = connect(server)) {
            for (int split : new int[]{2, 5, 20}) {
                MessageEnvelope first = message(), second = message(), partial = message(), independent = message();
                byte[] tail = encode(partial);
                var combined = new java.io.ByteArrayOutputStream();
                combined.write(encode(first));
                combined.write(encode(second));
                combined.write(tail, 0, split);
                busy.getOutputStream().write(combined.toByteArray());
                assertEquals(first, next(queue).frame());
                assertEquals(second, next(queue).frame());
                other.getOutputStream().write(encode(independent));
                assertEquals(independent, next(queue).frame());
                // Many tiny writes must neither lose bytes nor dispatch the unfinished frame.
                for (int offset = split; offset < tail.length; offset++) busy.getOutputStream().write(tail[offset]);
                assertEquals(partial, next(queue).frame());
            }
            assertNull(server.failure());
        }
    }

    @Test void maximumJsonAndBinaryFramesRoundTripInQueuedOrder() throws Exception {
        MessageEnvelope empty = new MessageEnvelope(1, MessageType.ERROR, UUID.randomUUID(), null,
                new MessageBody.Error(ErrorCode.BUSY, "", null));
        int padding = ProtocolLimits.MAX_JSON_BYTES - (encode(empty).length - 5);
        MessageEnvelope maximum = new MessageEnvelope(1, MessageType.ERROR, empty.messageId(), null,
                new MessageBody.Error(ErrorCode.BUSY, "x".repeat(padding), null));
        byte[] data = new byte[ProtocolLimits.MAX_CHUNK_BYTES];
        for (int i = 0; i < data.length; i++) data[i] = (byte) i; // Includes NUL and invalid UTF-8.
        byte[] jsonLooking = "{\"binary\":true}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        System.arraycopy(jsonLooking, 0, data, 0, jsonLooking.length);
        FileChunk binary = new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0, data);
        FileChunk last = new FileChunk(UUID.randomUUID(), binary.transferId(), 1, new byte[]{(byte) 255});
        assertEquals(65_537, ByteBuffer.wrap(encode(maximum)).getInt());
        assertEquals(16_421, ByteBuffer.wrap(encode(binary)).getInt());
        var frames = java.util.List.<OutboundFrame>of(message(), maximum, binary, message(), last);
        var owner = new AtomicReference<NioServer>();
        var results = new LinkedBlockingQueue<EnqueueResult>();
        try (NioServer server = new NioServer(0,
                (context, frame) -> results.add(owner.get().send(context.connectionId(), frame)),
                (context, frame) -> results.add(owner.get().send(context.connectionId(), frame)))) {
            owner.set(server);
            server.start();
            try (Socket client = connect(server)) {
                var combined = new java.io.ByteArrayOutputStream();
                for (OutboundFrame frame : frames) combined.write(encode(frame));
                client.getOutputStream().write(combined.toByteArray());
                for (OutboundFrame expected : frames) {
                    assertEquals(EnqueueResult.ACCEPTED, results.poll(3, TimeUnit.SECONDS));
                    assertEquals(expected, readFrame(client));
                }
                assertNull(server.failure());
            }
        }
    }

    @Test void oversizedBinaryFramesCloseAsSoonAsKindArrives() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket healthy = connect(server)) {
            int maximumBinaryLength = 1 + ProtocolLimits.CHUNK_HEADER_BYTES + ProtocolLimits.MAX_CHUNK_BYTES;
            for (int length : new int[]{maximumBinaryLength + 1, ProtocolLimits.MAX_FRAME_LENGTH}) {
                try (Socket bad = connect(server)) {
                    // These prefixes pass the global limit. No binary header/data is sent.
                    bad.getOutputStream().write(ByteBuffer.allocate(5).putInt(length)
                            .put(ProtocolLimits.CHUNK_KIND).array());
                    assertEquals(-1, bad.getInputStream().read());
                }
            }
            MessageEnvelope request = message();
            healthy.getOutputStream().write(encode(request));
            assertEquals(request, next(queue).frame());
            assertNull(server.failure());
        }
    }

    @Test void negativeBinarySequenceIsRejectedWithoutDispatch() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket bad = connect(server); Socket healthy = connect(server)) {
            byte[] wire = encode(new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0, new byte[]{42}));
            ByteBuffer.wrap(wire).putInt(4 + 1 + 16 + 16, -1);
            bad.getOutputStream().write(wire);
            assertEquals(-1, bad.getInputStream().read());
            MessageEnvelope request = message();
            healthy.getOutputStream().write(encode(request));
            assertEquals(request, next(queue).frame());
            assertTrue(queue.isEmpty());
            assertNull(server.failure());
        }
    }

    @Test void eofDuringPrefixKindOrBodyDiscardsPartialState() throws Exception {
        var states = new LinkedBlockingQueue<ConnectionState>();
        try (NioServer server = new NioServer(0, (context, frame) -> states.add((ConnectionState) context),
                (context, chunk) -> states.add((ConnectionState) context))) {
            server.start();
            try (Socket healthy = connect(server)) {
                for (int split : new int[]{1, 3, 4, 5, 20}) {
                    try (Socket truncated = connect(server)) {
                        truncated.getOutputStream().write(encode(message()));
                        ConnectionState state = states.poll(3, TimeUnit.SECONDS);
                        assertNotNull(state);
                        byte[] incomplete = encode(message());
                        truncated.getOutputStream().write(incomplete, 0, split);
                        truncated.shutdownOutput();
                        assertEquals(-1, truncated.getInputStream().read());
                        // A subsequent healthy callback synchronizes with the completed cleanup.
                        healthy.getOutputStream().write(encode(message()));
                        ConnectionState other = states.poll(3, TimeUnit.SECONDS);
                        assertNotNull(other);
                        assertNotSame(state, other, "Truncated frame must not reach a handler");
                        assertReleased(state);
                        assertTrue(states.isEmpty());
                    }
                }
                assertNull(server.failure());
            }
        }
    }

    @Test void recoverableBodyErrorDoesNotCorruptCoalescedFollowingFrame() throws Exception {
        var queue = new LinkedBlockingQueue<Received>();
        try (NioServer server = server(queue); Socket client = connect(server)) {
            MessageEnvelope invalid = message(), valid = message();
            byte[] encoded = encode(invalid);
            String json = new String(encoded, 5, encoded.length - 5, java.nio.charset.StandardCharsets.UTF_8)
                    .replace("\"body\":{}", "\"body\":{\"unexpected\":true}");
            byte[] body = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            var combined = new java.io.ByteArrayOutputStream();
            combined.write(ByteBuffer.allocate(5 + body.length).putInt(1 + body.length)
                    .put(ProtocolLimits.JSON_KIND).put(body).array());
            combined.write(encode(valid));
            client.getOutputStream().write(combined.toByteArray());
            assertEquals(valid, next(queue).frame());
            MessageEnvelope error = assertInstanceOf(MessageEnvelope.class, readFrame(client));
            assertEquals(invalid.messageId(), error.replyTo());
            assertEquals(ErrorCode.INVALID_REQUEST, ((MessageBody.Error) error.body()).code());
            assertTrue(queue.isEmpty());
            assertNull(server.failure());
        }
    }

    @Test void outputByteLimitRejectsBeforeFrameCountLimitAndClearsQueue() throws Exception {
        FileChunk chunk = new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0,
                new byte[ProtocolLimits.MAX_CHUNK_BYTES]);
        int encodedSize = encode(chunk).length;
        int acceptedCount = ProtocolLimits.MAX_OUTPUT_BYTES / encodedSize;
        assertTrue(acceptedCount + 1 < ProtocolLimits.MAX_OUTPUT_FRAMES);
        var owner = new AtomicReference<NioServer>();
        var checks = new LinkedBlockingQueue<Boolean>();
        var states = new LinkedBlockingQueue<ConnectionState>();
        try (NioServer server = new NioServer(0, (context, request) -> {
            if (request.type() != MessageType.LOGOUT_REQUEST) {
                checks.add(owner.get().send(context.connectionId(), request) == EnqueueResult.ACCEPTED);
                return;
            }
            ConnectionState state = (ConnectionState) context;
            boolean accepted = true;
            for (int i = 0; i < acceptedCount; i++) {
                accepted &= owner.get().send(context.connectionId(), chunk) == EnqueueResult.ACCEPTED;
            }
            boolean accounted = state.queuedBytes == acceptedCount * encodedSize
                    && state.output.size() == acceptedCount;
            boolean rejected = owner.get().send(context.connectionId(), chunk) == EnqueueResult.REJECTED;
            checks.add(accepted && accounted && rejected);
            states.add(state);
        }, (context, frame) -> {})) {
            owner.set(server);
            server.start();
            try (Socket slow = connect(server); Socket healthy = connect(server)) {
                slow.getOutputStream().write(encode(new MessageEnvelope(1, MessageType.LOGOUT_REQUEST,
                        UUID.randomUUID(), null, new MessageBody.Empty())));
                assertEquals(Boolean.TRUE, checks.poll(3, TimeUnit.SECONDS));
                ConnectionState state = states.poll(3, TimeUnit.SECONDS);
                assertNotNull(state);
                assertReleased(state);
                assertEquals(-1, slow.getInputStream().read());
                MessageEnvelope request = message();
                healthy.getOutputStream().write(encode(request));
                assertEquals(Boolean.TRUE, checks.poll(3, TimeUnit.SECONDS));
                assertEquals(request, readFrame(healthy));
                assertNull(server.failure());
            }
        }
    }
}
