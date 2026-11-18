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
}
