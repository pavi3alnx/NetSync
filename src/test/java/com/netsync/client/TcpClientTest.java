package com.netsync.client;

import com.netsync.common.*;
import org.junit.jupiter.api.*;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class TcpClientTest {
    static final FrameCodec CODEC = new FrameCodec(new MessageCodec());
    static MessageEnvelope message() {
        return new MessageEnvelope(1, MessageType.PRESENCE_QUERY, UUID.randomUUID(), null, new MessageBody.Empty());
    }
    static byte[] encode(OutboundFrame frame) throws Exception { return CODEC.encode(frame).array(); }
    static OutboundFrame receive(Socket socket) throws Exception {
        socket.setSoTimeout(3000);
        DataInputStream in = new DataInputStream(socket.getInputStream());
        int length = in.readInt();
        return CODEC.decodePayload(ByteBuffer.wrap(in.readNBytes(length)));
    }
    static void await(Callable<Boolean> condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.call() && System.nanoTime() < deadline) Thread.sleep(2);
        assertTrue(condition.call());
    }
    @Test void connectSendDisconnectReconnectAndSessionGeneration() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            assertEquals(EnqueueResult.REJECTED, client.send(message()));
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept()) {
                assertTrue(client.isConnected());
                UUID old = client.connectionId().orElseThrow();
                SessionView session = new SessionView(UUID.randomUUID(), "Alice", old);
                client.supplySession(old, session);
                assertEquals(session, client.session().orElseThrow());
                client.clearSession(); assertTrue(client.session().isEmpty());
                client.supplySession(old, session);
                for (int i = 0; i < 4; i++) {
                    var frame = message(); assertEquals(EnqueueResult.ACCEPTED, client.sendJson(frame));
                    assertEquals(frame, receive(peer));
                }
                client.disconnect(); assertEquals(-1, peer.getInputStream().read());
                assertTrue(client.session().isEmpty());
                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket next = server.accept()) {
                    assertNotEquals(old, client.connectionId().orElseThrow());
                    assertThrows(IllegalStateException.class, () -> client.supplySession(old, session));
                    client.disconnect();
                }
            }
            client.close(); client.close();
            assertEquals(TcpClient.State.CLOSED, client.state());
            assertThrows(IllegalStateException.class, client::connect);
        }
    }
    @Test void failureReleasesSocketAndAllowsRetry() throws Exception {
        int port;
        try (ServerSocket unused = new ServerSocket(0)) { port = unused.getLocalPort(); }
        try (TcpClient client = new TcpClient()) {
            int closedPort = port;
            assertThrows(IOException.class, () -> client.connect("127.0.0.1", closedPort));
            assertEquals(TcpClient.Failure.CONNECT_FAILED, client.failure());
            assertEquals(TcpClient.State.DISCONNECTED, client.state());
            try (ServerSocket server = new ServerSocket(0)) {
                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) { assertTrue(client.isConnected()); }
            }
        }
    }
    @Test void splitAndCoalescedFramesRetainMixedOrder() throws Exception {
        var received = new LinkedBlockingQueue<OutboundFrame>();
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            client.dispatcher().register(MessageType.PRESENCE_QUERY, received::add);
            client.dispatcher().registerBinary(received::add);
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept()) {
                var first = message(); byte[] split = encode(first);
                for (byte b : split) { peer.getOutputStream().write(b); peer.getOutputStream().flush(); }
                assertEquals(first, received.poll(3, TimeUnit.SECONDS));
                FileChunk chunk = new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0, new byte[]{0, -1, 2});
                var last = message();
                ByteArrayOutputStream batch = new ByteArrayOutputStream();
                batch.write(encode(first)); batch.write(encode(chunk)); batch.write(encode(last));
                peer.getOutputStream().write(batch.toByteArray());
                for (var expected : List.of(first, chunk, last)) assertEquals(expected, received.poll(3, TimeUnit.SECONDS));
            }
        }
    }
    @Test void invalidPrefixesKindsAndPayloadsCloseSafely() throws Exception {
        List<byte[]> bad = new ArrayList<>();
        for (int length : new int[]{0, 1, -1, 65_538}) bad.add(ByteBuffer.allocate(4).putInt(length).array());
        bad.add(ByteBuffer.allocate(5).putInt(50).put((byte) 9).array());
        bad.add(ByteBuffer.allocate(5).putInt(16_422).put((byte) 2).array());
        bad.add(new byte[]{0, 0, 0, 2, 1, (byte) 255});
        for (byte[] bytes : bad) {
            try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    peer.setSoTimeout(3000); peer.getOutputStream().write(bytes);
                    assertEquals(-1, peer.getInputStream().read());
                    await(() -> !client.isConnected());
                    assertEquals(TcpClient.Failure.INVALID_FRAME, client.failure());
                }
            }
        }
    }
    @Test void remoteEofDiscardsPartialFrame() throws Exception {
        for (int count : new int[]{0, 2, 5, 20}) {
            try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    peer.getOutputStream().write(encode(message()), 0, count); peer.shutdownOutput();
                    await(() -> !client.isConnected());
                    assertEquals(TcpClient.Failure.REMOTE_CLOSED, client.failure());
                }
            }
        }
    }
    @Test void concurrentSendsAreWholeFramesWithNoLoss() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept(); ExecutorService workers = Executors.newFixedThreadPool(4)) {
                Set<MessageEnvelope> expected = new HashSet<>(); List<Future<EnqueueResult>> results = new ArrayList<>();
                for (int i = 0; i < 80; i++) { var frame = message(); expected.add(frame); results.add(workers.submit(() -> client.send(frame))); }
                for (var result : results) assertEquals(EnqueueResult.ACCEPTED, result.get(3, TimeUnit.SECONDS));
                Set<OutboundFrame> actual = new HashSet<>();
                for (int i = 0; i < 80; i++) actual.add(receive(peer));
                assertEquals(expected, actual);
            }
        }
    }
    @Test void queueCountLimitIsDeterministicAndFifo() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient(2, 10000, 8)) {
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept()) {
                var first = message(); var second = message();
                // Hold the queue monitor so the writer cannot drain between enqueues.
                synchronized (client) {
                    assertEquals(EnqueueResult.ACCEPTED, client.send(first));
                    assertEquals(EnqueueResult.ACCEPTED, client.send(second));
                    assertEquals(EnqueueResult.REJECTED, client.send(message()));
                }
                assertEquals(first, receive(peer)); assertEquals(second, receive(peer));
                assertTrue(client.isConnected());
            }
        }
    }
    @Test void byteLimitAndTransferAdapterReportRejection() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient(256, 1, 8)) {
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept()) {
                assertEquals(EnqueueResult.REJECTED, client.send(message()));
                assertThrows(RejectedExecutionException.class, () -> client.sendOrThrow(message()));
            }
        }
    }
    @Test void missingBinaryReceiverIsExplicitFailure() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept()) {
                peer.getOutputStream().write(encode(new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0, new byte[]{1})));
                await(() -> !client.isConnected());
                assertEquals(TcpClient.Failure.NO_BINARY_HANDLER, client.failure());
            }
        }
    }
    @Test void slowHandlerDoesNotBlockReadsAndDispatchOverflowCloses() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient(8, 10000, 1)) {
            client.dispatcher().register(MessageType.PRESENCE_QUERY, m -> {
                entered.countDown();
                try { release.await(3, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept()) {
                peer.getOutputStream().write(encode(message()));
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                peer.getOutputStream().write(encode(message())); peer.getOutputStream().write(encode(message()));
                await(() -> !client.isConnected());
                assertEquals(TcpClient.Failure.DISPATCH_FULL, client.failure());
            } finally { release.countDown(); }
        }
    }
    @Test void handlerMayCloseClientWithoutDeadlock() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CountDownLatch done = new CountDownLatch(1);
            client.dispatcher().register(MessageType.PRESENCE_QUERY, m -> { client.close(); done.countDown(); });
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept()) {
                peer.getOutputStream().write(encode(message()));
                assertTrue(done.await(3, TimeUnit.SECONDS));
                assertEquals(TcpClient.State.CLOSED, client.state());
            }
        }
    }
    @Test void recoverableProtocolRejectionDoesNotLoseFollowingCoalescedFrame() throws Exception {
        var received = new LinkedBlockingQueue<MessageEnvelope>();
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            client.dispatcher().register(MessageType.PRESENCE_QUERY, received::add);
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept()) {
                byte[] wire = encode(message());
                String json = new String(wire, 5, wire.length - 5, java.nio.charset.StandardCharsets.UTF_8)
                    .replace("PRESENCE_QUERY", "FUTURE_TYPE");
                byte[] body = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                ByteArrayOutputStream batch = new ByteArrayOutputStream();
                batch.write(ByteBuffer.allocate(5 + body.length).putInt(body.length + 1).put((byte) 1).put(body).array());
                var valid = message(); batch.write(encode(valid));
                peer.getOutputStream().write(batch.toByteArray());
                assertEquals(valid, received.poll(3, TimeUnit.SECONDS));
                assertEquals(1, client.protocolRejections()); assertTrue(client.isConnected());
            }
        }
    }
    @Test void maximumBinaryFramesDrainExactlyInOrder() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept()) {
                List<FileChunk> frames = new ArrayList<>();
                byte[] data = new byte[ProtocolLimits.MAX_CHUNK_BYTES];
                for (int i = 0; i < data.length; i++) data[i] = (byte) i;
                UUID transfer = UUID.randomUUID();
                for (int i = 0; i < 40; i++) {
                    var chunk = new FileChunk(UUID.randomUUID(), transfer, i, data); frames.add(chunk);
                    assertEquals(EnqueueResult.ACCEPTED, client.sendChunk(chunk));
                }
                for (var frame : frames) assertEquals(frame, receive(peer));
            }
        }
    }
    @Test void slowBinaryReceiverRunsOffReaderAndOverflowIsObservable() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient(8, 10000, 1)) {
            client.dispatcher().registerBinary(chunk -> {
                assertEquals("netsync-client-dispatch", Thread.currentThread().getName()); entered.countDown();
                try { release.await(3, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept()) {
                byte[] chunk = encode(new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0, new byte[]{1}));
                peer.getOutputStream().write(chunk); assertTrue(entered.await(3, TimeUnit.SECONDS));
                peer.getOutputStream().write(chunk); peer.getOutputStream().write(chunk);
                await(() -> !client.isConnected());
                assertEquals(TcpClient.Failure.DISPATCH_FULL, client.failure());
            } finally { release.countDown(); }
        }
    }
    @Test void existingNioServerInteroperatesWithoutContractChanges() throws Exception {
        var owner = new java.util.concurrent.atomic.AtomicReference<com.netsync.server.NioServer>();
        var received = new LinkedBlockingQueue<OutboundFrame>();
        try (var server = new com.netsync.server.NioServer(0,
                (context, envelope) -> owner.get().send(context.connectionId(), envelope),
                (context, chunk) -> owner.get().send(context.connectionId(), chunk)); TcpClient client = new TcpClient()) {
            owner.set(server); server.start();
            client.dispatcher().register(MessageType.PRESENCE_QUERY, received::add);
            client.dispatcher().registerBinary(received::add);
            client.connect("127.0.0.1", server.port());
            var control = message(); var chunk = new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0, new byte[]{3});
            client.sendOrThrow(control); client.sendOrThrow(chunk);
            assertEquals(control, received.poll(3, TimeUnit.SECONDS)); assertEquals(chunk, received.poll(3, TimeUnit.SECONDS));
        }
    }
    @Test void invalidConfigurationAndOutboundModelsFailPredictably() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new TcpClient(0, 1, 1));
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            assertThrows(IllegalArgumentException.class, () -> client.connect("", 5050));
            assertThrows(IllegalArgumentException.class, () -> client.connect("127.0.0.1", 0));
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept()) {
                var invalid = new MessageEnvelope(1, MessageType.CHAT_SEND, UUID.randomUUID(), null, new MessageBody.Empty());
                assertThrows(com.netsync.common.ProtocolException.class, () -> client.send(invalid));
                assertThrows(IllegalArgumentException.class, () -> client.sendOrThrow(invalid));
                assertThrows(IllegalStateException.class, client::connect);
                assertTrue(client.isConnected());
            }
        }
    }

    @Test void completeFramesBeforeEofStillDispatchInOrder() throws Exception {
        var received = new LinkedBlockingQueue<OutboundFrame>();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            client.dispatcher().register(MessageType.PRESENCE_QUERY, m -> {
                entered.countDown();
                try { release.await(3, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                received.add(m);
            });
            client.dispatcher().registerBinary(received::add);
            client.connect("127.0.0.1", server.getLocalPort());
            try (Socket peer = server.accept()) {
                var control = message(); var binary = new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0, new byte[]{1});
                peer.getOutputStream().write(encode(control)); assertTrue(entered.await(3, TimeUnit.SECONDS));
                peer.getOutputStream().write(encode(binary)); peer.shutdownOutput();
                await(() -> !client.isConnected()); release.countDown();
                assertEquals(control, received.poll(3, TimeUnit.SECONDS));
                assertEquals(binary, received.poll(3, TimeUnit.SECONDS));
            } finally { release.countDown(); }
        }
    }

}
