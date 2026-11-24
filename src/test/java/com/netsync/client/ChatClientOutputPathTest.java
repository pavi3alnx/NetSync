package com.netsync.client;

import com.netsync.common.*;
import com.netsync.testsupport.ControlledTcpPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class ChatClientOutputPathTest {
    private static final UUID SENDER = UUID.fromString("33333333-3333-3333-3333-333333333333");
    private static final UUID RECIPIENT = UUID.fromString("22222222-2222-2222-2222-222222222222");

    @Test
    void blockedOutputDoesNotBlockChatAckPresenceOrDistinctDeliveries() throws Exception {
        try (ControlledTcpPeer peer = new ControlledTcpPeer()) {
            BlockingWriter writer = new BlockingWriter();
            NetSyncClient cli = newClient(writer);
            try {
                ControlledTcpPeer.Connection connection = connectLogin(cli, peer);
                assertTrue(writer.entered.await(2, TimeUnit.SECONDS));
                CountDownLatch deliveries = new CountDownLatch(2);
                CountDownLatch ackProcessed = new CountDownLatch(1);
                CountDownLatch presenceProcessed = new CountDownLatch(1);
                cli.transport().dispatcher().register(MessageType.CHAT_DELIVER, ignored -> deliveries.countDown());
                cli.transport().dispatcher().register(MessageType.ACK, ignored -> ackProcessed.countDown());
                cli.transport().dispatcher().register(MessageType.PRESENCE_UPDATE, ignored -> presenceProcessed.countDown());

                cli.execute("/chat " + RECIPIENT + " pending ack");
                MessageEnvelope request = connection.nextRequest(MessageType.CHAT_SEND);
                connection.deliver(SENDER, "distinct delivery one");
                connection.deliver(SENDER, "distinct delivery two");
                connection.acknowledge(request, MessageType.CHAT_SEND, AckStatus.QUEUED);
                connection.send(new MessageEnvelope(1, MessageType.PRESENCE_UPDATE, UUID.randomUUID(), null,
                        new MessageBody.PresenceUpdate(SENDER, "sender", PresenceState.ONLINE)));

                assertTrue(deliveries.await(2, TimeUnit.SECONDS), "chat callbacks must progress while the output sink is blocked");
                assertTrue(ackProcessed.await(2, TimeUnit.SECONDS), "ACK callback must progress while the output sink is blocked");
                assertTrue(presenceProcessed.await(2, TimeUnit.SECONDS), "presence callback must progress while the output sink is blocked");
                assertTrue(cli.outputQueueForTesting().pendingCount() <= ClientOutputQueue.DEFAULT_CAPACITY);

                writer.release.countDown();
                await(() -> writer.lines.stream().anyMatch(line -> line.contains("distinct delivery one"))
                        && writer.lines.stream().anyMatch(line -> line.contains("distinct delivery two"))
                        && writer.lines.stream().anyMatch(line -> line.contains("Queued for recipient " + RECIPIENT)));
                assertEquals(1, writer.lines.stream().filter(line -> line.contains("distinct delivery one")).count());
                assertEquals(1, writer.lines.stream().filter(line -> line.contains("distinct delivery two")).count());
            } finally {
                writer.release.countDown(); cli.close();
            }
        }
    }

    @Test
    void chatSaturationStaysBoundedAndReportsOmissions() throws Exception {
        try (ControlledTcpPeer peer = new ControlledTcpPeer()) {
            BlockingWriter writer = new BlockingWriter();
            NetSyncClient cli = newClient(writer);
            try {
                ControlledTcpPeer.Connection connection = connectLogin(cli, peer);
                assertTrue(writer.entered.await(2, TimeUnit.SECONDS));
                int count = 96;
                CountDownLatch callbacks = new CountDownLatch(count);
                cli.transport().dispatcher().register(MessageType.CHAT_DELIVER, ignored -> callbacks.countDown());
                for (int i = 0; i < count; i++) connection.deliver(SENDER, "saturated message " + i);
                assertTrue(callbacks.await(4, TimeUnit.SECONDS));
                assertTrue(cli.outputQueueForTesting().pendingCount() <= ClientOutputQueue.DEFAULT_CAPACITY);
                assertTrue(cli.outputQueueForTesting().omittedCount() > 0);
                writer.release.countDown();
                await(() -> writer.lines.stream().anyMatch(line -> line.startsWith("Client output was omitted")));
            } finally {
                writer.release.countDown(); cli.close();
            }
        }
    }

    @Test
    void throwingOutputConsumerDoesNotTerminateChatDispatch() throws Exception {
        try (ControlledTcpPeer peer = new ControlledTcpPeer()) {
            ThrowChatWriter writer = new ThrowChatWriter();
            NetSyncClient cli = newClient(writer);
            try {
                ControlledTcpPeer.Connection connection = connectLogin(cli, peer);
                CountDownLatch dispatchContinued = new CountDownLatch(2);
                cli.transport().dispatcher().register(MessageType.CHAT_DELIVER, ignored -> dispatchContinued.countDown());
                connection.deliver(SENDER, "controlled chat output exception");
                connection.deliver(SENDER, "chat after output exception");
                assertTrue(dispatchContinued.await(2, TimeUnit.SECONDS));
                await(() -> writer.lines.stream().anyMatch(line -> line.contains("chat after output exception")));
                assertTrue(writer.failedOnce.get());
            } finally { cli.close(); }
        }
    }

    @Test
    void queuedChatOutputFromOldGenerationIsDiscardedAfterReconnect() throws Exception {
        try (ControlledTcpPeer peer = new ControlledTcpPeer()) {
            BlockingWriter writer = new BlockingWriter();
            NetSyncClient cli = newClient(writer);
            try {
                ControlledTcpPeer.Connection first = connectLogin(cli, peer);
                UUID firstGeneration = cli.transport().connectionId().orElseThrow();
                assertTrue(writer.entered.await(2, TimeUnit.SECONDS));
                CountDownLatch oldCallback = new CountDownLatch(1);
                cli.transport().dispatcher().register(MessageType.CHAT_DELIVER, ignored -> oldCallback.countDown());
                first.deliver(SENDER, "stale generation message");
                assertTrue(oldCallback.await(2, TimeUnit.SECONDS));

                cli.execute("/disconnect");
                cli.execute("/connect 127.0.0.1 " + peer.port() + " 5051");
                ControlledTcpPeer.Connection second = peer.nextConnection();
                cli.execute("/login alice");
                await(() -> cli.authentication().presenceCredentials().isPresent());
                UUID secondGeneration = cli.transport().connectionId().orElseThrow();
                assertNotEquals(firstGeneration, secondGeneration);

                writer.release.countDown();
                CountDownLatch newCallback = new CountDownLatch(1);
                cli.transport().dispatcher().register(MessageType.CHAT_DELIVER, ignored -> newCallback.countDown());
                second.deliver(SENDER, "fresh generation message");
                assertTrue(newCallback.await(2, TimeUnit.SECONDS));
                await(() -> writer.lines.stream().anyMatch(line -> line.contains("fresh generation message")));
                assertFalse(writer.lines.stream().anyMatch(line -> line.contains("stale generation message")));
            } finally {
                writer.release.countDown(); cli.close();
            }
        }
    }

    @Test
    void staleGenerationAckAndErrorCannotConsumeCurrentGenerationChatRequest() throws Exception {
        try (ControlledTcpPeer peer = new ControlledTcpPeer()) {
            StringWriter sink = new StringWriter();
            NetSyncClient cli = newClient(new PrintWriter(sink, true));
            try {
                ControlledTcpPeer.Connection first = connectLogin(cli, peer);
                UUID oldGeneration = cli.transport().connectionId().orElseThrow();
                cli.execute("/chat " + RECIPIENT + " generation A request");
                first.nextRequest(MessageType.CHAT_SEND);
                cli.execute("/disconnect");

                cli.execute("/connect 127.0.0.1 " + peer.port() + " 5051");
                ControlledTcpPeer.Connection second = peer.nextConnection();
                cli.execute("/login alice");
                await(() -> cli.authentication().presenceCredentials().isPresent());
                UUID newGeneration = cli.transport().connectionId().orElseThrow();
                assertNotEquals(oldGeneration, newGeneration);
                cli.execute("/chat " + RECIPIENT + " generation B request");
                MessageEnvelope currentRequest = second.nextRequest(MessageType.CHAT_SEND);

                MessageEnvelope staleAck = new MessageEnvelope(1, MessageType.ACK, UUID.randomUUID(),
                        currentRequest.messageId(), new MessageBody.Ack(MessageType.CHAT_SEND, AckStatus.QUEUED, null));
                MessageEnvelope staleError = new MessageEnvelope(1, MessageType.ERROR, UUID.randomUUID(),
                        currentRequest.messageId(), new MessageBody.Error(ErrorCode.RECIPIENT_UNAVAILABLE, "stale", null));
                assertFalse(cli.transport().isGenerationCurrent(oldGeneration));
                assertTrue(cli.transport().dispatcher().dispatch(staleAck,
                        () -> cli.transport().isGenerationCurrent(oldGeneration)));
                assertTrue(cli.transport().dispatcher().dispatch(staleError,
                        () -> cli.transport().isGenerationCurrent(oldGeneration)));

                second.acknowledge(currentRequest, MessageType.CHAT_SEND, AckStatus.QUEUED);
                await(() -> synchronizedText(sink).contains("Queued for recipient " + RECIPIENT));
                assertFalse(synchronizedText(sink).contains("stale"));
            } finally { cli.close(); }
        }
    }

    private static NetSyncClient newClient(PrintWriter writer) {
        return new NetSyncClient(new TcpClient(), writer, () -> "disposable-alice".toCharArray(), 5051);
    }

    private static ControlledTcpPeer.Connection connectLogin(NetSyncClient cli, ControlledTcpPeer peer) throws Exception {
        cli.execute("/connect 127.0.0.1 " + peer.port() + " 5051");
        ControlledTcpPeer.Connection connection = peer.nextConnection();
        cli.execute("/login alice");
        await(() -> cli.authentication().presenceCredentials().isPresent());
        return connection;
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.yield();
        assertTrue(condition.getAsBoolean(), "Condition not reached before deadline");
    }

    private static String synchronizedText(StringWriter writer) {
        synchronized (writer) { return writer.toString(); }
    }

    private static final class BlockingWriter extends PrintWriter {
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        final AtomicBoolean first = new AtomicBoolean(true);
        final List<String> lines = new CopyOnWriteArrayList<>();
        BlockingWriter() { super(new StringWriter(), true); }
        @Override public void println(String text) {
            if (first.compareAndSet(true, false)) {
                entered.countDown();
                try { release.await(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            }
            lines.add(text);
        }
    }

    private static final class ThrowChatWriter extends PrintWriter {
        final AtomicBoolean failedOnce = new AtomicBoolean();
        final List<String> lines = new CopyOnWriteArrayList<>();
        ThrowChatWriter() { super(new StringWriter(), true); }
        @Override public void println(String text) {
            if (text.contains("controlled chat output exception") && failedOnce.compareAndSet(false, true))
                throw new IllegalStateException("controlled output failure");
            lines.add(text);
        }
    }
}
