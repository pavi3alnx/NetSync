package com.netsync.client;

import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class ClientOutputQueueTest {
    private static final UUID GENERATION = UUID.randomUUID();
    private static final UUID ALICE = UUID.randomUUID();

    @Test
    void blockedOutputDoesNotBlockPresenceDispatchOrFollowingHandlers() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), nextHandler = new CountDownLatch(1);
        ClientOutputQueue output = new ClientOutputQueue(text -> {
            entered.countDown();
            awaitUninterruptibly(release);
        }, GENERATION::equals);
        ClientDispatcher dispatcher = new ClientDispatcher();
        ExecutorService dispatch = Executors.newSingleThreadExecutor(task -> new Thread(task, "test-client-dispatch"));
        dispatcher.register(MessageType.PRESENCE_UPDATE, message ->
                output.offerPresence(GENERATION, ALICE, "presence"));
        dispatcher.register(MessageType.ERROR, message -> nextHandler.countDown());
        try {
            Future<?> firstHandler = dispatch.submit(() -> dispatcher.dispatch(presence()));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            Future<Boolean> secondHandler = dispatch.submit(() -> dispatcher.dispatch(error()));
            assertTrue(secondHandler.get(1, TimeUnit.SECONDS));
            assertTrue(nextHandler.await(1, TimeUnit.SECONDS));
            firstHandler.get(1, TimeUnit.SECONDS);
            assertEquals(0, output.pendingCount());
        } finally {
            release.countDown(); dispatch.shutdownNow(); output.close(); dispatcher.close();
            assertTrue(dispatch.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void newerStateCoalescesOverOlderStateForSameClientAndConnection() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), latest = new CountDownLatch(1);
        List<String> written = new CopyOnWriteArrayList<>();
        ClientOutputQueue output = new ClientOutputQueue(text -> {
            if (text.equals("block")) { entered.countDown(); awaitUninterruptibly(release); }
            else { written.add(text); if (text.equals("alice offline")) latest.countDown(); }
        }, GENERATION::equals, 3);
        try {
            output.offer("block"); assertTrue(entered.await(2, TimeUnit.SECONDS));
            output.offerPresence(GENERATION, ALICE, "alice online");
            output.offerPresence(GENERATION, ALICE, "alice offline");
            assertEquals(1, output.pendingCount());
            release.countDown();
            assertTrue(latest.await(2, TimeUnit.SECONDS));
            assertFalse(written.contains("alice online"));
        } finally { release.countDown(); output.close(); }
    }

    @Test
    void coalescesStateAndMakesOverflowVisibleWithoutExceedingCapacity() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), completed = new CountDownLatch(1);
        CountDownLatch commandWritten = new CountDownLatch(1);
        List<String> written = new CopyOnWriteArrayList<>();
        ClientOutputQueue output = new ClientOutputQueue(text -> {
            entered.countDown(); awaitUninterruptibly(release); written.add(text);
            if (text.startsWith("Client output was omitted")) completed.countDown();
            if (text.equals("command error")) commandWritten.countDown();
        }, GENERATION::equals, 2);
        try {
            output.offerImportant("block", null);
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            output.offerPresence(GENERATION, ALICE, "alice online");
            output.offerImportant("correlated query result", GENERATION);
            assertTrue(output.offerImportant("command error", GENERATION)); // replaces queued presence state
            assertTrue(output.pendingCount() <= 2);
            assertTrue(output.omittedCount() > 0);
            release.countDown();
            assertTrue(completed.await(2, TimeUnit.SECONDS));
            assertTrue(commandWritten.await(2, TimeUnit.SECONDS));
            assertTrue(written.contains("correlated query result"));
            assertTrue(written.contains("command error"));
            assertFalse(written.contains("alice online"));
            assertTrue(written.stream().anyMatch(text -> text.contains("retry the command or run /presence")));
        } finally { release.countDown(); output.close(); }
    }

    @Test
    void outputConsumerFailureIsContainedAndNextItemStillWrites() throws Exception {
        CountDownLatch second = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean(true);
        ClientOutputQueue output = new ClientOutputQueue(text -> {
            if (first.getAndSet(false)) throw new IllegalStateException("test sink failure");
            second.countDown();
        }, GENERATION::equals);
        try {
            output.offerImportant("first", GENERATION);
            output.offerImportant("second", GENERATION);
            assertTrue(second.await(2, TimeUnit.SECONDS));
            assertEquals(1, output.consumerFailureCount());
        } finally { output.close(); }
    }

    @Test
    void oldGenerationStateIsDiscardedAfterReconnect() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), freshWritten = new CountDownLatch(1);
        UUID newGeneration = UUID.randomUUID();
        AtomicReference<UUID> current = new AtomicReference<>(GENERATION);
        List<String> written = new CopyOnWriteArrayList<>();
        ClientOutputQueue output = new ClientOutputQueue(text -> {
            if (text.equals("block")) { entered.countDown(); awaitUninterruptibly(release); }
            else { written.add(text); if (text.equals("fresh")) freshWritten.countDown(); }
        }, generation -> generation.equals(current.get()), 4);
        try {
            output.offer("block"); assertTrue(entered.await(2, TimeUnit.SECONDS));
            output.offerPresence(GENERATION, ALICE, "old notification");
            current.set(newGeneration);
            output.offerPresence(newGeneration, ALICE, "fresh");
            release.countDown();
            assertTrue(freshWritten.await(2, TimeUnit.SECONDS));
            assertFalse(written.contains("old notification"));
        } finally { release.countDown(); output.close(); }
    }

    @Test
    void closeReturnsPromptlyWhenOutputConsumerIsBlocked() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        ClientOutputQueue output = new ClientOutputQueue(text -> { entered.countDown(); awaitUninterruptibly(release); },
                GENERATION::equals);
        try {
            output.offer("block"); assertTrue(entered.await(2, TimeUnit.SECONDS));
            long started = System.nanoTime();
            output.close();
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(1));
        } finally { release.countDown(); output.close(); }
    }

    private static MessageEnvelope presence() {
        return new MessageEnvelope(1, MessageType.PRESENCE_UPDATE, UUID.randomUUID(), null,
                new MessageBody.PresenceUpdate(ALICE, "alice", com.netsync.common.PresenceState.ONLINE));
    }
    private static MessageEnvelope error() {
        return new MessageEnvelope(1, MessageType.ERROR, UUID.randomUUID(), null,
                new MessageBody.Error(com.netsync.common.ErrorCode.INVALID_REQUEST, "safe", null));
    }
    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try { latch.await(); break; }
            catch (InterruptedException ignored) { interrupted = true; }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }
}
