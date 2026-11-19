package com.netsync.server;

import com.netsync.common.SelectorTasks;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class SelectorTaskQueueTest {
    private SelectorTaskQueue started(int capacity, Runnable wakeup) {
        SelectorTaskQueue queue = new SelectorTaskQueue(capacity, wakeup);
        queue.start(Thread.currentThread());
        return queue;
    }

    @Test void rejectsBeforeStartupAndAfterShutdown() {
        SelectorTaskQueue queue = new SelectorTaskQueue(2, () -> {});
        assertFalse(queue.submit(() -> fail("Must not execute")));
        assertTrue(queue.tryReserveCompletion().isEmpty());
        queue.start(Thread.currentThread());
        queue.finishShutdown();
        assertFalse(queue.submit(() -> fail("Must not execute")));
        assertTrue(queue.tryReserveCompletion().isEmpty());
        assertThrows(IllegalStateException.class, () -> queue.start(Thread.currentThread()));
    }

    @Test void backgroundSubmissionWakesButNeverExecutesOnCaller() throws Exception {
        AtomicInteger wakes = new AtomicInteger(), executions = new AtomicInteger();
        SelectorTaskQueue queue = started(2, wakes::incrementAndGet);
        CompletableFuture<Boolean> submitted = CompletableFuture.supplyAsync(() -> queue.submit(executions::incrementAndGet));
        assertTrue(submitted.get(3, TimeUnit.SECONDS));
        assertEquals(1, wakes.get());
        assertEquals(0, executions.get());
        queue.runTasks(1);
        assertEquals(1, executions.get());
    }

    @Test void concurrentProducersCannotExceedCapacity() throws Exception {
        SelectorTaskQueue queue = started(8, () -> {});
        AtomicInteger accepted = new AtomicInteger(), executed = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService workers = Executors.newFixedThreadPool(8)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 64; i++) futures.add(workers.submit(() -> {
                try {
                    if (!start.await(3, TimeUnit.SECONDS)) throw new IllegalStateException("Test gate timed out");
                    if (queue.submit(executed::incrementAndGet)) accepted.incrementAndGet();
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }));
            start.countDown();
            for (Future<?> future : futures) future.get(3, TimeUnit.SECONDS);
        }
        assertEquals(8, accepted.get());
        queue.runTasks(8);
        assertEquals(8, executed.get());
    }

    @Test void reservationSharesCapacityAndGuaranteesCompletionEnqueue() {
        SelectorTaskQueue queue = started(2, () -> {});
        List<String> results = new ArrayList<>();
        SelectorTasks.Reservation reserved = queue.tryReserveCompletion().orElseThrow();
        assertTrue(queue.submit(() -> results.add("ordinary")));
        assertFalse(queue.submit(() -> fail("Over capacity")));
        assertTrue(queue.tryReserveCompletion().isEmpty());
        reserved.submit(() -> results.add("completion"));
        reserved.close(); // Closing a submitted reservation must not free its queued slot.
        assertFalse(queue.submit(() -> fail("Still over capacity")));
        assertThrows(IllegalStateException.class, () -> reserved.submit(() -> {}));
        queue.runTasks(2);
        assertEquals(List.of("ordinary", "completion"), results);
        assertTrue(queue.submit(() -> {}));
    }

    @Test void unusedReservationReleaseIsIdempotentAndDoesNotCancel() {
        SelectorTaskQueue queue = started(1, () -> {});
        AtomicInteger cancellations = new AtomicInteger();
        SelectorTasks.Reservation reservation = queue.tryReserveCompletion(cancellations::incrementAndGet).orElseThrow();
        reservation.close();
        reservation.close();
        assertThrows(IllegalStateException.class, () -> reservation.submit(() -> {}));
        assertTrue(queue.submit(() -> {}));
        queue.finishShutdown();
        assertEquals(0, cancellations.get());
    }

    @Test void shutdownRevokesUnusedReservationAndRunsCancellationOnceOnOwner() throws Exception {
        SelectorTaskQueue queue = started(1, () -> {});
        List<Thread> cancelledOn = new ArrayList<>();
        SelectorTasks.Reservation reservation = queue.tryReserveCompletion(() -> cancelledOn.add(Thread.currentThread())).orElseThrow();
        CompletableFuture.runAsync(queue::beginShutdown).get(3, TimeUnit.SECONDS);
        assertTrue(cancelledOn.isEmpty());
        assertThrows(IllegalStateException.class, () -> reservation.submit(() -> fail("Late completion")));
        reservation.close();
        queue.finishShutdown();
        queue.finishShutdown();
        assertEquals(List.of(Thread.currentThread()), cancelledOn);
    }

    @Test void shutdownDrainsAcceptedCompletionInsteadOfCancellingIt() {
        SelectorTaskQueue queue = started(2, () -> {});
        List<String> results = new ArrayList<>();
        SelectorTasks.Reservation reservation = queue.tryReserveCompletion(() -> results.add("cancelled")).orElseThrow();
        queue.submit(() -> results.add("task"));
        reservation.submit(() -> results.add("completed"));
        queue.beginShutdown();
        assertFalse(queue.submit(() -> fail("Rejected task")));
        queue.finishShutdown();
        assertEquals(List.of("task", "completed"), results);
    }

    @Test void taskAndCancellationExceptionsDoNotLoseOtherTasks() {
        SelectorTaskQueue queue = started(3, () -> {});
        AtomicInteger completed = new AtomicInteger();
        queue.submit(() -> { throw new IllegalStateException("private task data"); });
        queue.tryReserveCompletion(() -> { throw new IllegalArgumentException("private cancellation data"); }).orElseThrow();
        queue.submit(completed::incrementAndGet);
        queue.finishShutdown();
        assertEquals(1, completed.get());
    }

    @Test void batchBudgetLeavesRemainingWorkQueued() {
        SelectorTaskQueue queue = started(5, () -> {});
        AtomicInteger completed = new AtomicInteger();
        for (int i = 0; i < 5; i++) assertTrue(queue.submit(completed::incrementAndGet));
        queue.runTasks(2);
        assertEquals(2, completed.get());
        assertTrue(queue.hasTasks());
        queue.runTasks(3);
        assertEquals(5, completed.get());
        assertFalse(queue.hasTasks());
    }

    @Test void onlyOwnerCanExecuteQueuedWork() throws Exception {
        SelectorTaskQueue queue = started(1, () -> {});
        CompletableFuture.runAsync(() -> assertThrows(IllegalStateException.class, () -> queue.runTasks(1)))
                .get(3, TimeUnit.SECONDS);
    }

    @Test void completionRacingShutdownEitherExecutesOrCancelsExactlyOnce() throws Exception {
        try (ExecutorService workers = Executors.newFixedThreadPool(2)) {
            for (int i = 0; i < 50; i++) {
                SelectorTaskQueue queue = started(1, () -> {});
                AtomicInteger completed = new AtomicInteger(), cancelled = new AtomicInteger();
                SelectorTasks.Reservation reservation = queue.tryReserveCompletion(cancelled::incrementAndGet).orElseThrow();
                CyclicBarrier gate = new CyclicBarrier(2);
                Future<?> completion = workers.submit(() -> {
                    await(gate);
                    try { reservation.submit(completed::incrementAndGet); }
                    catch (IllegalStateException expectedShutdown) { }
                });
                Future<?> shutdown = workers.submit(() -> { await(gate); queue.beginShutdown(); });
                completion.get(3, TimeUnit.SECONDS);
                shutdown.get(3, TimeUnit.SECONDS);
                queue.finishShutdown();
                assertEquals(1, completed.get() + cancelled.get());
            }
        }
    }

    private void await(CyclicBarrier gate) {
        try { gate.await(3, TimeUnit.SECONDS); }
        catch (Exception e) { throw new IllegalStateException("Test barrier failed", e); }
    }
}
