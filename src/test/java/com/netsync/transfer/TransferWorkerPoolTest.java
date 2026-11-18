package com.netsync.transfer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TransferWorkerPoolTest {

    @Test
    @DisplayName("Worker pool initializes with exactly two worker threads and 32 queue capacity")
    void defaultConfigurationBounds() {
        try (var pool = new TransferWorkerPool()) {
            assertEquals(32, pool.queueRemainingCapacity());
            assertEquals(0, pool.queueSize());
            assertEquals(0, pool.activeThreadCount());
        }
    }

    @Test
    @DisplayName("Invalid constructor arguments are rejected")
    void invalidConstructorArguments() {
        assertThrows(IllegalArgumentException.class, () -> new TransferWorkerPool(0, 32));
        assertThrows(IllegalArgumentException.class, () -> new TransferWorkerPool(-1, 32));
        assertThrows(IllegalArgumentException.class, () -> new TransferWorkerPool(2, 0));
        assertThrows(IllegalArgumentException.class, () -> new TransferWorkerPool(2, -1));
    }

    @Test
    @DisplayName("Never exceeds two worker threads under load")
    void maxTwoWorkerThreads() throws Exception {
        try (var pool = new TransferWorkerPool()) {
            CountDownLatch taskStart = new CountDownLatch(2);
            CountDownLatch taskHold = new CountDownLatch(1);
            ConcurrentHashMap<String, Boolean> threadNames = new ConcurrentHashMap<>();

            for (int i = 0; i < 5; i++) {
                pool.submit(() -> {
                    Thread current = Thread.currentThread();
                    assertTrue(current.isDaemon(), "Worker thread must be daemon");
                    assertTrue(current.getName().startsWith("netsync-transfer-worker-"));
                    threadNames.put(current.getName(), true);
                    taskStart.countDown();
                    try {
                        taskHold.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                    }
                });
            }

            assertTrue(taskStart.await(3, TimeUnit.SECONDS), "First two worker tasks must start concurrently");
            assertEquals(2, pool.activeThreadCount());
            assertEquals(2, pool.poolSize());
            assertEquals(3, pool.queueSize());
            assertTrue(threadNames.size() <= 2, "There must be at most 2 distinct worker threads");

            taskHold.countDown();
        }
    }

    @Test
    @DisplayName("Rejects 33rd queued task with RejectedExecutionException when queue of 32 is full")
    void queueSaturationRejection() throws Exception {
        try (var pool = new TransferWorkerPool(2, 32)) {
            CountDownLatch workersOccupied = new CountDownLatch(2);
            CountDownLatch releaseWorkers = new CountDownLatch(1);
            AtomicInteger completedTasks = new AtomicInteger(0);

            // Saturate 2 active worker threads
            for (int i = 0; i < 2; i++) {
                pool.submit(() -> {
                    workersOccupied.countDown();
                    try {
                        releaseWorkers.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                    }
                    completedTasks.incrementAndGet();
                });
            }

            assertTrue(workersOccupied.await(3, TimeUnit.SECONDS));

            // Fill all 32 queue slots
            for (int i = 0; i < 32; i++) {
                pool.submit(completedTasks::incrementAndGet);
            }

            assertEquals(32, pool.queueSize());
            assertEquals(0, pool.queueRemainingCapacity());

            // 33rd queued task (35th total) must be rejected
            assertThrows(RejectedExecutionException.class, () -> pool.submit(() -> {}));
            assertThrows(RejectedExecutionException.class, () -> pool.submit(() -> "result"));

            releaseWorkers.countDown();
        }
    }

    @Test
    @DisplayName("Safe shutdown and termination handling with pending tasks returned")
    void shutdownHandling() throws Exception {
        var pool = new TransferWorkerPool();
        assertFalse(pool.isShutdown());

        CountDownLatch latch = new CountDownLatch(1);
        pool.submit(latch::countDown);
        assertTrue(latch.await(3, TimeUnit.SECONDS));

        pool.shutdown();
        assertTrue(pool.isShutdown());
        assertThrows(RejectedExecutionException.class, () -> pool.submit(() -> {}));
        assertTrue(pool.awaitTermination(3, TimeUnit.SECONDS));
        assertTrue(pool.isTerminated());
    }

    @Test
    @DisplayName("shutdownNow returns pending tasks and null submissions are rejected")
    void shutdownNowAndNullTaskGuards() throws Exception {
        var pool = new TransferWorkerPool(2, 32);

        assertThrows(NullPointerException.class, () -> pool.submit((Runnable) null));
        assertThrows(NullPointerException.class, () -> pool.submit((Callable<String>) null));

        CountDownLatch occupied = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);

        for (int i = 0; i < 2; i++) {
            pool.submit(() -> {
                occupied.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {}
            });
        }
        assertTrue(occupied.await(3, TimeUnit.SECONDS));

        // Submit 3 queued tasks
        for (int i = 0; i < 3; i++) {
            pool.submit(() -> {});
        }
        assertEquals(3, pool.queueSize());

        // shutdownNow should drain and return the 3 queued tasks
        var dropped = pool.shutdownNow();
        assertEquals(3, dropped.size());
        assertTrue(pool.isShutdown());

        release.countDown();
        pool.close();
    }
}
