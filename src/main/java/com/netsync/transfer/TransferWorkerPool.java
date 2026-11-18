package com.netsync.transfer;

import com.netsync.common.ProtocolLimits;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounded worker pool dedicated to blocking disk I/O and SHA-256 checksumming.
 * <p>
 * Enforces exactly 2 worker threads and a bounded task queue of 32 tasks as required
 * by protocol specifications. Task rejection throws {@link RejectedExecutionException}
 * to permit the caller to emit {@link com.netsync.common.ErrorCode#BUSY} without blocking
 * the NIO Selector event loop.
 */
public final class TransferWorkerPool implements AutoCloseable {
    private final ThreadPoolExecutor executor;

    public TransferWorkerPool() {
        this(ProtocolLimits.WORKER_THREADS, ProtocolLimits.WORKER_QUEUE_SIZE);
    }

    public TransferWorkerPool(int threadCount, int queueCapacity) {
        if (threadCount <= 0) {
            throw new IllegalArgumentException("Thread count must be positive, got: " + threadCount);
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("Queue capacity must be positive, got: " + queueCapacity);
        }

        var threadFactory = new NamedWorkerThreadFactory("netsync-transfer-worker-");
        var workQueue = new ArrayBlockingQueue<Runnable>(queueCapacity);

        this.executor = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                0L,
                TimeUnit.MILLISECONDS,
                workQueue,
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    /**
     * Submits a task to the bounded worker pool.
     *
     * @param task the task to execute
     * @throws RejectedExecutionException if the queue of 32 tasks is full or the pool is shut down
     * @throws NullPointerException       if task is null
     */
    public void submit(Runnable task) {
        Objects.requireNonNull(task, "Task must not be null");
        executor.execute(task);
    }

    /**
     * Submits a value-returning task to the bounded worker pool.
     *
     * @param task the task to execute
     * @param <T>  result type
     * @return a Future representing pending completion
     * @throws RejectedExecutionException if the queue is full or the pool is shut down
     */
    public <T> Future<T> submit(Callable<T> task) {
        Objects.requireNonNull(task, "Task must not be null");
        return executor.submit(task);
    }

    public int activeThreadCount() {
        return executor.getActiveCount();
    }

    public int poolSize() {
        return executor.getPoolSize();
    }

    public int queueSize() {
        return executor.getQueue().size();
    }

    public int queueRemainingCapacity() {
        return executor.getQueue().remainingCapacity();
    }

    public boolean isShutdown() {
        return executor.isShutdown();
    }

    public boolean isTerminated() {
        return executor.isTerminated();
    }

    public void shutdown() {
        executor.shutdown();
    }

    public List<Runnable> shutdownNow() {
        return executor.shutdownNow();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }

    @Override
    public void close() {
        shutdown();
        try {
            if (!awaitTermination(ProtocolLimits.SHUTDOWN_GRACE_SECONDS, TimeUnit.SECONDS)) {
                shutdownNow();
            }
        } catch (InterruptedException e) {
            shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static final class NamedWorkerThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        NamedWorkerThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, prefix + threadNumber.getAndIncrement());
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, e) ->
                    System.getLogger(TransferWorkerPool.class.getName())
                            .log(System.Logger.Level.WARNING, "Uncaught error in worker thread " + t.getName(), e));
            return thread;
        }
    }
}
