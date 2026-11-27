package com.netsync.gui;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Bounded, generation-aware, coalescing event dispatcher for JavaFX or headless UI consumers.
 * <p>
 * Guarantees that:
 * <ul>
 *   <li>At most one drain task is scheduled onto the UI executor at any time (never floods {@code Platform.runLater}).</li>
 *   <li>Presence updates and transfer progress updates coalesce by entity ID (latest wins).</li>
 *   <li>Distinct important events (chat messages, auth transitions, terminal transfer outcomes) are never overwritten by coalescing.</li>
 *   <li>On queue saturation, coalescable events are evicted first; if still full, overflow is counted and explicitly reported.</li>
 *   <li>Events belonging to stale generations or delivered after {@link #close()} are discarded.</li>
 *   <li>RuntimeExceptions thrown by UI consumers are contained and never propagate to networking threads.</li>
 * </ul>
 */
public final class UiEventDispatcher implements AutoCloseable {
    public static final int DEFAULT_CAPACITY = 256;
    private static final int DRAIN_BATCH_LIMIT = 64;

    public enum Category {
        IMPORTANT,
        PRESENCE,
        TRANSFER_PROGRESS
    }

    private record QueuedEvent(Category category, UUID generation, UUID coalesceKey, Runnable action) {}

    private final Object lock = new Object();
    private final ArrayDeque<QueuedEvent> queue = new ArrayDeque<>();
    private final Consumer<Runnable> uiExecutor;
    private final LongConsumer overflowHandler;
    private final int capacity;

    private UUID activeGeneration;
    private boolean drainScheduled;
    private boolean closed;
    private long droppedOverflowCount;
    private long consumerFailureCount;

    public UiEventDispatcher(Consumer<Runnable> uiExecutor) {
        this(uiExecutor, DEFAULT_CAPACITY, ignored -> {});
    }

    public UiEventDispatcher(Consumer<Runnable> uiExecutor, int capacity, LongConsumer overflowHandler) {
        this.uiExecutor = Objects.requireNonNull(uiExecutor, "uiExecutor");
        this.overflowHandler = Objects.requireNonNull(overflowHandler, "overflowHandler");
        if (capacity < 1) {
            throw new IllegalArgumentException("Dispatcher capacity must be positive");
        }
        this.capacity = capacity;
    }

    /** Updates the currently active session/connection generation; older queued generation-bound events are purged. */
    public void setActiveGeneration(UUID generation) {
        synchronized (lock) {
            this.activeGeneration = generation;
            queue.removeIf(item -> item.generation() != null && !item.generation().equals(generation));
        }
    }

    public UUID activeGeneration() {
        synchronized (lock) {
            return activeGeneration;
        }
    }

    /** Enqueues an important event that must not be coalesced away. {@code generation} may be null for global UI events. */
    public boolean postImportant(UUID generation, Runnable action) {
        return enqueue(Category.IMPORTANT, generation, null, action);
    }

    /** Enqueues a coalescable presence update keyed by user/client UUID. */
    public boolean postPresence(UUID generation, UUID clientId, Runnable action) {
        Objects.requireNonNull(clientId, "clientId");
        return enqueue(Category.PRESENCE, generation, clientId, action);
    }

    /** Enqueues a coalescable transfer progress update keyed by transfer UUID. */
    public boolean postTransferProgress(UUID generation, UUID transferId, Runnable action) {
        Objects.requireNonNull(transferId, "transferId");
        return enqueue(Category.TRANSFER_PROGRESS, generation, transferId, action);
    }

    private boolean enqueue(Category category, UUID generation, UUID coalesceKey, Runnable action) {
        Objects.requireNonNull(action, "action");
        boolean shouldSchedule = false;
        synchronized (lock) {
            if (closed) {
                return false;
            }
            if (generation != null && !generation.equals(activeGeneration)) {
                return false;
            }

            if (category != Category.IMPORTANT && coalesceKey != null) {
                Iterator<QueuedEvent> it = queue.iterator();
                while (it.hasNext()) {
                    QueuedEvent existing = it.next();
                    if (existing.category() == category
                            && Objects.equals(existing.generation(), generation)
                            && coalesceKey.equals(existing.coalesceKey())) {
                        it.remove();
                        break;
                    }
                }
            }

            if (queue.size() >= capacity) {
                if (!evictOneCoalescableLocked()) {
                    droppedOverflowCount++;
                    if (!drainScheduled) {
                        drainScheduled = true;
                        shouldSchedule = true;
                    }
                    if (shouldSchedule) {
                        scheduleDrainSafe();
                    }
                    return false;
                }
            }

            queue.addLast(new QueuedEvent(category, generation, coalesceKey, action));
            if (!drainScheduled) {
                drainScheduled = true;
                shouldSchedule = true;
            }
        }
        if (shouldSchedule) {
            scheduleDrainSafe();
        }
        return true;
    }

    private boolean evictOneCoalescableLocked() {
        Iterator<QueuedEvent> it = queue.iterator();
        while (it.hasNext()) {
            QueuedEvent candidate = it.next();
            if (candidate.category() != Category.IMPORTANT) {
                it.remove();
                return true;
            }
        }
        return false;
    }

    private void scheduleDrainSafe() {
        try {
            uiExecutor.accept(this::drainBatch);
        } catch (RuntimeException failure) {
            synchronized (lock) {
                drainScheduled = false;
                consumerFailureCount++;
            }
        }
    }

    private void drainBatch() {
        for (int processed = 0; processed < DRAIN_BATCH_LIMIT; processed++) {
            QueuedEvent next = null;
            long overflowToReport = 0;
            synchronized (lock) {
                if (closed) {
                    queue.clear();
                    drainScheduled = false;
                    return;
                }
                if (droppedOverflowCount > 0) {
                    overflowToReport = droppedOverflowCount;
                    droppedOverflowCount = 0;
                } else {
                    next = queue.pollFirst();
                    if (next == null) {
                        drainScheduled = false;
                        return;
                    }
                    if (next.generation() != null && !next.generation().equals(activeGeneration)) {
                        continue;
                    }
                }
            }
            if (overflowToReport > 0) {
                try {
                    overflowHandler.accept(overflowToReport);
                } catch (RuntimeException failure) {
                    synchronized (lock) {
                        consumerFailureCount++;
                    }
                }
            } else if (next != null) {
                try {
                    next.action().run();
                } catch (RuntimeException failure) {
                    synchronized (lock) {
                        consumerFailureCount++;
                    }
                }
            }
        }

        boolean reschedule;
        synchronized (lock) {
            reschedule = !closed && (!queue.isEmpty() || droppedOverflowCount > 0);
            if (!reschedule) {
                drainScheduled = false;
            }
        }
        if (reschedule) {
            scheduleDrainSafe();
        }
    }

    public int pendingCount() {
        synchronized (lock) {
            return queue.size();
        }
    }

    public long consumerFailureCount() {
        synchronized (lock) {
            return consumerFailureCount;
        }
    }

    public boolean isClosed() {
        synchronized (lock) {
            return closed;
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            queue.clear();
            activeGeneration = null;
            drainScheduled = false;
        }
    }
}
