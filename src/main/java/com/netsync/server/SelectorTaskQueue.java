package com.netsync.server;

import com.netsync.common.SelectorTasks;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Bounded selector mailbox: queued tasks plus unused completion reservations share capacity.
 * Rejects submissions before startup and after shutdown begins. Accepted tasks are drained
 * on the selector thread during shutdown; this does not drain socket output.
 */
public final class SelectorTaskQueue implements SelectorTasks {
    private static final System.Logger LOG = System.getLogger(SelectorTaskQueue.class.getName());
    private enum State { NEW, RUNNING, STOPPING, TERMINATED }
    private enum ReservationState { RESERVED, SUBMITTED, RELEASED, CANCELLED }
    private final int capacity;
    private final Runnable wakeup;
    private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
    private final Set<CompletionReservation> reservations = new HashSet<>();
    private State state = State.NEW;
    private Thread owner;

    SelectorTaskQueue(int capacity, Runnable wakeup) {
        if (capacity < 1) throw new IllegalArgumentException("Task capacity must be positive");
        this.capacity = capacity;
        this.wakeup = Objects.requireNonNull(wakeup);
    }

    synchronized void start(Thread owner) {
        if (state != State.NEW) throw new IllegalStateException("Task queue is single-use");
        this.owner = Objects.requireNonNull(owner);
        state = State.RUNNING;
    }

    @Override public boolean submit(Runnable task) {
        Objects.requireNonNull(task);
        synchronized (this) {
            if (state != State.RUNNING || queue.size() + reservations.size() >= capacity) return false;
            queue.add(task);
        }
        wakeup.run();
        return true;
    }

    /**
     * Reserves capacity only. Owners using this shared API must cancel associated work via
     * their lifecycle hooks when the server stops. A late submit throws rather than losing work.
     * Prefer the cancellation-aware overload when the operation has a cancellation action.
     */
    @Override public Optional<Reservation> tryReserveCompletion() {
        return tryReserveCompletion(() -> {});
    }

    /**
     * The callback cancels the associated operation if shutdown revokes an unused reservation.
     * It runs once on the selector thread and must not block. close() releases unused capacity
     * without invoking cancellation; an accepted completion runs instead of cancellation.
     */
    public Optional<Reservation> tryReserveCompletion(Runnable onCancellation) {
        Objects.requireNonNull(onCancellation);
        synchronized (this) {
            if (state != State.RUNNING || queue.size() + reservations.size() >= capacity) return Optional.empty();
            CompletionReservation reservation = new CompletionReservation(onCancellation);
            reservations.add(reservation);
            return Optional.of(reservation);
        }
    }

    synchronized boolean hasTasks() { return !queue.isEmpty(); }
    synchronized boolean isShutdown() { return state == State.STOPPING || state == State.TERMINATED; }

    /** A finite batch ensures that background producers cannot starve socket readiness. */
    void runTasks(int budget) {
        if (Thread.currentThread() != owner) throw new IllegalStateException("Selector thread required");
        for (int i = 0; i < budget; i++) {
            Runnable task;
            synchronized (this) { task = queue.poll(); }
            if (task == null) return;
            try { task.run(); }
            catch (RuntimeException e) {
                // Task messages/causes can contain credentials; log only the exception type.
                LOG.log(System.Logger.Level.WARNING, "Selector task failed (" + e.getClass().getName() + ")");
            }
        }
    }

    void beginShutdown() {
        synchronized (this) {
            if (isShutdown()) return;
            state = State.STOPPING;
            // Each cancellation replaces its reserved slot, so this cannot exceed capacity.
            for (CompletionReservation reservation : reservations) {
                reservation.status = ReservationState.CANCELLED;
                queue.add(reservation.cancellation);
                reservation.cancellation = null;
            }
            reservations.clear();
        }
        wakeup.run();
    }

    void finishShutdown() {
        beginShutdown();
        runTasks(capacity); // No new work is accepted; at most capacity entries remain.
        synchronized (this) { state = State.TERMINATED; }
    }

    private final class CompletionReservation implements Reservation {
        private ReservationState status = ReservationState.RESERVED;
        private Runnable cancellation;

        CompletionReservation(Runnable cancellation) { this.cancellation = cancellation; }

        @Override public void submit(Runnable completion) {
            Objects.requireNonNull(completion);
            synchronized (SelectorTaskQueue.this) {
                if (status != ReservationState.RESERVED || state != State.RUNNING) {
                    throw new IllegalStateException("Completion reservation is no longer available");
                }
                reservations.remove(this);
                status = ReservationState.SUBMITTED;
                cancellation = null;
                queue.add(completion); // Replaces reserved capacity; no capacity rejection.
            }
            wakeup.run();
        }

        @Override public void close() {
            synchronized (SelectorTaskQueue.this) {
                if (status == ReservationState.RESERVED) {
                    reservations.remove(this);
                    status = ReservationState.RELEASED;
                    cancellation = null;
                }
            }
        }
    }
}
