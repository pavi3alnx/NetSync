package com.netsync.common;

import java.util.Optional;
/** Thread-safe bounded submission; accepted work wakes the Selector and never runs on the caller thread. */
public interface SelectorTasks {
    boolean submit(Runnable task);
    /** Reserve capacity before starting worker work; empty means reject that work. */
    Optional<Reservation> tryReserveCompletion();
    interface Reservation extends AutoCloseable {
        /** Consume once, enqueue and wake without capacity rejection; reuse throws IllegalStateException.
         * Shutdown must cancel/revoke the associated operation rather than silently lose completion. */
        void submit(Runnable completion);
        /** Release unused capacity; idempotent, including after submission. */
        @Override void close();
    }
}
