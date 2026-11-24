package com.netsync.client;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

/** One bounded output worker shared by command, authentication, and presence messages. */
final class ClientOutputQueue implements AutoCloseable {
    static final int DEFAULT_CAPACITY = 64;
    private static final String OVERFLOW_NOTICE = "Client output was omitted because its display queue filled; retry the command or run /presence to refresh.";

    private enum Kind { IMPORTANT, PRESENCE }
    private record Item(String text, UUID generation, Kind kind, UUID entity, boolean requireActive) {}

    private final Object lock = new Object();
    private final ArrayDeque<Item> pending = new ArrayDeque<>();
    private final Consumer<String> sink;
    private final Predicate<UUID> currentGeneration;
    private final Predicate<UUID> activeGeneration;
    private final int capacity;
    private final Thread worker;
    private boolean closing, closed;
    private long omitted, consumerFailures;

    ClientOutputQueue(Consumer<String> sink, Predicate<UUID> currentGeneration) {
        this(sink, currentGeneration, DEFAULT_CAPACITY);
    }

    ClientOutputQueue(Consumer<String> sink, Predicate<UUID> currentGeneration, int capacity) {
        this(sink, currentGeneration, currentGeneration, capacity);
    }

    ClientOutputQueue(Consumer<String> sink, Predicate<UUID> currentGeneration,
                      Predicate<UUID> activeGeneration, int capacity) {
        this.sink = Objects.requireNonNull(sink);
        this.currentGeneration = Objects.requireNonNull(currentGeneration);
        this.activeGeneration = Objects.requireNonNull(activeGeneration);
        if (capacity < 1) throw new IllegalArgumentException("Output capacity must be positive");
        this.capacity = capacity;
        worker = new Thread(this::run, "netsync-client-output");
        worker.setDaemon(true);
        worker.start();
    }

    boolean offer(String text) { return offerImportant(text, null); }

    boolean offerImportant(String text, UUID generation) {
        return offerItem(new Item(Objects.requireNonNull(text), generation, Kind.IMPORTANT, null, false));
    }

    boolean offerActiveImportant(String text, UUID generation) {
        return offerItem(new Item(Objects.requireNonNull(text), generation, Kind.IMPORTANT, null, true));
    }

    boolean offerPresence(UUID generation, UUID clientId, String text) {
        return offerItem(new Item(Objects.requireNonNull(text), Objects.requireNonNull(generation),
                Kind.PRESENCE, Objects.requireNonNull(clientId), true));
    }

    private boolean offerItem(Item item) {
        synchronized (lock) {
            if (closing || closed) return false;
            if (item.kind() == Kind.PRESENCE) {
                pending.removeIf(old -> old.kind() == Kind.PRESENCE
                        && old.generation().equals(item.generation()) && old.entity().equals(item.entity()));
            }
            if (pending.size() == capacity) {
                Item replaceable = pending.stream().filter(old -> old.kind() == Kind.PRESENCE).findFirst().orElse(null);
                if (replaceable != null) pending.remove(replaceable);
                else {
                    omitted++;
                    lock.notifyAll();
                    return false;
                }
                omitted++;
            }
            pending.addLast(item);
            lock.notifyAll();
            return true;
        }
    }

    private void run() {
        while (true) {
            Item item;
            long missing;
            synchronized (lock) {
                while (!closing && !closed && pending.isEmpty() && omitted == 0) {
                    try { lock.wait(); }
                    catch (InterruptedException interrupted) { if (closed) return; }
                }
                if (closed) return;
                if (closing && pending.isEmpty() && omitted == 0) return;
                missing = omitted;
                omitted = 0;
                item = pending.pollFirst();
            }
            if (missing > 0) writeSafely(OVERFLOW_NOTICE + " (" + missing + " message(s)).");
            if (item != null && (item.generation() == null || isCurrent(item)))
                writeSafely(item.text());
        }
    }

    private boolean isCurrent(Item item) {
        try {
            return currentGeneration.test(item.generation())
                    && (!item.requireActive() || activeGeneration.test(item.generation()));
        }
        catch (RuntimeException failure) { return false; }
    }

    private void writeSafely(String text) {
        try { sink.accept(text); }
        catch (RuntimeException failure) { synchronized (lock) { consumerFailures++; } }
    }

    int pendingCount() { synchronized (lock) { return pending.size(); } }
    long omittedCount() { synchronized (lock) { return omitted; } }
    long consumerFailureCount() { synchronized (lock) { return consumerFailures; } }

    @Override public void close() {
        synchronized (lock) { closing = true; lock.notifyAll(); }
        if (Thread.currentThread() != worker) {
            try { worker.join(100); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }
        synchronized (lock) { closed = true; pending.clear(); lock.notifyAll(); }
        worker.interrupt();
    }
}
