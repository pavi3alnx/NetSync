package com.netsync.client;

import com.netsync.common.*;
import java.util.*;
import java.util.function.Consumer;

/** Thread-safe registration. Callbacks run serially on the client's dispatch thread.
 * Close prevents future invocations; an invocation already in progress may finish.
 */
public final class ClientDispatcher implements AutoCloseable {
    public interface Registration extends AutoCloseable { @Override void close(); }
    private final Map<MessageType, List<Slot<MessageEnvelope>>> json = new EnumMap<>(MessageType.class);
    private final List<Slot<FileChunk>> binary = new ArrayList<>();
    private boolean closed;
    private long unhandled, failures;

    public synchronized Registration register(MessageType type, Consumer<MessageEnvelope> handler) {
        Objects.requireNonNull(type);
        if (type == MessageType.FILE_CHUNK) throw new IllegalArgumentException("Use registerBinary for chunks");
        return add(json.computeIfAbsent(type, ignored -> new ArrayList<>()), handler);
    }
    public synchronized Registration registerBinary(Consumer<FileChunk> handler) { return add(binary, handler); }
    private <T> Registration add(List<Slot<T>> slots, Consumer<T> handler) {
        Objects.requireNonNull(handler);
        if (closed) throw new IllegalStateException("Dispatcher is closed");
        if (slots.stream().anyMatch(s -> s.active && s.handler == handler))
            throw new IllegalArgumentException("Handler already registered");
        Slot<T> slot = new Slot<>(handler);
        slots.add(slot);
        return () -> { synchronized (this) { slot.active = false; slots.remove(slot); } };
    }
    /** False means a binary chunk had no receiver; transport must close rather than lose data. */
    boolean dispatch(OutboundFrame frame) {
        if (frame instanceof MessageEnvelope message) {
            List<Slot<MessageEnvelope>> slots;
            synchronized (this) {
                if (closed) return true;
                slots = List.copyOf(json.getOrDefault(message.type(), List.of()));
                if (slots.isEmpty()) unhandled++;
            }
            invoke(slots, message);
            return true;
        }
        List<Slot<FileChunk>> slots;
        synchronized (this) { if (closed) return false; slots = List.copyOf(binary); }
        if (slots.isEmpty()) return false;
        return invoke(slots, (FileChunk) frame) > 0;
    }
    private <T> int invoke(List<Slot<T>> slots, T value) {
        int invoked = 0;
        for (Slot<T> slot : slots) {
            if (!slot.active) continue;
            invoked++;
            try { slot.handler.accept(value); }
            catch (RuntimeException e) { synchronized (this) { failures++; } }
        }
        return invoked;
    }
    public synchronized long unhandledMessages() { return unhandled; }
    public synchronized long handlerFailures() { return failures; }
    @Override public synchronized void close() {
        closed = true;
        json.values().forEach(list -> list.forEach(s -> s.active = false));
        binary.forEach(s -> s.active = false);
        json.clear(); binary.clear();
    }
    private static final class Slot<T> {
        final Consumer<T> handler;
        volatile boolean active = true;
        Slot(Consumer<T> handler) { this.handler = handler; }
    }
}
