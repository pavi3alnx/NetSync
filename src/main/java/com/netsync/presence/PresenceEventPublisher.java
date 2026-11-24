package com.netsync.presence;

import com.netsync.common.*;
import java.util.*;

/** Bounded latest-state mailbox. Only its Selector drain calls the outbound sink. */
public final class PresenceEventPublisher implements PresenceListener, AutoCloseable {
    private final SelectorTasks tasks;
    private final SessionRegistry sessions;
    private final OutboundSink outbound;
    private final int capacity;
    private final Map<UUID, Entry> entries = new LinkedHashMap<>();
    private boolean scheduled, draining, closed;

    public PresenceEventPublisher(SelectorTasks tasks, SessionRegistry sessions, OutboundSink outbound) {
        this(tasks, sessions, outbound, 2 * ProtocolLimits.MAX_CONNECTIONS);
    }
    PresenceEventPublisher(SelectorTasks tasks, SessionRegistry sessions, OutboundSink outbound, int capacity) {
        this.tasks = Objects.requireNonNull(tasks); this.sessions = Objects.requireNonNull(sessions);
        this.outbound = Objects.requireNonNull(outbound);
        if (capacity < 1) throw new IllegalArgumentException("Invalid presence capacity");
        this.capacity = capacity;
    }

    /** Reserve identity before tracker registration. Saturation rejects new admission, never grows memory. */
    public synchronized boolean registerSession(SessionView session) {
        Objects.requireNonNull(session);
        if (closed) return false;
        Entry existing = entries.get(session.clientId());
        if (existing != null) return !existing.revoked && existing.session.equals(session);
        if (entries.size() == capacity) return false;
        entries.put(session.clientId(), new Entry(session));
        return true;
    }

    @Override public void onPresenceChanged(UUID clientId, PresenceState state) {
        Objects.requireNonNull(clientId); Objects.requireNonNull(state);
        SessionView active = sessions.findByClientId(clientId).orElse(null);
        synchronized (this) {
            if (closed) return;
            Entry entry = entries.get(clientId);
            if (entry == null) {
                if (active == null || !registerSession(active)) return;
                entry = entries.get(clientId);
            }
            if (state == PresenceState.ONLINE && (active == null || entry.revoked)) return;
            entry.desired = state;
        }
        retryPending();
    }

    /** Call after tracker revocation. Retains a pending final OFFLINE until drained. */
    public void forgetSession(UUID clientId) {
        synchronized (this) {
            Entry entry = entries.get(Objects.requireNonNull(clientId));
            if (entry == null) return;
            entry.revoked = true;
            // A queued ONLINE that has not reached the selector can be cancelled.
            // If the drain is already in a send callback, let it finish first.
            if (entry.published == null && draining) {
                // The current selector drain may already have captured ONLINE
                // and will emit it before its queued disconnect callback runs.
                entry.desired = PresenceState.OFFLINE;
            } else if (entry.published == null || entry.published == PresenceState.OFFLINE) {
                entries.remove(clientId);
                // A drain may already be queued but has not linearized this event.
                // It will see the missing entry and discard the stale transition.
            } else entry.desired = PresenceState.OFFLINE;
        }
        retryPending();
    }

    /** Also called by the existing one-second UDP expiry loop; rejected work stays retained. */
    public void retryPending() {
        synchronized (this) {
            if (closed || scheduled || entries.values().stream().noneMatch(Entry::dirty)) return;
            scheduled = true;
        }
        if (!tasks.submit(this::drain)) synchronized (this) { scheduled = false; }
    }

    private void drain() {
        synchronized (this) { if (closed) { scheduled = false; return; } draining = true; }
        try {
            List<UUID> ids;
            synchronized (this) { if (closed) return; ids = List.copyOf(entries.keySet()); }
            for (UUID id : ids) {
                boolean active = sessions.findByClientId(id).isPresent();
                MessageBody.PresenceUpdate update;
                synchronized (this) {
                    if (closed) return;
                    Entry entry = entries.get(id);
                    if (entry == null) continue;
                    if (!active && entry.desired != null) { entry.revoked = true; entry.desired = PresenceState.OFFLINE; }
                    if (!entry.dirty()) continue;
                    // Linearize here. New changes get a later drain and cannot overtake
                    // this event. No lock across send: disconnect can reenter the tracker.
                    entry.published = entry.desired;
                    update = new MessageBody.PresenceUpdate(id, entry.session.username(), entry.published);
                }
                MessageEnvelope event = new MessageEnvelope(ProtocolLimits.VERSION, MessageType.PRESENCE_UPDATE,
                        UUID.randomUUID(), null, update);
                for (SessionView recipient : sessions.activeSessions()) outbound.send(recipient.connectionId(), event);
                synchronized (this) {
                    Entry entry = entries.get(id);
                    if (entry != null && entry.revoked && !entry.dirty()) entries.remove(id);
                }
            }
        } finally {
            synchronized (this) { scheduled = false; draining = false; }
            retryPending();
        }
    }

    @Override public synchronized void close() { closed = true; entries.clear(); }
    synchronized int retainedSessions() { return entries.size(); }
    private static final class Entry {
        final SessionView session;
        PresenceState desired, published;
        boolean revoked;
        Entry(SessionView session) { this.session = session; }
        boolean dirty() { return desired != null && desired != published; }
    }
}
