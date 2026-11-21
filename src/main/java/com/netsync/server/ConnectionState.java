package com.netsync.server;

import com.netsync.common.ConnectionContext;
import com.netsync.common.SessionView;
import com.netsync.common.SessionRegistry;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayDeque;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.UUID;

/** Mutable transport state, owned exclusively by the selector thread. */
final class ConnectionState implements ConnectionContext {
    static final int RECENT_JSON_IDS = 1024;
    private final UUID id = UUID.randomUUID();
    private final SessionRegistry sessions;
    final SelectionKey key;
    final ByteBuffer prefix = ByteBuffer.allocate(4);
    final ArrayDeque<ByteBuffer> output = new ArrayDeque<>();
    final LinkedHashSet<UUID> recentJsonIds = new LinkedHashSet<>();
    final long acceptedAt;
    ByteBuffer payload;
    int queuedBytes;
    boolean closing;
    volatile boolean closed;
    long lastProgress;

    ConnectionState(SelectionKey key) { this(key, null); }
    ConnectionState(SelectionKey key, SessionRegistry sessions) {
        this(key, sessions, System.nanoTime());
    }
    ConnectionState(SelectionKey key, SessionRegistry sessions, long acceptedAt) {
        this.key = key;
        this.sessions = sessions;
        this.acceptedAt = acceptedAt;
        this.lastProgress = acceptedAt;
    }
    /** Selector-owned FIFO window; rejected duplicates do not refresh retention. */
    boolean acceptJsonId(UUID messageId) {
        if (recentJsonIds.contains(messageId)) return false;
        if (recentJsonIds.size() == RECENT_JSON_IDS) recentJsonIds.removeFirst();
        recentJsonIds.add(messageId);
        return true;
    }
    @Override public UUID connectionId() { return id; }
    @Override public Optional<SessionView> session() {
        if (closed || sessions == null) return Optional.empty();
        // Registry ownership stays with security; read current identity rather than caching it.
        return sessions.findByConnectionId(id).filter(session -> id.equals(session.connectionId()) && !closed);
    }
}
