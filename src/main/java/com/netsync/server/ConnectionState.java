package com.netsync.server;

import com.netsync.common.ConnectionContext;
import com.netsync.common.SessionView;
import com.netsync.common.SessionRegistry;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.UUID;

/** Mutable transport state, owned exclusively by the selector thread. */
final class ConnectionState implements ConnectionContext {
    private final UUID id = UUID.randomUUID();
    private final SessionRegistry sessions;
    final SelectionKey key;
    final ByteBuffer prefix = ByteBuffer.allocate(4);
    final ArrayDeque<ByteBuffer> output = new ArrayDeque<>();
    ByteBuffer payload;
    int queuedBytes;
    boolean closing;
    volatile boolean closed;
    long lastProgress = System.nanoTime();

    ConnectionState(SelectionKey key) { this(key, null); }
    ConnectionState(SelectionKey key, SessionRegistry sessions) {
        this.key = key;
        this.sessions = sessions;
    }
    @Override public UUID connectionId() { return id; }
    @Override public Optional<SessionView> session() {
        if (closed || sessions == null) return Optional.empty();
        // Registry ownership stays with security; read current identity rather than caching it.
        return sessions.findByConnectionId(id).filter(session -> id.equals(session.connectionId()) && !closed);
    }
}
