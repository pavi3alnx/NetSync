package com.netsync.server;

import com.netsync.common.ConnectionContext;
import com.netsync.common.SessionView;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.UUID;

/** Mutable transport state, owned exclusively by the selector thread. */
final class ConnectionState implements ConnectionContext {
    private final UUID id = UUID.randomUUID();
    final SelectionKey key;
    final ByteBuffer prefix = ByteBuffer.allocate(4);
    final ArrayDeque<ByteBuffer> output = new ArrayDeque<>();
    ByteBuffer payload;
    int queuedBytes;
    boolean closing;
    long lastProgress = System.nanoTime();

    ConnectionState(SelectionKey key) { this.key = key; }
    @Override public UUID connectionId() { return id; }
    // Authentication/session wiring belongs to the security integration milestone.
    @Override public Optional<SessionView> session() { return Optional.empty(); }
}
