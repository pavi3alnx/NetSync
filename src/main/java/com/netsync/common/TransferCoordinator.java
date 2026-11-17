package com.netsync.common;

import java.util.UUID;
/** Selector-thread contract. Implementation owns participant/state checks and asynchronous file work. */
public interface TransferCoordinator {
    void handleControl(ConnectionContext context, MessageEnvelope message);
    void handleChunk(ConnectionContext context, FileChunk chunk);
    void onDisconnected(UUID connectionId);
}
