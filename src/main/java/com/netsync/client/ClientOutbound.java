package com.netsync.client;

import com.netsync.common.*;
import java.util.concurrent.RejectedExecutionException;

/** Enqueue acceptance is not delivery. Concurrent calls are ordered by enqueue acquisition. */
public interface ClientOutbound {
    EnqueueResult send(OutboundFrame frame) throws ProtocolException;
    boolean isConnected();
    default EnqueueResult sendJson(MessageEnvelope message) throws ProtocolException { return send(message); }
    default EnqueueResult sendChunk(FileChunk chunk) throws ProtocolException { return send(chunk); }
    /** Adapter for existing transfer module Consumer<OutboundFrame> constructors. */
    default void sendOrThrow(OutboundFrame frame) {
        try {
            if (send(frame) != EnqueueResult.ACCEPTED) throw new RejectedExecutionException("Client send rejected");
        } catch (ProtocolException e) { throw new IllegalArgumentException("Invalid outbound frame"); }
    }
}
