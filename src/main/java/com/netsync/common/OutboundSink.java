package com.netsync.common;

import java.util.UUID;
/** Selector-thread only. ACCEPTED means queued, not delivered. Owner enforces queue limits and cleanup. */
@FunctionalInterface
public interface OutboundSink { EnqueueResult send(UUID connectionId, OutboundFrame frame); }
