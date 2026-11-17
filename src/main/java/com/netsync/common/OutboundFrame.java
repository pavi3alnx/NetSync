package com.netsync.common;

public sealed interface OutboundFrame permits MessageEnvelope, FileChunk {}
