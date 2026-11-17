package com.netsync.common;

import java.util.Objects;
import java.util.UUID;
/** Null replyTo means the optional wire field is absent. Codec validates version and body pairing. */
public record MessageEnvelope(int version, MessageType type, UUID messageId, UUID replyTo, MessageBody body)
        implements OutboundFrame {
    public MessageEnvelope { Objects.requireNonNull(type); Objects.requireNonNull(messageId); Objects.requireNonNull(body); }
}
