package com.netsync.server;

import com.netsync.common.ConnectionContext;
import com.netsync.common.EnqueueResult;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageHandler;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundSink;
import com.netsync.common.ProtocolLimits;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Routes already validated JSON envelopes on the selector thread; never decodes bytes.
 * The handler table is an immutable startup snapshot. Handlers must not block.
 * Handler exceptions propagate to NioServer's existing per-connection error boundary.
 */
public final class MessageDispatcher implements MessageHandler {
    private static final System.Logger LOG = System.getLogger(MessageDispatcher.class.getName());
    private final Map<MessageType, MessageHandler> handlers;
    private final OutboundSink outbound;

    private MessageDispatcher(Map<MessageType, MessageHandler> handlers, OutboundSink outbound) {
        this.handlers = Map.copyOf(handlers);
        this.outbound = Objects.requireNonNull(outbound);
    }

    public static Builder builder() { return new Builder(); }

    @Override public void handle(ConnectionContext context, MessageEnvelope message) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(message);
        MessageHandler handler = handlers.get(message.type());
        if (handler != null) {
            handler.handle(context, message);
            return;
        }
        // A recognized operation without an implementation is not an unknown wire type.
        MessageEnvelope error = new MessageEnvelope(ProtocolLimits.VERSION, MessageType.ERROR,
                UUID.randomUUID(), message.messageId(),
                new MessageBody.Error(ErrorCode.INVALID_REQUEST, "Operation is not supported", null));
        // Best effort: transport owns slow-consumer closure; rejection never means delivery.
        if (outbound.send(context.connectionId(), error) == EnqueueResult.REJECTED) {
            LOG.log(System.Logger.Level.DEBUG, "Dispatch error response was not queued");
        }
    }

    /** Configure on one startup thread. Later builder changes cannot alter a built dispatcher. */
    public static final class Builder {
        private final EnumMap<MessageType, MessageHandler> handlers = new EnumMap<>(MessageType.class);

        private Builder() {}

        public Builder register(MessageType type, MessageHandler handler) {
            Objects.requireNonNull(type);
            Objects.requireNonNull(handler);
            if (type == MessageType.FILE_CHUNK) {
                throw new IllegalArgumentException("Binary chunks use the separate chunk callback");
            }
            if (handlers.putIfAbsent(type, handler) != null) {
                throw new IllegalArgumentException("Handler already registered for " + type);
            }
            return this;
        }

        /** The supplied sink must be used only on its owning selector thread. */
        public MessageDispatcher build(OutboundSink outbound) {
            return new MessageDispatcher(handlers, outbound);
        }
    }
}
