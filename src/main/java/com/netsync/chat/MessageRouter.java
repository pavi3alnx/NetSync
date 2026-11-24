package com.netsync.chat;

import com.netsync.common.EnqueueResult;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundSink;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Resolves recipient connection identity using SessionRegistry and delivers
 * length-prefixed CHAT_DELIVER frames to the target client's output queue.
 *
 * <p>All operations are non-blocking and execute synchronously on the Selector thread.
 */
public final class MessageRouter {

    /**
     * Outcome of attempting to route a direct message.
     */
    public enum RoutingStatus {
        /** Enqueued to the recipient's output queue successfully. */
        DELIVERED,
        /** Recipient is not registered or not active in the session registry. */
        RECIPIENT_UNAVAILABLE,
        /** Recipient's output queue is saturated; message was rejected to protect memory. */
        QUEUE_FULL
    }

    private final SessionRegistry sessionRegistry;
    private final OutboundSink outboundSink;

    public MessageRouter(SessionRegistry sessionRegistry, OutboundSink outboundSink) {
        this.sessionRegistry = Objects.requireNonNull(sessionRegistry, "sessionRegistry cannot be null");
        this.outboundSink = Objects.requireNonNull(outboundSink, "outboundSink cannot be null");
    }

    /**
     * Routes a direct chat message to the designated recipient.
     *
     * @param senderClientId the verified client ID of the sender from server session
     * @param recipientClientId the client ID of the intended recipient
     * @param text the validated chat message text
     * @param sourceMessageId the message ID of the original CHAT_SEND request
     * @return the routing outcome
     */
    public RoutingStatus routeDirectMessage(UUID senderClientId, UUID recipientClientId, String text, UUID sourceMessageId) {
        Objects.requireNonNull(senderClientId, "senderClientId cannot be null");
        Objects.requireNonNull(recipientClientId, "recipientClientId cannot be null");
        Objects.requireNonNull(text, "text cannot be null");
        Objects.requireNonNull(sourceMessageId, "sourceMessageId cannot be null");

        Optional<SessionView> recipientSession = sessionRegistry.findByClientId(recipientClientId);
        if (recipientSession.isEmpty()) {
            return RoutingStatus.RECIPIENT_UNAVAILABLE;
        }

        MessageEnvelope deliverEnvelope = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.CHAT_DELIVER,
                UUID.randomUUID(),
                null,
                new MessageBody.ChatDeliver(senderClientId, text, sourceMessageId)
        );

        EnqueueResult result = outboundSink.send(recipientSession.get().connectionId(), deliverEnvelope);
        if (result == EnqueueResult.ACCEPTED) {
            return RoutingStatus.DELIVERED;
        } else {
            return RoutingStatus.QUEUE_FULL;
        }
    }
}
