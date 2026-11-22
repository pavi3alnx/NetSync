package com.netsync.chat;

import com.netsync.common.AckStatus;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundSink;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionView;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Coordinates chat validation, routing through MessageRouter, and generation
 * of correlated ACK and ERROR frames to the sender.
 *
 * <p>All operations are non-blocking and execute synchronously on the Selector thread.
 */
public final class ChatService {

    private final MessageRouter messageRouter;
    private final OutboundSink outboundSink;

    public ChatService(MessageRouter messageRouter, OutboundSink outboundSink) {
        this.messageRouter = Objects.requireNonNull(messageRouter, "messageRouter cannot be null");
        this.outboundSink = Objects.requireNonNull(outboundSink, "outboundSink cannot be null");
    }

    /**
     * Validates and processes a CHAT_SEND request from an authenticated sender.
     *
     * @param senderSession the verified sender session snapshot
     * @param requestId the correlation ID from the client's CHAT_SEND request
     * @param chatSend the request payload
     */
    public void processChatSend(SessionView senderSession, UUID requestId, MessageBody.ChatSend chatSend) {
        Objects.requireNonNull(senderSession, "senderSession cannot be null");
        Objects.requireNonNull(requestId, "requestId cannot be null");
        Objects.requireNonNull(chatSend, "chatSend cannot be null");

        // Validate recipient client ID
        if (chatSend.recipientClientId() == null) {
            sendError(senderSession.connectionId(), requestId, ErrorCode.INVALID_REQUEST, "Recipient ID must not be null");
            return;
        }

        // Validate message text
        String text = chatSend.text();
        if (text == null || text.isEmpty()) {
            sendError(senderSession.connectionId(), requestId, ErrorCode.INVALID_REQUEST, "Message text cannot be empty");
            return;
        }

        byte[] textBytes = text.getBytes(StandardCharsets.UTF_8);
        if (textBytes.length > ProtocolLimits.MAX_CHAT_BYTES) {
            sendError(senderSession.connectionId(), requestId, ErrorCode.SIZE_LIMIT,
                    "Message text exceeds maximum length of " + ProtocolLimits.MAX_CHAT_BYTES + " bytes");
            return;
        }

        // Route direct message to recipient
        MessageRouter.RoutingStatus status = messageRouter.routeDirectMessage(
                senderSession.clientId(),
                chatSend.recipientClientId(),
                text,
                requestId
        );

        switch (status) {
            case DELIVERED -> sendAck(senderSession.connectionId(), requestId);
            case RECIPIENT_UNAVAILABLE -> sendError(
                    senderSession.connectionId(),
                    requestId,
                    ErrorCode.RECIPIENT_UNAVAILABLE,
                    "Recipient is not connected");
            case QUEUE_FULL -> sendError(
                    senderSession.connectionId(),
                    requestId,
                    ErrorCode.BUSY,
                    "Recipient output queue is full");
        }
    }

    private void sendAck(UUID connectionId, UUID requestId) {
        MessageEnvelope ack = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ACK,
                UUID.randomUUID(),
                requestId,
                new MessageBody.Ack(MessageType.CHAT_SEND, AckStatus.QUEUED, null)
        );
        outboundSink.send(connectionId, ack);
    }

    private void sendError(UUID connectionId, UUID requestId, ErrorCode errorCode, String message) {
        MessageEnvelope error = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ERROR,
                UUID.randomUUID(),
                requestId,
                new MessageBody.Error(errorCode, message, null)
        );
        outboundSink.send(connectionId, error);
    }
}
