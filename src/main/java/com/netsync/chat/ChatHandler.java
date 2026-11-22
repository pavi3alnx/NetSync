package com.netsync.chat;

import com.netsync.common.AuthorizationService;
import com.netsync.common.ConnectionContext;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageHandler;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundSink;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * MessageHandler implementation for MessageType.CHAT_SEND.
 * Invoked by the NIO selector loop upon receiving a length-prefixed chat frame.
 *
 * <p>Enforces server-side authentication, rejects invalid message types or unauthenticated
 * requests, and delegates valid chat payloads to ChatService.
 */
public final class ChatHandler implements MessageHandler {

    private final ChatService chatService;
    private final AuthorizationService authorizationService;
    private final OutboundSink outboundSink;

    public ChatHandler(
            ChatService chatService,
            AuthorizationService authorizationService,
            OutboundSink outboundSink) {
        this.chatService = Objects.requireNonNull(chatService, "chatService cannot be null");
        this.authorizationService = Objects.requireNonNull(authorizationService, "authorizationService cannot be null");
        this.outboundSink = Objects.requireNonNull(outboundSink, "outboundSink cannot be null");
    }

    /**
     * Convenience constructor assembling MessageRouter and ChatService.
     */
    public ChatHandler(
            SessionRegistry sessionRegistry,
            AuthorizationService authorizationService,
            OutboundSink outboundSink) {
        this(new ChatService(new MessageRouter(sessionRegistry, outboundSink), outboundSink),
                authorizationService,
                outboundSink);
    }

    @Override
    public void handle(ConnectionContext context, MessageEnvelope message) {
        Objects.requireNonNull(context, "context cannot be null");
        Objects.requireNonNull(message, "message cannot be null");

        if (message.type() != MessageType.CHAT_SEND) {
            sendError(context.connectionId(), message.messageId(), ErrorCode.INVALID_REQUEST, "Unexpected message type");
            return;
        }

        if (!(message.body() instanceof MessageBody.ChatSend chatSend)) {
            sendError(context.connectionId(), message.messageId(), ErrorCode.INVALID_REQUEST, "Invalid chat request body");
            return;
        }

        Optional<SessionView> senderSession = authorizationService.authenticatedSession(context)
                .filter(authorizationService::isActive);
        if (senderSession.isEmpty()) {
            sendError(context.connectionId(), message.messageId(), ErrorCode.UNAUTHENTICATED, "Authentication required");
            return;
        }

        chatService.processChatSend(senderSession.get(), message.messageId(), chatSend);
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
