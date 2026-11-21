package com.netsync.security;

import com.netsync.common.AuthorizationService;
import com.netsync.common.ConnectionContext;
import com.netsync.common.ConnectionCloser;
import com.netsync.common.EnqueueResult;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageHandler;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundSink;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionView;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

/** Revokes the authenticated session after accepting the protocol logout acknowledgement. */
public final class LogoutHandler implements MessageHandler {
    private final AuthService authService;
    private final AuthorizationService authorizationService;
    private final OutboundSink outboundSink;
    private final ConnectionCloser connectionCloser;
    private final Consumer<SessionView> onRevoked;

    public LogoutHandler(
            AuthService authService,
            AuthorizationService authorizationService,
            OutboundSink outboundSink,
            ConnectionCloser connectionCloser) {
        this(authService, authorizationService, outboundSink, connectionCloser, session -> {});
    }

    public LogoutHandler(
            AuthService authService,
            AuthorizationService authorizationService,
            OutboundSink outboundSink,
            ConnectionCloser connectionCloser,
            Consumer<SessionView> onRevoked) {
        this.authService = Objects.requireNonNull(authService);
        this.authorizationService = Objects.requireNonNull(authorizationService);
        this.outboundSink = Objects.requireNonNull(outboundSink);
        this.connectionCloser = Objects.requireNonNull(connectionCloser);
        this.onRevoked = Objects.requireNonNull(onRevoked);
    }

    @Override
    public void handle(ConnectionContext context, MessageEnvelope message) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(message);
        UUID connectionId = context.connectionId();

        if (message.type() != MessageType.LOGOUT_REQUEST
                || !(message.body() instanceof MessageBody.Empty)) {
            sendError(connectionId, message.messageId(), ErrorCode.INVALID_REQUEST, "Invalid logout request");
            return;
        }

        Optional<SessionView> session = authorizationService.authenticatedSession(context);
        if (session.isEmpty()) {
            sendError(connectionId, message.messageId(), ErrorCode.UNAUTHENTICATED, "Authentication required");
            return;
        }

        MessageEnvelope acknowledgement = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ACK,
                UUID.randomUUID(),
                message.messageId(),
                new MessageBody.Ack(MessageType.LOGOUT_REQUEST, com.netsync.common.AckStatus.ACCEPTED, null));
        EnqueueResult queued;
        try {
            queued = outboundSink.send(connectionId, acknowledgement);
        } finally {
            if (authService.logout(connectionId)) {
                onRevoked.accept(session.get());
            }
        }
        if (queued == EnqueueResult.REJECTED) {
            authService.disconnect(connectionId);
        } else {
            connectionCloser.closeAfterDrain(connectionId);
        }
    }

    private void sendError(UUID connectionId, UUID requestId, ErrorCode code, String safeMessage) {
        if (connectionId == null) {
            return;
        }
        outboundSink.send(connectionId, new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ERROR,
                UUID.randomUUID(),
                requestId,
                new MessageBody.Error(code, safeMessage, null)));
    }
}
