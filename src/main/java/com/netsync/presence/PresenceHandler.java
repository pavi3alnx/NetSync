package com.netsync.presence;

import com.netsync.common.AuthorizationService;
import com.netsync.common.ConnectionContext;
import com.netsync.common.EnqueueResult;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageHandler;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundSink;
import com.netsync.common.PresenceState;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public final class PresenceHandler implements MessageHandler {
    private final AuthorizationService authorizationService;
    private final SessionRegistry sessionRegistry;
    private final HeartbeatTracker heartbeatTracker;
    private final OutboundSink outboundSink;

    public PresenceHandler(
            AuthorizationService authorizationService,
            SessionRegistry sessionRegistry,
            HeartbeatTracker heartbeatTracker,
            OutboundSink outboundSink) {
        this.authorizationService = Objects.requireNonNull(authorizationService);
        this.sessionRegistry = Objects.requireNonNull(sessionRegistry);
        this.heartbeatTracker = Objects.requireNonNull(heartbeatTracker);
        this.outboundSink = Objects.requireNonNull(outboundSink);
    }

    @Override
    public void handle(ConnectionContext context, MessageEnvelope message) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(message);

        if (message.type() != MessageType.PRESENCE_QUERY) {
            enqueueError(
                    context.connectionId(),
                    message.messageId(),
                    ErrorCode.INVALID_REQUEST,
                    "Unexpected presence request");
            return;
        }

        if (!(message.body() instanceof MessageBody.Empty)) {
            enqueueError(
                    context.connectionId(),
                    message.messageId(),
                    ErrorCode.INVALID_REQUEST,
                    "Invalid presence query");
            return;
        }

        if (!authorizationService.authenticatedSession(context).filter(
                        session -> authorizationService.isActive(session)).isPresent()) {
            enqueueError(
                    context.connectionId(),
                    message.messageId(),
                    ErrorCode.UNAUTHENTICATED,
                    "Authentication required");
            return;
        }

        List<MessageBody.PresenceEntry> entries = new ArrayList<>();
        for (SessionView session : sessionRegistry.activeSessions()) {
            PresenceState state = heartbeatTracker
                    .getPresenceState(session.clientId())
                    .orElse(PresenceState.OFFLINE);
            entries.add(new MessageBody.PresenceEntry(
                    session.clientId(),
                    session.username(),
                    state));
        }

        MessageEnvelope response = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.PRESENCE_SNAPSHOT,
                UUID.randomUUID(),
                message.messageId(),
                new MessageBody.PresenceSnapshot(List.copyOf(entries)));
        outboundSink.send(context.connectionId(), response);
    }

    private void enqueueError(
            UUID connectionId,
            UUID requestId,
            ErrorCode code,
            String safeMessage) {
        MessageEnvelope response = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ERROR,
                UUID.randomUUID(),
                requestId,
                new MessageBody.Error(code, safeMessage, null));
        outboundSink.send(connectionId, response);
    }
}
