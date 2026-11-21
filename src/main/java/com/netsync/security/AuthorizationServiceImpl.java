package com.netsync.security;

import com.netsync.common.AuthorizationService;
import com.netsync.common.ConnectionContext;
import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Revalidates sender and recipient identities against the live session registry. */
public final class AuthorizationServiceImpl implements AuthorizationService {
    private final SessionRegistry sessionRegistry;

    public AuthorizationServiceImpl(SessionRegistry sessionRegistry) {
        this.sessionRegistry = Objects.requireNonNull(sessionRegistry);
    }

    @Override
    public Optional<SessionView> authenticatedSession(ConnectionContext context) {
        if (context == null || context.connectionId() == null) {
            return Optional.empty();
        }
        Optional<SessionView> connectionSession = sessionRegistry.findByConnectionId(context.connectionId());
        if (connectionSession.isEmpty()) {
            return Optional.empty();
        }

        SessionView registered = connectionSession.get();
        Optional<SessionView> clientSession = sessionRegistry.findByClientId(registered.clientId());
        if (clientSession.isEmpty() || !registered.equals(clientSession.get())) {
            return Optional.empty();
        }
        Optional<SessionView> contextSession = context.session();
        return contextSession.filter(registered::equals);
    }

    @Override
    public boolean isActive(SessionView session) {
        if (session == null) {
            return false;
        }
        Optional<SessionView> connectionSession = sessionRegistry.findByConnectionId(session.connectionId());
        Optional<SessionView> clientSession = sessionRegistry.findByClientId(session.clientId());
        return connectionSession.filter(session::equals).isPresent()
                && clientSession.filter(session::equals).isPresent();
    }

    /** Returns the authenticated sender identity only when the recipient is also active. */
    public Optional<SessionView> authorizedChatSender(ConnectionContext context, UUID recipientClientId) {
        Optional<SessionView> sender = authenticatedSession(context);
        if (sender.isEmpty() || recipientClientId == null
                || sessionRegistry.findByClientId(recipientClientId).isEmpty()) {
            return Optional.empty();
        }
        return sender.filter(this::isActive);
    }

    /** Returns whether both transfer participants are currently active sessions. */
    public boolean canTransfer(ConnectionContext context, UUID recipientClientId) {
        return authorizedChatSender(context, recipientClientId).isPresent();
    }
}
