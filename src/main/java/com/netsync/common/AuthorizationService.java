package com.netsync.common;

import java.util.Optional;
/** Security implementation rechecks active registry state, never trusts body identities. */
public interface AuthorizationService {
    Optional<SessionView> authenticatedSession(ConnectionContext context);
    boolean isActive(SessionView session);
}
