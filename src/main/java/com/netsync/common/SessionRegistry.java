package com.netsync.common;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
/** Security-owned thread-safe lookups returning immutable snapshots; writes stay security-owned. */
public interface SessionRegistry {
    Optional<SessionView> findByConnectionId(UUID id);
    Optional<SessionView> findByClientId(UUID id);
    List<SessionView> activeSessions();
}
