package com.netsync.common;

import java.util.Optional;
import java.util.UUID;
/** Server-owned read-only connection identity; exposes no transport resources. */
public interface ConnectionContext {
    UUID connectionId();
    Optional<SessionView> session();
}
