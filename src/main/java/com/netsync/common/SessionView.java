package com.netsync.common;

import java.util.Objects;
import java.util.UUID;
/** Immutable identity snapshot, not a permanent authorization grant. */
public record SessionView(UUID clientId, String username, UUID connectionId) {
    public SessionView { Objects.requireNonNull(clientId); Objects.requireNonNull(username); Objects.requireNonNull(connectionId); }
}
