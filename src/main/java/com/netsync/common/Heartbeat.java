package com.netsync.common;

import java.util.Objects;
import java.util.UUID;
/** Separate UDP schema. Credential validity and sequence progression are checked by owners. */
public record Heartbeat(int version, String type, UUID clientId, String presenceToken, long sequence) {
    public Heartbeat { Objects.requireNonNull(type); Objects.requireNonNull(clientId); Objects.requireNonNull(presenceToken); }
    @Override public String toString() { return "Heartbeat[presenceToken=REDACTED]"; }
}
