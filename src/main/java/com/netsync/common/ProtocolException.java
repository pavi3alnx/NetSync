package com.netsync.common;

import java.util.Optional;
import java.util.UUID;
/** Safe metadata for transport error handling; never retains raw input or parser causes. */
public final class ProtocolException extends Exception {
    public enum Action { KEEP_OPEN, ERROR_THEN_CLOSE, CLOSE }
    private final ErrorCode code;
    private final UUID replyTo;
    private final Action action;
    public ProtocolException(ErrorCode code, UUID replyTo, Action action) {
        super(action==Action.CLOSE ? "Invalid frame" : code.name());
        this.code=code; this.replyTo=replyTo; this.action=action;
    }
    /** Empty for framing errors that require closure without a protocol response. */
    public Optional<ErrorCode> code() { return Optional.ofNullable(code); }
    public Optional<UUID> replyTo() { return Optional.ofNullable(replyTo); }
    public Action action() { return action; }
}
