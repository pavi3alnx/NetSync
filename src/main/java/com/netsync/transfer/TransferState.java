package com.netsync.transfer;

/**
 * Lifecycle states of a NetSync file transfer.
 * <p>
 * State progression is owned and validated strictly on the Selector thread.
 */
public enum TransferState {
    /** Sender offered file via FILE_OFFER; waiting for recipient decision or timeout. */
    OFFERED,

    /** Recipient accepted transfer via FILE_ACCEPT; ready to receive chunks. */
    ACCEPTED,

    /** Chunks are actively being transferred and acknowledged via stop-and-wait flow control. */
    IN_PROGRESS,

    /** All chunks received and server-side SHA-256 matches FILE_COMPLETE; awaiting recipient confirmation. */
    SERVER_VERIFIED,

    /** Recipient verified and finalized file via FILE_RECEIVED; transfer successfully concluded. */
    COMPLETED,

    /** Recipient declined the transfer via FILE_REJECT. */
    REJECTED,

    /** Transfer explicitly cancelled by either sender or recipient via FILE_CANCEL. */
    CANCELLED,

    /** Transfer failed due to I/O error, checksum mismatch, timeout, saturation, or disconnection. */
    FAILED;

    /**
     * Returns true if this state represents a final, immutable transfer outcome.
     */
    public boolean isTerminal() {
        return this == COMPLETED || this == REJECTED || this == CANCELLED || this == FAILED;
    }

    /**
     * Returns true if the transfer is currently active and occupying concurrency slots.
     */
    public boolean isActive() {
        return !isTerminal();
    }

    /**
     * Validates whether a transition from this state to the target state is permissible.
     *
     * @param target the desired next state
     * @return true if the transition is valid
     */
    public boolean canTransitionTo(TransferState target) {
        if (target == null) return false;
        if (this == target) return true; // idempotent
        if (isTerminal()) return false;  // terminal states are immutable

        return switch (this) {
            case OFFERED -> target == ACCEPTED || target == REJECTED || target == CANCELLED || target == FAILED;
            case ACCEPTED -> target == IN_PROGRESS || target == SERVER_VERIFIED || target == CANCELLED || target == FAILED;
            case IN_PROGRESS -> target == IN_PROGRESS || target == SERVER_VERIFIED || target == CANCELLED || target == FAILED;
            case SERVER_VERIFIED -> target == COMPLETED || target == CANCELLED || target == FAILED;
            default -> false;
        };
    }
}
