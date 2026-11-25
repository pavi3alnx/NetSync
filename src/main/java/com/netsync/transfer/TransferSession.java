package com.netsync.transfer;

import com.netsync.common.ProtocolLimits;

import java.util.Objects;
import java.util.UUID;

/**
 * Encapsulates the runtime state of a single file transfer.
 * <p>
 * Mutable state is updated and queried strictly on the Selector event-loop thread.
 * Worker threads never mutate instances of this class directly.
 */
public final class TransferSession {
    private final UUID transferId;
    private final UUID senderClientId;
    private final UUID recipientClientId;
    private final String fileName;
    private final long declaredSizeBytes;
    private final UUID senderConnectionId;
    private final long offerEpochMs;

    private UUID recipientConnectionId;
    private TransferState state;
    private int expectedSequence;
    private long receivedBytes;
    private int chunkCount;
    private long lastActivityEpochMs;
    private String verifiedSha256;
    private boolean chunkWritePending;
    private final Object storageLock = new Object();

    public TransferSession(
            UUID transferId,
            UUID senderClientId,
            UUID recipientClientId,
            String fileName,
            long declaredSizeBytes,
            UUID senderConnectionId,
            long nowEpochMs
    ) {
        this.transferId = Objects.requireNonNull(transferId, "Transfer ID must not be null");
        this.senderClientId = Objects.requireNonNull(senderClientId, "Sender client ID must not be null");
        this.recipientClientId = Objects.requireNonNull(recipientClientId, "Recipient client ID must not be null");
        this.senderConnectionId = Objects.requireNonNull(senderConnectionId, "Sender connection ID must not be null");

        if (senderClientId.equals(recipientClientId)) {
            throw new IllegalArgumentException("Self-transfers are not permitted");
        }

        SafeFileStorage.validateSafeFileName(fileName);
        this.fileName = fileName;

        if (declaredSizeBytes < 0 || declaredSizeBytes > ProtocolLimits.MAX_FILE_BYTES) {
            throw new IllegalArgumentException("Declared file size out of bounds (0 - 10 MiB): " + declaredSizeBytes);
        }
        this.declaredSizeBytes = declaredSizeBytes;

        this.offerEpochMs = nowEpochMs;
        this.lastActivityEpochMs = nowEpochMs;
        this.state = TransferState.OFFERED;
        this.expectedSequence = 0;
        this.receivedBytes = 0L;
        this.chunkCount = 0;
    }

    public UUID transferId() { return transferId; }
    public UUID senderClientId() { return senderClientId; }
    public UUID recipientClientId() { return recipientClientId; }
    public String fileName() { return fileName; }
    public long declaredSizeBytes() { return declaredSizeBytes; }
    public UUID senderConnectionId() { return senderConnectionId; }
    public UUID recipientConnectionId() { return recipientConnectionId; }

    public void setRecipientConnectionId(UUID recipientConnectionId) {
        this.recipientConnectionId = Objects.requireNonNull(recipientConnectionId, "Recipient connection ID must not be null");
    }

    public TransferState state() { return state; }
    public int expectedSequence() { return expectedSequence; }
    public long receivedBytes() { return receivedBytes; }
    public int chunkCount() { return chunkCount; }
    public long offerEpochMs() { return offerEpochMs; }
    public long lastActivityEpochMs() { return lastActivityEpochMs; }
    public String verifiedSha256() { return verifiedSha256; }
    public boolean chunkWritePending() { return chunkWritePending; }

    /** Selector-owned stop-and-wait reservation for the single outstanding chunk. */
    public void setChunkWritePending(boolean pending) {
        if (pending && chunkWritePending) throw new IllegalStateException("A chunk write is already pending");
        chunkWritePending = pending;
    }
    Object storageLock() { return storageLock; }

    public void setVerifiedSha256(String sha256) {
        if (sha256 != null && !sha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Invalid SHA-256 hex string: " + sha256);
        }
        this.verifiedSha256 = sha256;
    }

    /**
     * Executes a validated state transition.
     *
     * @param nextState target state
     * @param nowEpochMs current epoch millisecond timestamp
     * @throws IllegalStateException if transition is not permissible
     */
    public void transitionTo(TransferState nextState, long nowEpochMs) {
        Objects.requireNonNull(nextState, "Next state must not be null");
        if (!state.canTransitionTo(nextState)) {
            throw new IllegalStateException("Invalid transfer state transition from " + state + " to " + nextState);
        }
        this.state = nextState;
        this.lastActivityEpochMs = nowEpochMs;
    }

    /**
     * Validates and records incoming binary chunk metrics.
     *
     * @param sequence sequence number from chunk header
     * @param chunkBytes byte length of the chunk
     * @param nowEpochMs current epoch millisecond timestamp
     * @throws IllegalStateException if transfer state or sequence is invalid, or if bytes exceed declared size
     */
    public void validateAndRecordChunk(int sequence, int chunkBytes, long nowEpochMs) {
        if (state.isTerminal()) {
            throw new IllegalStateException("Cannot receive chunk in terminal state: " + state);
        }
        if (state == TransferState.OFFERED) {
            throw new IllegalStateException("Cannot receive chunk for unaccepted offer");
        }
        if (sequence != expectedSequence) {
            throw new IllegalStateException("Chunk sequence mismatch: expected " + expectedSequence + ", got " + sequence);
        }
        if (chunkBytes < 1 || chunkBytes > ProtocolLimits.MAX_CHUNK_BYTES) {
            throw new IllegalArgumentException("Chunk size out of bounds (1 - 16 KiB): " + chunkBytes);
        }
        if (receivedBytes + chunkBytes > declaredSizeBytes) {
            throw new IllegalStateException("Received bytes (" + (receivedBytes + chunkBytes)
                    + ") exceeds declared size (" + declaredSizeBytes + ")");
        }
        boolean isFinalChunk = (receivedBytes + chunkBytes == declaredSizeBytes);
        if (!isFinalChunk && chunkBytes != ProtocolLimits.MAX_CHUNK_BYTES) {
            throw new IllegalStateException("Non-final chunk must be exactly "
                    + ProtocolLimits.MAX_CHUNK_BYTES + " bytes, got: " + chunkBytes);
        }

        if (state == TransferState.ACCEPTED) {
            transitionTo(TransferState.IN_PROGRESS, nowEpochMs);
        }

        this.expectedSequence++;
        this.receivedBytes += chunkBytes;
        this.chunkCount++;
        this.lastActivityEpochMs = nowEpochMs;
    }

    public boolean isOfferExpired(long nowEpochMs, long timeoutMillis) {
        return state == TransferState.OFFERED && (nowEpochMs - offerEpochMs >= timeoutMillis);
    }

    public boolean isProgressExpired(long nowEpochMs, long timeoutMillis) {
        return state.isActive() && state != TransferState.OFFERED && (nowEpochMs - lastActivityEpochMs >= timeoutMillis);
    }

    public boolean isExpired(long nowEpochMs, long timeoutMillis) {
        return isOfferExpired(nowEpochMs, timeoutMillis) || isProgressExpired(nowEpochMs, timeoutMillis);
    }

    public void touch(long nowEpochMs) {
        this.lastActivityEpochMs = nowEpochMs;
    }
}
