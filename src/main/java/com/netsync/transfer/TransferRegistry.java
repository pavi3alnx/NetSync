package com.netsync.transfer;

import com.netsync.common.ProtocolLimits;

import java.util.*;

/**
 * Registry managing active file transfers and participant concurrency slots.
 * <p>
 * Enforces global limit (max 10 active transfers) and per-client limit (max 1 active
 * transfer per client as either sender or recipient).
 * <p>
 * This registry is owned and mutated strictly on the Selector event-loop thread.
 */
public final class TransferRegistry {
    private final int maxGlobalTransfers;
    private final int maxClientTransfers;

    private final Map<UUID, TransferSession> activeTransfers = new HashMap<>();
    private final Map<UUID, UUID> clientToTransfer = new HashMap<>();

    public TransferRegistry() {
        this(ProtocolLimits.MAX_TRANSFERS, ProtocolLimits.MAX_CLIENT_TRANSFERS);
    }

    public TransferRegistry(int maxGlobalTransfers, int maxClientTransfers) {
        if (maxGlobalTransfers <= 0 || maxClientTransfers <= 0) {
            throw new IllegalArgumentException("Transfer limits must be positive");
        }
        this.maxGlobalTransfers = maxGlobalTransfers;
        this.maxClientTransfers = maxClientTransfers;
    }

    /**
     * Registers a new transfer offer, atomically claiming global and per-client transfer slots.
     *
     * @param session the transfer session to register
     * @throws IllegalStateException    if global limit or per-client limit is exceeded
     * @throws IllegalArgumentException if transfer ID is already registered
     */
    public void registerOffer(TransferSession session) {
        Objects.requireNonNull(session, "Transfer session must not be null");

        if (activeTransfers.size() >= maxGlobalTransfers) {
            throw new IllegalStateException("Global active transfer limit (" + maxGlobalTransfers + ") reached");
        }
        if (activeTransfers.containsKey(session.transferId())) {
            throw new IllegalArgumentException("Transfer ID already exists: " + session.transferId());
        }
        if (clientToTransfer.containsKey(session.senderClientId())) {
            throw new IllegalStateException("Sender client (" + session.senderClientId()
                    + ") already has an active transfer");
        }
        if (clientToTransfer.containsKey(session.recipientClientId())) {
            throw new IllegalStateException("Recipient client (" + session.recipientClientId()
                    + ") already has an active transfer");
        }

        activeTransfers.put(session.transferId(), session);
        clientToTransfer.put(session.senderClientId(), session.transferId());
        clientToTransfer.put(session.recipientClientId(), session.transferId());
    }

    public Optional<TransferSession> get(UUID transferId) {
        if (transferId == null) return Optional.empty();
        return Optional.ofNullable(activeTransfers.get(transferId));
    }

    public Optional<TransferSession> findByClientId(UUID clientId) {
        if (clientId == null) return Optional.empty();
        UUID transferId = clientToTransfer.get(clientId);
        return transferId != null ? Optional.ofNullable(activeTransfers.get(transferId)) : Optional.empty();
    }

    public Optional<TransferSession> findByConnectionId(UUID connectionId) {
        if (connectionId == null) return Optional.empty();
        for (TransferSession session : activeTransfers.values()) {
            if (connectionId.equals(session.senderConnectionId())
                    || connectionId.equals(session.recipientConnectionId())) {
                return Optional.of(session);
            }
        }
        return Optional.empty();
    }

    /**
     * Removes an active transfer and releases both sender and recipient concurrency slots.
     *
     * @param transferId transfer UUID
     * @return the removed session, or empty if not found
     */
    public Optional<TransferSession> remove(UUID transferId) {
        if (transferId == null) return Optional.empty();
        TransferSession session = activeTransfers.remove(transferId);
        if (session != null) {
            clientToTransfer.remove(session.senderClientId(), session.transferId());
            clientToTransfer.remove(session.recipientClientId(), session.transferId());
            return Optional.of(session);
        }
        return Optional.empty();
    }

    public int activeCount() {
        return activeTransfers.size();
    }

    public boolean isClientActive(UUID clientId) {
        return clientId != null && clientToTransfer.containsKey(clientId);
    }

    public Collection<TransferSession> activeTransfers() {
        return Collections.unmodifiableCollection(activeTransfers.values());
    }

    /**
     * Finds transfers that have timed out based on offer or progress inactivity.
     *
     * @param nowEpochMs current epoch millisecond timestamp
     * @param timeoutMillis timeout duration in milliseconds
     * @return list of expired transfer sessions
     */
    public List<TransferSession> findExpiredTransfers(long nowEpochMs, long timeoutMillis) {
        List<TransferSession> expired = new ArrayList<>();
        for (TransferSession session : activeTransfers.values()) {
            if (session.isExpired(nowEpochMs, timeoutMillis)) {
                expired.add(session);
            }
        }
        return expired;
    }

    public void clear() {
        activeTransfers.clear();
        clientToTransfer.clear();
    }
}
