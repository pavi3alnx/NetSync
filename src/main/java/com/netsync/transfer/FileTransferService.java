package com.netsync.transfer;

import com.netsync.common.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;

/**
 * Core business service managing file transfer lifecycles, multithreaded chunk
 * coordination, and NIO Selector integration.
 * <p>
 * Thread-safety model:
 * <ul>
 *   <li>All transfer control operations, state transitions, and frame enqueueing
 *       are owned and executed strictly on the Selector event-loop thread.</li>
 *   <li>Blocking disk I/O (writing chunks to {@code .part} files) and SHA-256
 *       digest computations are strictly offloaded to {@link TransferWorkerPool}.</li>
 *   <li>Worker completion callbacks are posted back to the Selector thread via
 *       pre-reserved {@link SelectorTasks.Reservation} instances, guaranteeing that
 *       worker results are never silently lost due to task-queue saturation.</li>
 * </ul>
 */
public final class FileTransferService {
    static final int MAX_DEFERRED_CLEANUPS = 64;
    static final int MAX_FAILED_CLEANUPS = 64;

    private final TransferRegistry registry;
    private final SafeFileStorage storage;
    private final TransferWorkerPool workerPool;
    private final OutboundSink outboundSink;
    private final SelectorTasks selectorTasks;
    private final SessionRegistry sessionRegistry;
    private final AuthorizationService authorizationService;
    private final Supplier<Long> clock;
    private final Object cleanupLock = new Object();
    private final LinkedHashSet<UUID> deferredCleanups = new LinkedHashSet<>();
    private final LinkedHashSet<UUID> failedCleanups = new LinkedHashSet<>();
    private boolean cleanupDrainActive;

    public FileTransferService(
            TransferRegistry registry,
            SafeFileStorage storage,
            TransferWorkerPool workerPool,
            OutboundSink outboundSink,
            SelectorTasks selectorTasks,
            SessionRegistry sessionRegistry,
            AuthorizationService authorizationService
    ) {
        this(registry, storage, workerPool, outboundSink, selectorTasks, sessionRegistry, authorizationService, System::currentTimeMillis);
    }

    public FileTransferService(
            TransferRegistry registry,
            SafeFileStorage storage,
            TransferWorkerPool workerPool,
            OutboundSink outboundSink,
            SelectorTasks selectorTasks,
            SessionRegistry sessionRegistry,
            AuthorizationService authorizationService,
            Supplier<Long> clock
    ) {
        this.registry = Objects.requireNonNull(registry, "TransferRegistry must not be null");
        this.storage = Objects.requireNonNull(storage, "SafeFileStorage must not be null");
        this.workerPool = Objects.requireNonNull(workerPool, "TransferWorkerPool must not be null");
        this.outboundSink = Objects.requireNonNull(outboundSink, "OutboundSink must not be null");
        this.selectorTasks = Objects.requireNonNull(selectorTasks, "SelectorTasks must not be null");
        this.sessionRegistry = Objects.requireNonNull(sessionRegistry, "SessionRegistry must not be null");
        this.authorizationService = Objects.requireNonNull(authorizationService, "AuthorizationService must not be null");
        this.clock = Objects.requireNonNull(clock, "Clock must not be null");
    }

    public TransferRegistry registry() {
        return registry;
    }

    public SafeFileStorage storage() {
        return storage;
    }

    public TransferWorkerPool workerPool() {
        return workerPool;
    }

    // =========================================================================
    // Control Message Dispatching (Selector Thread)
    // =========================================================================

    /**
     * Dispatches a file transfer control envelope on the Selector thread.
     *
     * @param context connection identity
     * @param message decoded JSON control message
     */
    public void handleControl(ConnectionContext context, MessageEnvelope message) {
        if (context == null || message == null) {
            return;
        }

        switch (message.type()) {
            case FILE_OFFER -> handleFileOffer(context, message);
            case FILE_ACCEPT -> handleFileAccept(context, message);
            case FILE_REJECT -> handleFileReject(context, message);
            case FILE_COMPLETE -> handleFileComplete(context, message);
            case FILE_RECEIVED -> handleFileReceived(context, message);
            case FILE_CANCEL -> handleFileCancel(context, message);
            case FILE_STATUS -> handleFileStatus(context, message);
            default -> sendError(context.connectionId(), ErrorCode.INVALID_REQUEST,
                    "Unsupported file transfer control operation: " + message.type(), message.messageId(), null);
        }
    }

    // =========================================================================
    // 1. FILE_OFFER
    // =========================================================================

    private void handleFileOffer(ConnectionContext context, MessageEnvelope message) {
        Optional<SessionView> senderSessionOpt = authenticate(context);
        if (senderSessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.UNAUTHENTICATED, "Client is not authenticated", message.messageId(), null);
            return;
        }
        SessionView senderSession = senderSessionOpt.get();

        if (!(message.body() instanceof MessageBody.FileOffer offer)) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Invalid body for FILE_OFFER", message.messageId(), null);
            return;
        }

        // Validate recipient identity & self-transfer
        if (offer.recipientClientId() == null) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Recipient client ID is required", message.messageId(), null);
            return;
        }
        if (senderSession.clientId().equals(offer.recipientClientId())) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Self-transfers are not permitted", message.messageId(), null);
            return;
        }

        // Validate safe filename
        try {
            SafeFileStorage.validateSafeFileName(offer.fileName());
        } catch (IllegalArgumentException | SecurityException e) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Invalid file name: " + e.getMessage(), message.messageId(), null);
            return;
        }

        // Validate file size bounds (0 up to 10 MiB)
        if (offer.sizeBytes() < 0 || offer.sizeBytes() > ProtocolLimits.MAX_FILE_BYTES) {
            sendError(context.connectionId(), ErrorCode.SIZE_LIMIT,
                    "File size out of bounds (0 - " + ProtocolLimits.MAX_FILE_BYTES + " bytes): " + offer.sizeBytes(),
                    message.messageId(), null);
            return;
        }

        // Verify recipient availability and active session
        Optional<SessionView> recipientSessionOpt = sessionRegistry.findByClientId(offer.recipientClientId());
        if (recipientSessionOpt.isEmpty() || !authorizationService.isActive(recipientSessionOpt.get())) {
            sendError(context.connectionId(), ErrorCode.RECIPIENT_UNAVAILABLE, "Recipient is unavailable", message.messageId(), null);
            return;
        }
        SessionView recipientSession = recipientSessionOpt.get();

        // Enforce transfer concurrency limits and deferred cleanup backlog limit
        if (isDeferredCleanupExhausted()) {
            triggerDeferredCleanupDrain();
            sendError(context.connectionId(), ErrorCode.BUSY, "Server deferred transfer cleanup capacity reached", message.messageId(), null);
            return;
        }
        if (registry.activeCount() >= ProtocolLimits.MAX_TRANSFERS) {
            sendError(context.connectionId(), ErrorCode.BUSY, "Server active transfer capacity reached", message.messageId(), null);
            return;
        }
        if (registry.isClientActive(senderSession.clientId())) {
            sendError(context.connectionId(), ErrorCode.BUSY, "Sender already has an active transfer", message.messageId(), null);
            return;
        }
        if (registry.isClientActive(recipientSession.clientId())) {
            sendError(context.connectionId(), ErrorCode.BUSY, "Recipient already has an active transfer", message.messageId(), null);
            return;
        }

        // Register new transfer offer
        UUID transferId = UUID.randomUUID();
        long now = clock.get();
        TransferSession transferSession = new TransferSession(
                transferId,
                senderSession.clientId(),
                recipientSession.clientId(),
                offer.fileName(),
                offer.sizeBytes(),
                context.connectionId(),
                now
        );
        transferSession.setRecipientConnectionId(recipientSession.connectionId());

        try {
            registry.registerOffer(transferSession);
        } catch (IllegalStateException e) {
            sendError(context.connectionId(), ErrorCode.BUSY, e.getMessage(), message.messageId(), null);
            return;
        }

        // Deliver FILE_OFFERED event to recipient
        EnqueueResult offerResult = sendFileOffered(
                recipientSession.connectionId(),
                transferId,
                senderSession.clientId(),
                offer.fileName(),
                offer.sizeBytes()
        );

        if (offerResult == EnqueueResult.REJECTED) {
            registry.remove(transferId);
            sendError(context.connectionId(), ErrorCode.RECIPIENT_UNAVAILABLE,
                    "Failed to deliver offer to recipient outbound queue", message.messageId(), transferId);
            return;
        }

        // Acknowledge sender: FILE_OFFER accepted
        sendAck(context.connectionId(), message.messageId(), MessageType.FILE_OFFER, AckStatus.ACCEPTED, transferId);
    }

    // =========================================================================
    // 2. FILE_ACCEPT
    // =========================================================================

    private void handleFileAccept(ConnectionContext context, MessageEnvelope message) {
        Optional<SessionView> callerSessionOpt = authenticate(context);
        if (callerSessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.UNAUTHENTICATED, "Client is not authenticated", message.messageId(), null);
            return;
        }
        SessionView callerSession = callerSessionOpt.get();

        if (!(message.body() instanceof MessageBody.TransferReference ref)) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Invalid body for FILE_ACCEPT", message.messageId(), null);
            return;
        }

        UUID transferId = ref.transferId();
        Optional<TransferSession> sessionOpt = registry.get(transferId);
        if (sessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Transfer not found: " + transferId, message.messageId(), transferId);
            return;
        }
        TransferSession session = sessionOpt.get();

        // Only designated recipient can accept
        if (!callerSession.clientId().equals(session.recipientClientId())) {
            sendError(context.connectionId(), ErrorCode.FORBIDDEN, "Only the recipient can accept a transfer offer", message.messageId(), transferId);
            return;
        }

        // Must be in OFFERED state
        if (session.state() != TransferState.OFFERED) {
            sendError(context.connectionId(), ErrorCode.INVALID_STATE,
                    "Transfer is in invalid state for accept: " + session.state(), message.messageId(), transferId);
            return;
        }

        long now = clock.get();
        session.setRecipientConnectionId(context.connectionId());
        session.transitionTo(TransferState.ACCEPTED, now);

        // Notify sender via FILE_STATUS: ACCEPTED
        sendFileStatus(session.senderConnectionId(), transferId, FileStatusState.ACCEPTED, "Recipient accepted transfer offer", null);

        // Acknowledge recipient
        sendAck(context.connectionId(), message.messageId(), MessageType.FILE_ACCEPT, AckStatus.ACCEPTED, transferId);
    }

    // =========================================================================
    // 3. FILE_REJECT
    // =========================================================================

    private void handleFileReject(ConnectionContext context, MessageEnvelope message) {
        Optional<SessionView> callerSessionOpt = authenticate(context);
        if (callerSessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.UNAUTHENTICATED, "Client is not authenticated", message.messageId(), null);
            return;
        }
        SessionView callerSession = callerSessionOpt.get();

        if (!(message.body() instanceof MessageBody.TransferReference ref)) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Invalid body for FILE_REJECT", message.messageId(), null);
            return;
        }

        UUID transferId = ref.transferId();
        Optional<TransferSession> sessionOpt = registry.get(transferId);
        if (sessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Transfer not found: " + transferId, message.messageId(), transferId);
            return;
        }
        TransferSession session = sessionOpt.get();

        // Only designated recipient can reject
        if (!callerSession.clientId().equals(session.recipientClientId())) {
            sendError(context.connectionId(), ErrorCode.FORBIDDEN, "Only the recipient can reject a transfer offer", message.messageId(), transferId);
            return;
        }

        // Must be in OFFERED state
        if (session.state() != TransferState.OFFERED) {
            sendError(context.connectionId(), ErrorCode.INVALID_STATE,
                    "Transfer is in invalid state for reject: " + session.state(), message.messageId(), transferId);
            return;
        }

        long now = clock.get();
        session.transitionTo(TransferState.REJECTED, now);
        registry.remove(transferId);
        scheduleCleanup(session);

        // Notify sender via FILE_STATUS: REJECTED
        sendFileStatus(session.senderConnectionId(), transferId, FileStatusState.REJECTED, "Recipient rejected transfer offer", null);

        // Acknowledge recipient
        sendAck(context.connectionId(), message.messageId(), MessageType.FILE_REJECT, AckStatus.ACCEPTED, transferId);
    }

    // =========================================================================
    // 4. FILE_CHUNK (Binary Frame Kind 0x02)
    // =========================================================================

    /**
     * Processes an incoming binary chunk on the Selector thread.
     * <p>
     * Validates participant authorization, transfer state, and sequence ordering.
     * Disk writing is strictly dispatched to {@link TransferWorkerPool} after
     * reserving capacity via {@link SelectorTasks#tryReserveCompletion()}.
     */
    public void handleChunk(ConnectionContext context, FileChunk chunk) {
        if (context == null || chunk == null) {
            return;
        }

        Optional<SessionView> callerSessionOpt = authenticate(context);
        if (callerSessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.UNAUTHENTICATED, "Client is not authenticated", chunk.messageId(), chunk.transferId());
            return;
        }
        SessionView callerSession = callerSessionOpt.get();

        UUID transferId = chunk.transferId();
        Optional<TransferSession> sessionOpt = registry.get(transferId);
        if (sessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Transfer not found: " + transferId, chunk.messageId(), transferId);
            return;
        }
        TransferSession session = sessionOpt.get();

        if (!callerSession.connectionId().equals(session.senderConnectionId())) {
            sendError(context.connectionId(), ErrorCode.FORBIDDEN,
                    "File chunks must come from the connection that created the transfer", chunk.messageId(), transferId);
            return;
        }

        // Only designated sender can upload chunks
        if (!callerSession.clientId().equals(session.senderClientId())) {
            sendError(context.connectionId(), ErrorCode.FORBIDDEN, "Only the sender can upload file chunks", chunk.messageId(), transferId);
            return;
        }

        // Validate transfer state
        if (session.state() == TransferState.OFFERED) {
            sendError(context.connectionId(), ErrorCode.INVALID_STATE, "Cannot send chunks before offer is accepted", chunk.messageId(), transferId);
            return;
        }
        if (session.state().isTerminal()) {
            sendError(context.connectionId(), ErrorCode.INVALID_STATE, "Cannot send chunks for terminated transfer: " + session.state(), chunk.messageId(), transferId);
            return;
        }

        // Stop-and-wait means only one chunk may be undergoing disk work at a time.
        // A repeated current sequence is rejected without scheduling a second append.
        if (session.chunkWritePending()) {
            sendError(context.connectionId(), ErrorCode.BUSY,
                    "A chunk write is already pending for this transfer", chunk.messageId(), transferId);
            return;
        }

        // Validate sequence number
        if (chunk.sequence() != session.expectedSequence()) {
            if (chunk.sequence() < session.expectedSequence()) {
                abortTransfer(session, ErrorCode.INVALID_SEQUENCE,
                        "Duplicate chunk sequence: expected " + session.expectedSequence() + ", got " + chunk.sequence(),
                        chunk.messageId());
            } else {
                abortTransfer(session, ErrorCode.INVALID_SEQUENCE,
                        "Out-of-order chunk sequence: expected " + session.expectedSequence() + ", got " + chunk.sequence(),
                        chunk.messageId());
            }
            return;
        }

        // Validate byte bounds against declared file size
        byte[] chunkData = chunk.data();
        if (session.receivedBytes() + chunkData.length > session.declaredSizeBytes()) {
            abortTransfer(session, ErrorCode.SIZE_LIMIT,
                    "Received bytes (" + (session.receivedBytes() + chunkData.length)
                            + ") exceeds declared size (" + session.declaredSizeBytes() + ")",
                    chunk.messageId());
            return;
        }

        // Validate non-final chunk size rule (must be exactly 16,384 bytes unless final)
        boolean isFinalChunk = (session.receivedBytes() + chunkData.length == session.declaredSizeBytes());
        if (!isFinalChunk && chunkData.length != ProtocolLimits.MAX_CHUNK_BYTES) {
            abortTransfer(session, ErrorCode.INVALID_REQUEST,
                    "Non-final chunk must be exactly " + ProtocolLimits.MAX_CHUNK_BYTES + " bytes, got: " + chunkData.length,
                    chunk.messageId());
            return;
        }

        // Reserve Selector completion capacity BEFORE submitting worker work
        Optional<SelectorTasks.Reservation> reservationOpt = selectorTasks.tryReserveCompletion();
        if (reservationOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.BUSY, "Selector completion queue capacity saturated", chunk.messageId(), transferId);
            return;
        }
        SelectorTasks.Reservation reservation = reservationOpt.get();
        session.setChunkWritePending(true);

        // Offload disk write to bounded worker pool
        try {
            workerPool.submit(() -> {
                try {
                    boolean current;
                    boolean wrote = false;
                    synchronized (session.storageLock()) {
                        current = registry.get(transferId).orElse(null) == session
                                && session.state().isActive() && session.chunkWritePending();
                        if (current) {
                            storage.appendChunk(transferId, chunkData);
                            wrote = true;
                        }
                        if (wrote && (!registry.get(transferId).isPresent()
                                || !session.state().isActive()
                                || registry.get(transferId).orElse(null) != session)) {
                            deletePartialWithBoundedRetry(transferId);
                        }
                    }

                    // Post completion back to Selector thread via reservation
                    boolean wroteChunk = wrote;
                    reservation.submit(() -> onChunkWriteCompleted(session, chunk, wroteChunk));
                } catch (Throwable t) {
                    if (registry.get(transferId).orElse(null) != session || !session.state().isActive()) {
                        synchronized (session.storageLock()) { deletePartialWithBoundedRetry(transferId); }
                    }
                    try {
                        reservation.submit(() -> onChunkWriteFailed(session, chunk, safeFailure(t)));
                    } catch (IllegalStateException cancelledByShutdown) {
                        // Selector shutdown owns terminal cleanup; the application closes
                        // the worker pool and this worker owns the final partial-file removal.
                        synchronized (session.storageLock()) { deletePartialWithBoundedRetry(transferId); }
                    }
                } finally {
                    drainDeferredCleanupsOnWorker();
                }
            });
        } catch (RejectedExecutionException e) {
            reservation.close();
            session.setChunkWritePending(false);
            sendError(context.connectionId(), ErrorCode.BUSY, "Transfer worker pool saturated", chunk.messageId(), transferId);
        }
    }

    /**
     * Executes on the Selector thread when chunk disk writing succeeds.
     */
    private void onChunkWriteCompleted(TransferSession acceptedSession, FileChunk chunk, boolean wroteChunk) {
        UUID transferId = acceptedSession.transferId();
        Optional<TransferSession> sessionOpt = registry.get(transferId);
        if (sessionOpt.isEmpty() || sessionOpt.get() != acceptedSession || !sessionOpt.get().state().isActive()
                || !acceptedSession.chunkWritePending()
                || acceptedSession.expectedSequence() != chunk.sequence()) {
            // Transfer was cancelled, failed, or disconnected while worker was running
            if (acceptedSession.chunkWritePending()) acceptedSession.setChunkWritePending(false);
            // Worker has already observed terminal ownership and removed the file.
            // Keep cleanup idempotent for cancellation races that arrive after append.
            if (wroteChunk) scheduleCleanup(acceptedSession);
            return;
        }
        TransferSession session = sessionOpt.get();
        session.setChunkWritePending(false);

        long now = clock.get();
        session.validateAndRecordChunk(chunk.sequence(), chunk.data().length, now);

        // Forward chunk to recipient with a fresh message UUID
        FileChunk forwardedChunk = new FileChunk(UUID.randomUUID(), transferId, chunk.sequence(), chunk.data());
        EnqueueResult recipientResult = outboundSink.send(session.recipientConnectionId(), forwardedChunk);

        if (recipientResult == EnqueueResult.REJECTED) {
            abortTransfer(session, ErrorCode.TRANSFER_FAILED, "Recipient outbound queue saturated", null);
            return;
        }

        // Acknowledge sender with ACK/STORED
        sendAck(session.senderConnectionId(), chunk.messageId(), MessageType.FILE_CHUNK, AckStatus.STORED, transferId);
    }

    /**
     * Executes on the Selector thread when chunk disk writing fails.
     */
    private void onChunkWriteFailed(TransferSession acceptedSession, FileChunk chunk, String errorMessage) {
        UUID transferId = acceptedSession.transferId();
        Optional<TransferSession> sessionOpt = registry.get(transferId);
        if (sessionOpt.isEmpty() || sessionOpt.get() != acceptedSession || !sessionOpt.get().state().isActive()) {
            if (acceptedSession.chunkWritePending()) acceptedSession.setChunkWritePending(false);
            scheduleCleanup(acceptedSession);
            return;
        }
        TransferSession session = sessionOpt.get();
        if (session.chunkWritePending()) session.setChunkWritePending(false);
        abortTransfer(session, ErrorCode.TRANSFER_FAILED, "Storage error writing chunk: " + errorMessage, chunk.messageId());
    }

    private static String safeFailure(Throwable failure) {
        return failure == null || failure.getMessage() == null ? "I/O failure" : failure.getMessage();
    }

    private void scheduleCleanup(TransferSession session) {
        if (session.chunkWritePending()) return;
        UUID transferId = session.transferId();
        try {
            workerPool.submit(() -> {
                try {
                    synchronized (session.storageLock()) {
                        // Recheck after acquiring storage ownership; a queued append may
                        // have been accepted before the terminal transition.
                        if (registry.get(transferId).orElse(null) != session || !session.state().isActive()) {
                            deletePartialWithBoundedRetry(transferId);
                        }
                    }
                } finally {
                    drainDeferredCleanupsOnWorker();
                }
            });
        } catch (RejectedExecutionException rejected) {
            // Queue was saturated or pool is shutting down; retain transferId in bounded
            // deferred cleanup set so the next available worker or checkTimeouts pass drains it.
            enqueueDeferredCleanup(transferId);
        }
    }

    private void cleanupWhenIdle(TransferSession session) {
        // Always queue cleanup behind any accepted append on the session's storage
        // lock. The worker rechecks registry/state while holding that same lock.
        scheduleCleanup(session);
    }

    private boolean isDeferredCleanupExhausted() {
        synchronized (cleanupLock) {
            return deferredCleanups.size() + registry.activeCount() >= MAX_DEFERRED_CLEANUPS;
        }
    }

    private void enqueueDeferredCleanup(UUID transferId) {
        if (transferId == null) return;
        synchronized (cleanupLock) {
            if (deferredCleanups.contains(transferId) || deferredCleanups.size() >= MAX_DEFERRED_CLEANUPS) {
                return;
            }
            deferredCleanups.add(transferId);
        }
    }

    private void recordFailedCleanup(UUID transferId) {
        if (transferId == null) return;
        synchronized (cleanupLock) {
            recordFailedCleanupLocked(transferId);
        }
    }

    private void recordFailedCleanupLocked(UUID transferId) {
        failedCleanups.remove(transferId);
        while (failedCleanups.size() >= MAX_FAILED_CLEANUPS) {
            Iterator<UUID> it = failedCleanups.iterator();
            it.next();
            it.remove();
        }
        failedCleanups.add(transferId);
    }

    private void clearCleanupTracking(UUID transferId) {
        if (transferId == null) return;
        synchronized (cleanupLock) {
            deferredCleanups.remove(transferId);
            failedCleanups.remove(transferId);
        }
    }

    private boolean deletePartialWithBoundedRetry(UUID transferId) {
        for (int attempt = 0; attempt < 50; attempt++) {
            if (!storage.exists(transferId)) {
                clearCleanupTracking(transferId);
                return true;
            }
            if (storage.deletePartialFile(transferId) || !storage.exists(transferId)) {
                clearCleanupTracking(transferId);
                return true;
            }
            if (attempt < 49) {
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (storage.exists(transferId)) {
            recordFailedCleanup(transferId);
            return false;
        }
        clearCleanupTracking(transferId);
        return true;
    }

    private void drainDeferredCleanupsOnWorker() {
        while (true) {
            UUID next;
            synchronized (cleanupLock) {
                Iterator<UUID> it = deferredCleanups.iterator();
                if (!it.hasNext()) {
                    cleanupDrainActive = false;
                    return;
                }
                next = it.next();
                it.remove();
            }
            // Only delete if no active session currently owns this transferId
            Optional<TransferSession> current = registry.get(next);
            if (current.isEmpty() || !current.get().state().isActive()) {
                deletePartialWithBoundedRetry(next);
            }
        }
    }

    private void triggerDeferredCleanupDrain() {
        boolean shouldSubmit = false;
        synchronized (cleanupLock) {
            if (!deferredCleanups.isEmpty() && !cleanupDrainActive) {
                cleanupDrainActive = true;
                shouldSubmit = true;
            }
        }
        if (shouldSubmit) {
            try {
                workerPool.submit(this::drainDeferredCleanupsOnWorker);
            } catch (RejectedExecutionException rejected) {
                synchronized (cleanupLock) {
                    cleanupDrainActive = false;
                }
            }
        }
    }

    public Set<UUID> pendingCleanupIds() {
        synchronized (cleanupLock) {
            return Set.copyOf(deferredCleanups);
        }
    }

    public Set<UUID> failedCleanupIds() {
        synchronized (cleanupLock) {
            return Set.copyOf(failedCleanups);
        }
    }

    // =========================================================================
    // 5. FILE_COMPLETE
    // =========================================================================

    private void handleFileComplete(ConnectionContext context, MessageEnvelope message) {
        Optional<SessionView> senderSessionOpt = authenticate(context);
        if (senderSessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.UNAUTHENTICATED, "Client is not authenticated", message.messageId(), null);
            return;
        }
        SessionView senderSession = senderSessionOpt.get();

        if (!(message.body() instanceof MessageBody.FileComplete complete)) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Invalid body for FILE_COMPLETE", message.messageId(), null);
            return;
        }

        UUID transferId = complete.transferId();
        Optional<TransferSession> sessionOpt = registry.get(transferId);
        if (sessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Transfer not found: " + transferId, message.messageId(), transferId);
            return;
        }
        TransferSession session = sessionOpt.get();

        // Only sender can complete transfer
        if (!senderSession.clientId().equals(session.senderClientId())) {
            sendError(context.connectionId(), ErrorCode.FORBIDDEN, "Only the sender can complete the transfer", message.messageId(), transferId);
            return;
        }

        // Validate state (ACCEPTED for 0-byte file, IN_PROGRESS for non-empty file)
        if (session.state() != TransferState.ACCEPTED && session.state() != TransferState.IN_PROGRESS) {
            sendError(context.connectionId(), ErrorCode.INVALID_STATE,
                    "Invalid state for FILE_COMPLETE: " + session.state(), message.messageId(), transferId);
            return;
        }

        // Validate chunk count
        if (complete.chunkCount() != session.chunkCount()) {
            abortTransfer(session, ErrorCode.INVALID_SEQUENCE,
                    "Chunk count mismatch: expected " + session.chunkCount() + ", got " + complete.chunkCount(),
                    message.messageId());
            return;
        }

        // Validate received byte count
        if (session.receivedBytes() != session.declaredSizeBytes()) {
            abortTransfer(session, ErrorCode.SIZE_LIMIT,
                    "Received bytes mismatch: expected " + session.declaredSizeBytes() + ", got " + session.receivedBytes(),
                    message.messageId());
            return;
        }

        // Reserve completion capacity before offloading SHA-256 computation
        Optional<SelectorTasks.Reservation> reservationOpt = selectorTasks.tryReserveCompletion();
        if (reservationOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.BUSY, "Selector completion queue capacity saturated", message.messageId(), transferId);
            return;
        }
        SelectorTasks.Reservation reservation = reservationOpt.get();

        // Offload SHA-256 calculation to worker pool
        try {
            workerPool.submit(() -> {
                try {
                    String actualSha256;
                    if (session.declaredSizeBytes() == 0L) {
                        actualSha256 = TransferChecksum.EMPTY_SHA256;
                    } else {
                        Path partFile = storage.resolvePartFile(transferId);
                        actualSha256 = TransferChecksum.computeFileDigestHex(partFile);
                    }

                    reservation.submit(() -> onChecksumComputed(transferId, message.messageId(), complete.sha256(), actualSha256, complete.chunkCount()));
                } catch (Throwable t) {
                    try {
                        reservation.submit(() -> onChecksumComputationFailed(transferId, message.messageId(), safeFailure(t)));
                    } catch (IllegalStateException cancelledByShutdown) {
                        // Shutdown invokes disconnect cleanup and terminates the worker owner.
                    }
                } finally {
                    drainDeferredCleanupsOnWorker();
                }
            });
        } catch (RejectedExecutionException e) {
            reservation.close();
            sendError(context.connectionId(), ErrorCode.BUSY, "Transfer worker pool saturated", message.messageId(), transferId);
        }
    }

    /**
     * Executes on the Selector thread after SHA-256 checksum calculation finishes.
     */
    private void onChecksumComputed(UUID transferId, UUID completeMessageId, String expectedSha256, String actualSha256, int chunkCount) {
        Optional<TransferSession> sessionOpt = registry.get(transferId);
        if (sessionOpt.isEmpty() || !sessionOpt.get().state().isActive()) {
            return;
        }
        TransferSession session = sessionOpt.get();

        // Verify SHA-256 hash match
        if (!TransferChecksum.matches(expectedSha256, actualSha256)) {
            abortTransfer(session, ErrorCode.TRANSFER_FAILED,
                    "SHA-256 checksum mismatch: expected " + expectedSha256 + ", computed " + actualSha256,
                    completeMessageId);
            return;
        }

        long now = clock.get();
        session.setVerifiedSha256(actualSha256);
        session.transitionTo(TransferState.SERVER_VERIFIED, now);

        // Forward FILE_COMPLETE event to recipient (fresh message ID, null replyTo)
        EnqueueResult recipientResult = sendFileComplete(session.recipientConnectionId(), transferId, chunkCount, actualSha256);
        if (recipientResult == EnqueueResult.REJECTED) {
            abortTransfer(session, ErrorCode.TRANSFER_FAILED, "Recipient outbound queue saturated", completeMessageId);
            return;
        }

        // Acknowledge sender: FILE_COMPLETE accepted
        sendAck(session.senderConnectionId(), completeMessageId, MessageType.FILE_COMPLETE, AckStatus.ACCEPTED, transferId);
    }

    private void onChecksumComputationFailed(UUID transferId, UUID completeMessageId, String errorMessage) {
        Optional<TransferSession> sessionOpt = registry.get(transferId);
        if (sessionOpt.isEmpty() || !sessionOpt.get().state().isActive()) {
            return;
        }
        TransferSession session = sessionOpt.get();
        abortTransfer(session, ErrorCode.TRANSFER_FAILED, "Failed to compute checksum: " + errorMessage, completeMessageId);
    }

    // =========================================================================
    // 6. FILE_RECEIVED
    // =========================================================================

    private void handleFileReceived(ConnectionContext context, MessageEnvelope message) {
        Optional<SessionView> recipientSessionOpt = authenticate(context);
        if (recipientSessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.UNAUTHENTICATED, "Client is not authenticated", message.messageId(), null);
            return;
        }
        SessionView recipientSession = recipientSessionOpt.get();

        if (!(message.body() instanceof MessageBody.FileReceived received)) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Invalid body for FILE_RECEIVED", message.messageId(), null);
            return;
        }

        UUID transferId = received.transferId();
        Optional<TransferSession> sessionOpt = registry.get(transferId);
        if (sessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Transfer not found: " + transferId, message.messageId(), transferId);
            return;
        }
        TransferSession session = sessionOpt.get();

        // Only designated recipient can confirm receipt
        if (!recipientSession.clientId().equals(session.recipientClientId())) {
            sendError(context.connectionId(), ErrorCode.FORBIDDEN, "Only the recipient can send FILE_RECEIVED", message.messageId(), transferId);
            return;
        }

        // Transfer must be in SERVER_VERIFIED state
        if (session.state() != TransferState.SERVER_VERIFIED) {
            sendError(context.connectionId(), ErrorCode.INVALID_STATE,
                    "Invalid state for FILE_RECEIVED: " + session.state(), message.messageId(), transferId);
            return;
        }

        // Validate recipient confirmed hash against server-verified hash
        if (!TransferChecksum.matches(session.verifiedSha256(), received.sha256())) {
            abortTransfer(session, ErrorCode.TRANSFER_FAILED,
                    "Recipient confirmed hash does not match server-verified hash", message.messageId());
            return;
        }

        // Conclude transfer successfully
        long now = clock.get();
        session.transitionTo(TransferState.COMPLETED, now);
        cleanupWhenIdle(session);
        registry.remove(transferId);

        // Acknowledge recipient
        sendAck(context.connectionId(), message.messageId(), MessageType.FILE_RECEIVED, AckStatus.ACCEPTED, transferId);

        // Send FILE_STATUS(COMPLETED) to sender
        sendFileStatus(session.senderConnectionId(), transferId, FileStatusState.COMPLETED, "Transfer completed successfully", null);
    }

    // =========================================================================
    // 7. FILE_CANCEL
    // =========================================================================

    private void handleFileCancel(ConnectionContext context, MessageEnvelope message) {
        Optional<SessionView> callerSessionOpt = authenticate(context);
        if (callerSessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.UNAUTHENTICATED, "Client is not authenticated", message.messageId(), null);
            return;
        }
        SessionView callerSession = callerSessionOpt.get();

        if (!(message.body() instanceof MessageBody.TransferReference ref)) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Invalid body for FILE_CANCEL", message.messageId(), null);
            return;
        }

        UUID transferId = ref.transferId();
        Optional<TransferSession> sessionOpt = registry.get(transferId);
        if (sessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Transfer not found: " + transferId, message.messageId(), transferId);
            return;
        }
        TransferSession session = sessionOpt.get();

        // Either participant can cancel
        boolean isSender = callerSession.clientId().equals(session.senderClientId());
        boolean isRecipient = callerSession.clientId().equals(session.recipientClientId());
        if (!isSender && !isRecipient) {
            sendError(context.connectionId(), ErrorCode.FORBIDDEN, "Only transfer participants can cancel", message.messageId(), transferId);
            return;
        }

        // Must not already be terminal
        if (session.state().isTerminal()) {
            sendError(context.connectionId(), ErrorCode.INVALID_STATE, "Transfer is already terminated: " + session.state(), message.messageId(), transferId);
            return;
        }

        long now = clock.get();
        session.transitionTo(TransferState.CANCELLED, now);
        cleanupWhenIdle(session);
        registry.remove(transferId);

        // Acknowledge caller
        sendAck(context.connectionId(), message.messageId(), MessageType.FILE_CANCEL, AckStatus.ACCEPTED, transferId);

        // Notify peer participant via FILE_STATUS: CANCELLED
        UUID peerConnectionId = isSender ? session.recipientConnectionId() : session.senderConnectionId();
        if (peerConnectionId != null) {
            sendFileStatus(peerConnectionId, transferId, FileStatusState.CANCELLED, "Transfer cancelled by peer", null);
        }
    }

    // =========================================================================
    // 8. FILE_STATUS (Client Query)
    // =========================================================================

    private void handleFileStatus(ConnectionContext context, MessageEnvelope message) {
        Optional<SessionView> callerSessionOpt = authenticate(context);
        if (callerSessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.UNAUTHENTICATED, "Client is not authenticated", message.messageId(), null);
            return;
        }
        SessionView callerSession = callerSessionOpt.get();

        if (!(message.body() instanceof MessageBody.FileStatus status)) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Invalid body for FILE_STATUS", message.messageId(), null);
            return;
        }

        UUID transferId = status.transferId();
        Optional<TransferSession> sessionOpt = registry.get(transferId);
        if (sessionOpt.isEmpty()) {
            sendError(context.connectionId(), ErrorCode.INVALID_REQUEST, "Transfer not found: " + transferId, message.messageId(), transferId);
            return;
        }
        TransferSession session = sessionOpt.get();

        if (!callerSession.clientId().equals(session.senderClientId()) && !callerSession.clientId().equals(session.recipientClientId())) {
            sendError(context.connectionId(), ErrorCode.FORBIDDEN, "Only transfer participants can query status", message.messageId(), transferId);
            return;
        }

        FileStatusState mappedState = switch (session.state()) {
            case OFFERED, ACCEPTED, IN_PROGRESS, SERVER_VERIFIED -> FileStatusState.ACCEPTED;
            case COMPLETED -> FileStatusState.COMPLETED;
            case REJECTED -> FileStatusState.REJECTED;
            case CANCELLED -> FileStatusState.CANCELLED;
            case FAILED -> FileStatusState.FAILED;
        };

        sendFileStatus(context.connectionId(), transferId, mappedState, "Current transfer state: " + session.state(), message.messageId());
    }

    // =========================================================================
    // Disconnection & Timeout Cleanup
    // =========================================================================

    /**
     * Cleans up all active transfers associated with a disconnected connection.
     *
     * @param connectionId disconnected connection UUID
     */
    public void onDisconnected(UUID connectionId) {
        if (connectionId == null) {
            return;
        }

        List<TransferSession> affected = new ArrayList<>();
        for (TransferSession session : registry.activeTransfers()) {
            if (connectionId.equals(session.senderConnectionId())
                    || connectionId.equals(session.recipientConnectionId())) {
                affected.add(session);
            }
        }

        long now = clock.get();
        for (TransferSession session : affected) {
            try {
                session.transitionTo(TransferState.FAILED, now);
            } catch (Exception ignored) {}

            cleanupWhenIdle(session);
            registry.remove(session.transferId());

            // Notify surviving peer
            UUID peerConnectionId = connectionId.equals(session.senderConnectionId())
                    ? session.recipientConnectionId()
                    : session.senderConnectionId();

            if (peerConnectionId != null && !peerConnectionId.equals(connectionId)) {
                sendFileStatus(peerConnectionId, session.transferId(), FileStatusState.FAILED, "Peer disconnected", null);
            }
        }
    }

    /**
     * Checks for and cleans up transfers that have exceeded offer or progress timeouts.
     *
     * @return number of expired transfers cleaned up
     */
    public int checkTimeouts() {
        long now = clock.get();
        long timeoutMillis = ProtocolLimits.TRANSFER_TIMEOUT_SECONDS * 1000L;
        List<TransferSession> expired = registry.findExpiredTransfers(now, timeoutMillis);

        for (TransferSession session : expired) {
            try {
                session.transitionTo(TransferState.FAILED, now);
            } catch (Exception ignored) {}

            cleanupWhenIdle(session);
            registry.remove(session.transferId());

            sendFileStatus(session.senderConnectionId(), session.transferId(), FileStatusState.FAILED, "Transfer timed out", null);
            if (session.recipientConnectionId() != null) {
                sendFileStatus(session.recipientConnectionId(), session.transferId(), FileStatusState.FAILED, "Transfer timed out", null);
            }
        }
        triggerDeferredCleanupDrain();
        return expired.size();
    }

    // =========================================================================
    // Helper & Abort Routines
    // =========================================================================

    /**
     * Aborts an active transfer due to error or protocol violation.
     */
    private void abortTransfer(TransferSession session, ErrorCode errorCode, String reason, UUID replyToMessageId) {
        long now = clock.get();
        try {
            session.transitionTo(TransferState.FAILED, now);
        } catch (Exception ignored) {}

        cleanupWhenIdle(session);
        registry.remove(session.transferId());

        if (replyToMessageId != null) {
            sendError(session.senderConnectionId(), errorCode, reason, replyToMessageId, session.transferId());
        }

        if (session.recipientConnectionId() != null) {
            sendFileStatus(session.recipientConnectionId(), session.transferId(), FileStatusState.FAILED, reason, null);
        }
    }

    private Optional<SessionView> authenticate(ConnectionContext context) {
        if (context == null) {
            return Optional.empty();
        }
        Optional<SessionView> sessionOpt = authorizationService.authenticatedSession(context);
        if (sessionOpt.isEmpty()) {
            sessionOpt = context.session();
        }
        if (sessionOpt.isEmpty()) {
            return Optional.empty();
        }
        SessionView session = sessionOpt.get();
        if (!authorizationService.isActive(session)) {
            return Optional.empty();
        }
        return Optional.of(session);
    }

    private EnqueueResult sendAck(UUID connectionId, UUID replyTo, MessageType op, AckStatus status, UUID transferId) {
        MessageEnvelope envelope = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ACK,
                UUID.randomUUID(),
                replyTo,
                new MessageBody.Ack(op, status, transferId)
        );
        return outboundSink.send(connectionId, envelope);
    }

    private EnqueueResult sendError(UUID connectionId, ErrorCode code, String message, UUID replyTo, UUID transferId) {
        MessageEnvelope envelope = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ERROR,
                UUID.randomUUID(),
                replyTo,
                new MessageBody.Error(code, message, transferId)
        );
        return outboundSink.send(connectionId, envelope);
    }

    private EnqueueResult sendFileStatus(UUID connectionId, UUID transferId, FileStatusState state, String reason, UUID replyTo) {
        MessageEnvelope envelope = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_STATUS,
                UUID.randomUUID(),
                replyTo,
                new MessageBody.FileStatus(transferId, state, reason)
        );
        return outboundSink.send(connectionId, envelope);
    }

    private EnqueueResult sendFileOffered(UUID recipientConnectionId, UUID transferId, UUID senderClientId, String fileName, long sizeBytes) {
        MessageEnvelope envelope = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(transferId, senderClientId, fileName, sizeBytes)
        );
        return outboundSink.send(recipientConnectionId, envelope);
    }

    private EnqueueResult sendFileComplete(UUID recipientConnectionId, UUID transferId, int chunkCount, String sha256) {
        MessageEnvelope envelope = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_COMPLETE,
                UUID.randomUUID(),
                null,
                new MessageBody.FileComplete(transferId, chunkCount, sha256)
        );
        return outboundSink.send(recipientConnectionId, envelope);
    }
}
