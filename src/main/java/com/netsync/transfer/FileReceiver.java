package com.netsync.transfer;

import com.netsync.common.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Client-side file receiver implementing the stop-and-wait chunk protocol.
 * <p>
 * Handles incoming {@link MessageBody.FileOffered} proposals, accepts/rejects transfers,
 * validates chunk sequencing and sizes, appends binary data to safe partial storage,
 * and verifies final byte count, chunk count, and SHA-256 checksums before finalization.
 */
public final class FileReceiver implements AutoCloseable {

    public enum ReceiverState {
        IDLE,
        OFFERED,
        RECEIVING,
        AWAITING_RECEIVED_ACK,
        COMPLETED,
        REJECTED,
        CANCELLED,
        FAILED
    }

    private final Consumer<OutboundFrame> frameSink;
    private final SafeFileStorage storage;
    private final TransferWorkerPool workerPool;
    private final boolean ownsWorkerPool;

    private final Object stateLock = new Object();
    private final Object ioLock = new Object();
    private volatile ReceiverState state = ReceiverState.IDLE;

    private UUID transferId;
    private UUID senderClientId;
    private String fileName;
    private long declaredSizeBytes;

    private int expectedNextSequence;
    private int chunksReceived;
    private long bytesReceived;
    private TransferChecksum.Incremental incrementalChecksum;
    private String finalSha256;
    private Path finalFile;
    private boolean finalPublished;
    private CompletableFuture<Void> cleanupFuture = CompletableFuture.completedFuture(null);

    private CompletableFuture<FileTransferResult> completionFuture;
    private Consumer<FileTransferProgress> progressListener;
    private boolean writePending;
    private MessageBody.FileComplete deferredComplete;
    private final ArrayDeque<FileChunk> queuedChunks = new ArrayDeque<>();
    private long pendingBytes;
    private static final int MAX_QUEUED_CHUNKS = 32;

    public FileReceiver(Consumer<OutboundFrame> frameSink, SafeFileStorage storage) {
        this(frameSink, storage, new TransferWorkerPool(2, 32), true);
    }

    public FileReceiver(Consumer<OutboundFrame> frameSink, Path downloadDir, TransferWorkerPool workerPool) {
        this(frameSink, new SafeFileStorage(downloadDir), workerPool, false);
    }

    public FileReceiver(Consumer<OutboundFrame> frameSink, SafeFileStorage storage, TransferWorkerPool workerPool) {
        this(frameSink, storage, workerPool, false);
    }

    private FileReceiver(Consumer<OutboundFrame> frameSink, SafeFileStorage storage, TransferWorkerPool workerPool, boolean ownsWorkerPool) {
        this.frameSink = Objects.requireNonNull(frameSink, "Frame sink must not be null");
        this.storage = Objects.requireNonNull(storage, "Storage must not be null");
        this.workerPool = Objects.requireNonNull(workerPool, "Worker pool must not be null");
        this.ownsWorkerPool = ownsWorkerPool;
    }

    public void setProgressListener(Consumer<FileTransferProgress> listener) {
        this.progressListener = listener;
    }

    public ReceiverState state() {
        return state;
    }

    public UUID transferId() {
        return transferId;
    }

    public UUID senderClientId() {
        return senderClientId;
    }

    public String fileName() {
        return fileName;
    }

    public long declaredSizeBytes() {
        return declaredSizeBytes;
    }

    public Path finalFile() {
        return finalFile;
    }

    public boolean isActive() {
        return state == ReceiverState.OFFERED
                || state == ReceiverState.RECEIVING
                || state == ReceiverState.AWAITING_RECEIVED_ACK;
    }

    /**
     * Handles incoming control message envelopes from the server.
     *
     * @param envelope control message envelope
     */
    public void handleControl(MessageEnvelope envelope) {
        Objects.requireNonNull(envelope, "MessageEnvelope must not be null");

        synchronized (stateLock) {
            switch (envelope.type()) {
                case FILE_OFFERED -> handleFileOffered(envelope);
                case FILE_COMPLETE -> handleFileComplete(envelope);
                case ACK -> handleAck(envelope);
                case FILE_STATUS -> handleFileStatus(envelope);
                case ERROR -> handleError(envelope);
                default -> {
                    // Ignore unrelated message types
                }
            }
        }
    }

    private void handleFileOffered(MessageEnvelope envelope) {
        if (!(envelope.body() instanceof MessageBody.FileOffered offered)) {
            return;
        }

        boolean reject = false;
        UUID rejectTid = offered.transferId();

        synchronized (stateLock) {
            if (isActive()) {
                // Already handling a transfer; reject new offer per single-transfer-per-client rule
                reject = true;
            } else if (offered.sizeBytes() < 0 || offered.sizeBytes() > ProtocolLimits.MAX_FILE_BYTES) {
                reject = true;
            } else {
                try {
                    SafeFileStorage.validateSafeFileName(offered.fileName());
                    this.transferId = offered.transferId();
                    this.senderClientId = offered.senderClientId();
                    this.fileName = offered.fileName();
                    this.declaredSizeBytes = offered.sizeBytes();
                    this.finalFile = null;
                    this.completionFuture = new CompletableFuture<>();
                    this.state = ReceiverState.OFFERED;
                } catch (IllegalArgumentException | SecurityException e) {
                    reject = true;
                }
            }
        }

        if (reject) {
            sendReject(rejectTid);
        }
    }

    /**
     * Accepts the pending offered transfer.
     *
     * @param transferId transfer UUID matching the offered transfer
     * @return completion future
     */
    public CompletableFuture<FileTransferResult> accept(UUID transferId) {
        Objects.requireNonNull(transferId, "Transfer ID must not be null");

        MessageEnvelope acceptEnvelope;
        CompletableFuture<FileTransferResult> future;

        synchronized (stateLock) {
            if (state != ReceiverState.OFFERED || !transferId.equals(this.transferId)) {
                throw new IllegalStateException("No offered transfer with ID: " + transferId);
            }

            this.expectedNextSequence = 0;
            this.chunksReceived = 0;
            this.bytesReceived = 0;
            this.pendingBytes = 0;
            this.queuedChunks.clear();
            this.writePending = false;
            this.deferredComplete = null;
            this.incrementalChecksum = new TransferChecksum.Incremental();

            state = ReceiverState.RECEIVING;
            future = this.completionFuture;

            acceptEnvelope = new MessageEnvelope(
                    ProtocolLimits.VERSION,
                    MessageType.FILE_ACCEPT,
                    UUID.randomUUID(),
                    null,
                    new MessageBody.TransferReference(transferId)
            );
        }

        try {
            workerPool.submit(() -> {
                try {
                    synchronized (ioLock) {
                        synchronized (stateLock) {
                            if (state != ReceiverState.RECEIVING || !transferId.equals(this.transferId)) return;
                        }
                        storage.deletePartialFile(transferId);
                    }
                    frameSink.accept(acceptEnvelope);
                } catch (RuntimeException failure) { failTransfer("Unable to prepare receiver storage"); }
            });
        } catch (RuntimeException rejected) {
            failTransfer("Receiver worker capacity is unavailable");
        }
        return future;
    }

    /**
     * Rejects the pending offered transfer.
     *
     * @param transferId transfer UUID matching the offered transfer
     */
    public void reject(UUID transferId) {
        Objects.requireNonNull(transferId, "Transfer ID must not be null");

        CompletableFuture<FileTransferResult> future = null;

        synchronized (stateLock) {
            if (state != ReceiverState.OFFERED || !transferId.equals(this.transferId)) {
                throw new IllegalStateException("No offered transfer with ID: " + transferId);
            }

            state = ReceiverState.REJECTED;
            future = this.completionFuture;
        }

        sendReject(transferId);

        if (future != null) {
            future.complete(FileTransferResult.rejected(transferId, "Rejected by receiver"));
        }
    }

    private void sendReject(UUID tid) {
        try {
            frameSink.accept(new MessageEnvelope(
                    ProtocolLimits.VERSION,
                    MessageType.FILE_REJECT,
                    UUID.randomUUID(),
                    null,
                    new MessageBody.TransferReference(tid)
            ));
        } catch (Exception ignored) {
        }
    }

    /**
     * Handles an incoming binary file chunk.
     *
     * @param chunk binary chunk
     */
    public void handleChunk(FileChunk chunk) {
        Objects.requireNonNull(chunk, "FileChunk must not be null");
        String failureReason = null;
        UUID tid = chunk.transferId();
        FileChunk firstToSchedule = null;

        synchronized (stateLock) {
            if (state != ReceiverState.RECEIVING || !chunk.transferId().equals(this.transferId)) {
                return;
            }

            // Sequence validation
            int nextUnreserved = expectedNextSequence + (writePending ? 1 : 0) + queuedChunks.size();
            if (chunk.sequence() < nextUnreserved) {
                // Duplicate chunk; drop without mutating state or corrupting file
                return;
            }

            if (chunk.sequence() > nextUnreserved) {
                // Out-of-order chunk! Sequence violation
                failureReason = "Invalid chunk sequence: expected " + nextUnreserved + " but got " + chunk.sequence();
            } else {
                byte[] data = chunk.data();
                // Chunk size validation
                if (data.length < 1 || data.length > ProtocolLimits.MAX_CHUNK_BYTES) {
                    failureReason = "Invalid chunk size: " + data.length;
                } else if (bytesReceived + pendingBytes + data.length > declaredSizeBytes) {
                    failureReason = "Transfer size exceeded declared bounds";
                } else {
                    // Protocol requirement: non-final chunk must be exactly 16,384 bytes
                    boolean isFinalChunk = (bytesReceived + pendingBytes + data.length == declaredSizeBytes);
                    if (!isFinalChunk && data.length != ProtocolLimits.MAX_CHUNK_BYTES) {
                        failureReason = "Non-final chunk size was " + data.length + " bytes (expected exactly " + ProtocolLimits.MAX_CHUNK_BYTES + ")";
                    } else if (queuedChunks.size() >= MAX_QUEUED_CHUNKS) {
                        failureReason = "Receiver pending chunk limit reached";
                    } else {
                        pendingBytes += data.length;
                        queuedChunks.addLast(new FileChunk(chunk.messageId(), tid, chunk.sequence(), data));
                        if (!writePending) {
                            writePending = true;
                            firstToSchedule = queuedChunks.removeFirst();
                        }
                    }
                }
            }
        }
        if (failureReason != null) { failTransfer(failureReason); return; }
        if (firstToSchedule != null) scheduleChunkWrite(firstToSchedule);
    }

    private void scheduleChunkWrite(FileChunk chunk) {
        try { workerPool.submit(() -> appendChunkOnWorker(chunk)); }
        catch (RuntimeException rejected) { failTransfer("Receiver worker capacity is unavailable"); }
    }

    private void appendChunkOnWorker(FileChunk chunk) {
        FileTransferProgress progress = null;
        MessageBody.FileComplete completion = null;
        FileChunk next = null;
        UUID tid = chunk.transferId();
        byte[] data = chunk.data();
        try {
            synchronized (ioLock) {
                synchronized (stateLock) {
                    if (state != ReceiverState.RECEIVING || !tid.equals(transferId)) return;
                }
                storage.appendChunk(tid, data);
            }
            synchronized (stateLock) {
                if (state != ReceiverState.RECEIVING || !tid.equals(transferId)) return;
                incrementalChecksum.update(data);
                expectedNextSequence++;
                chunksReceived++;
                bytesReceived += data.length;
                pendingBytes -= data.length;
                progress = new FileTransferProgress(tid, bytesReceived, declaredSizeBytes, chunk.sequence());
                if (!queuedChunks.isEmpty()) next = queuedChunks.removeFirst();
                else {
                    writePending = false;
                    completion = deferredComplete;
                    deferredComplete = null;
                }
            }
        } catch (IOException | RuntimeException failure) {
            synchronized (stateLock) { writePending = false; }
            failTransfer("Disk append error");
            return;
        }
        Consumer<FileTransferProgress> listener = progressListener;
        if (listener != null && progress != null) try { listener.accept(progress); } catch (RuntimeException ignored) { }
        if (next != null) { scheduleChunkWrite(next); return; }
        if (completion != null) verifyAndFinalize(completion);
    }

    private void handleFileComplete(MessageEnvelope envelope) {
        if (!(envelope.body() instanceof MessageBody.FileComplete complete)) {
            return;
        }

        if (state != ReceiverState.RECEIVING || !complete.transferId().equals(this.transferId)) {
            return;
        }

        if (writePending) { deferredComplete = complete; return; }
        try { workerPool.submit(() -> verifyAndFinalize(complete)); }
        catch (RuntimeException rejected) { failTransfer("Receiver worker capacity is unavailable"); }
    }

    private void verifyAndFinalize(MessageBody.FileComplete complete) {
        String failureReason = null;
        UUID tid;
        String computedHash = null;
        Path partFile = null;
        Path destination = null;
        boolean emptyFile;
        synchronized (stateLock) {
            if (state != ReceiverState.RECEIVING || !complete.transferId().equals(transferId)) return;
            emptyFile = declaredSizeBytes == 0;
            if (complete.chunkCount() != chunksReceived) {
                failureReason = "Chunk count mismatch: expected " + complete.chunkCount() + " but received " + chunksReceived;
            } else if (bytesReceived != declaredSizeBytes) {
                failureReason = "Byte count mismatch: declared " + declaredSizeBytes + " but received " + bytesReceived;
            } else {
                computedHash = (declaredSizeBytes == 0)
                        ? TransferChecksum.EMPTY_SHA256
                        : incrementalChecksum.digestHex();
                if (!TransferChecksum.matches(complete.sha256(), computedHash)) {
                    failureReason = "Checksum mismatch: expected " + complete.sha256() + " but computed " + computedHash;
                } else {
                    this.finalSha256 = computedHash;
                    this.state = ReceiverState.AWAITING_RECEIVED_ACK;
                    tid = transferId;
                    try {
                        partFile = storage.resolvePartFile(tid);
                        destination = storage.resolveFinalFile(tid);
                        finalFile = destination;
                    } catch (RuntimeException invalidStorage) {
                        failureReason = "Finalization path validation failed";
                    }
                    if (failureReason == null) {
                        // Paths and the state are captured; filesystem operations happen outside stateLock.
                    }
                }
            }
            tid = transferId;
        }

        if (failureReason != null) {
            failTransfer(failureReason);
            return;
        }

        try {
            synchronized (ioLock) {
                synchronized (stateLock) {
                    if (state != ReceiverState.AWAITING_RECEIVED_ACK || !tid.equals(transferId)) return;
                }
                if (emptyFile && !Files.exists(partFile)) Files.createFile(destination);
                else Files.move(partFile, destination); // No replace option: collisions preserve existing bytes.
                boolean stillOwned;
                synchronized (stateLock) {
                    stillOwned = state == ReceiverState.AWAITING_RECEIVED_ACK && tid.equals(transferId);
                }
                if (!stillOwned) { Files.deleteIfExists(destination); return; }
                    synchronized (stateLock) { finalPublished = true; }
            }
        } catch (IOException | RuntimeException failure) {
            failTransfer("Finalization failed");
            return;
        }
        try {
            frameSink.accept(new MessageEnvelope(ProtocolLimits.VERSION, MessageType.FILE_RECEIVED,
                    UUID.randomUUID(), null, new MessageBody.FileReceived(tid, computedHash)));
        } catch (RuntimeException failure) { failTransfer("Unable to send recipient integrity confirmation"); }
    }

    private void handleAck(MessageEnvelope envelope) {
        if (!(envelope.body() instanceof MessageBody.Ack ack)) {
            return;
        }

        if (state == ReceiverState.AWAITING_RECEIVED_ACK && ack.operation() == MessageType.FILE_RECEIVED) {
            state = ReceiverState.COMPLETED;
            if (completionFuture != null) {
                completionFuture.complete(FileTransferResult.completed(transferId, finalFile, bytesReceived, finalSha256));
            }
        }
    }

    private void handleFileStatus(MessageEnvelope envelope) {
        if (!(envelope.body() instanceof MessageBody.FileStatus status)) {
            return;
        }

        if (transferId == null || !transferId.equals(status.transferId())) {
            return;
        }

        switch (status.state()) {
            case CANCELLED -> {
                cleanupPartialAsync(transferId);
                state = ReceiverState.CANCELLED;
                if (completionFuture != null) {
                    completionFuture.complete(FileTransferResult.cancelled(transferId, status.reason()));
                }
            }
            case FAILED -> {
                cleanupPartialAsync(transferId);
                state = ReceiverState.FAILED;
                if (completionFuture != null) {
                    completionFuture.complete(FileTransferResult.failed(transferId, status.reason()));
                }
            }
            default -> {
                // Ignore other statuses
            }
        }
    }

    private void handleError(MessageEnvelope envelope) {
        if (!(envelope.body() instanceof MessageBody.Error error)) {
            return;
        }

        if (transferId != null && transferId.equals(error.transferId())) {
            cleanupPartialAsync(transferId);
            state = ReceiverState.FAILED;
            if (completionFuture != null) {
                completionFuture.complete(FileTransferResult.failed(transferId, error.message()));
            }
        }
    }

    private void failTransfer(String message) {
        CompletableFuture<FileTransferResult> future = null;
        UUID tid;

        synchronized (stateLock) {
            tid = transferId;
            state = ReceiverState.FAILED;
            queuedChunks.clear();
            pendingBytes = 0;
            deferredComplete = null;
            future = completionFuture;
        }
        cleanupPartialAsync(tid);

        if (tid != null) {
            try {
                frameSink.accept(new MessageEnvelope(
                        ProtocolLimits.VERSION,
                        MessageType.FILE_CANCEL,
                        UUID.randomUUID(),
                        null,
                        new MessageBody.TransferReference(tid)
                ));
            } catch (Exception ignored) {
            }
        }

        if (future != null) {
            future.complete(FileTransferResult.failed(tid, message));
        }
    }

    /**
     * Cancels the active transfer.
     */
    public void cancel() {
        UUID tid;
        CompletableFuture<FileTransferResult> future = null;

        synchronized (stateLock) {
            if (!isActive()) {
                return;
            }

            tid = transferId;
            state = ReceiverState.CANCELLED;
            queuedChunks.clear();
            pendingBytes = 0;
            deferredComplete = null;
            future = completionFuture;
        }
        cleanupPartialAsync(tid);

        if (tid != null) {
            try {
                frameSink.accept(new MessageEnvelope(
                        ProtocolLimits.VERSION,
                        MessageType.FILE_CANCEL,
                        UUID.randomUUID(),
                        null,
                        new MessageBody.TransferReference(tid)
                ));
            } catch (Exception ignored) {
            }
        }

        if (future != null) {
            future.complete(FileTransferResult.cancelled(tid, "Cancelled by receiver"));
        }
    }

    @Override
    public void close() {
        cancel();
        if (ownsWorkerPool) {
            workerPool.close();
        }
    }

    private void cleanupPartialAsync(UUID tid) {
        if (tid == null) return;
        CompletableFuture<Void> completion = new CompletableFuture<>();
        synchronized (stateLock) { cleanupFuture = completion; }
        try {
            workerPool.submit(() -> {
                try {
                    synchronized (ioLock) {
                        for (int attempt = 0; attempt < 50 && storage.exists(tid); attempt++) {
                            if (storage.deletePartialFile(tid) || !storage.exists(tid)) break;
                            if (attempt < 49) {
                                try {
                                    Thread.sleep(10L);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        }
                        if (storage.exists(tid)) {
                            completion.completeExceptionally(new IOException("Persistent partial file deletion failure for " + tid));
                            return;
                        }
                        Path published = storage.resolveFinalFile(tid);
                        boolean deletePublished;
                        synchronized (stateLock) {
                            deletePublished = finalPublished;
                            finalPublished = false;
                        }
                        if (deletePublished) {
                            try {
                                Files.deleteIfExists(published);
                            } catch (IOException e) {
                                completion.completeExceptionally(e);
                                return;
                            }
                        }
                    }
                    completion.complete(null);
                } catch (RuntimeException failure) {
                    completion.completeExceptionally(failure);
                }
            });
        } catch (RuntimeException rejected) {
            completion.completeExceptionally(rejected);
        }
    }

    CompletableFuture<Void> cleanupCompletion() { synchronized (stateLock) { return cleanupFuture; } }
}
