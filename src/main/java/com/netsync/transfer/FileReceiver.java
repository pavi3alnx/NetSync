package com.netsync.transfer;

import com.netsync.common.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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

    private CompletableFuture<FileTransferResult> completionFuture;
    private Consumer<FileTransferProgress> progressListener;

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

        if (isActive()) {
            // Already handling a transfer; reject new offer per single-transfer-per-client rule
            sendReject(offered.transferId());
            return;
        }

        if (offered.sizeBytes() < 0 || offered.sizeBytes() > ProtocolLimits.MAX_FILE_BYTES) {
            sendReject(offered.transferId());
            return;
        }

        try {
            SafeFileStorage.validateSafeFileName(offered.fileName());
        } catch (IllegalArgumentException | SecurityException e) {
            sendReject(offered.transferId());
            return;
        }

        this.transferId = offered.transferId();
        this.senderClientId = offered.senderClientId();
        this.fileName = offered.fileName();
        this.declaredSizeBytes = offered.sizeBytes();
        this.finalFile = null;
        this.completionFuture = new CompletableFuture<>();
        this.state = ReceiverState.OFFERED;
    }

    /**
     * Accepts the pending offered transfer.
     *
     * @param transferId transfer UUID matching the offered transfer
     * @return completion future
     */
    public CompletableFuture<FileTransferResult> accept(UUID transferId) {
        Objects.requireNonNull(transferId, "Transfer ID must not be null");

        synchronized (stateLock) {
            if (state != ReceiverState.OFFERED || !transferId.equals(this.transferId)) {
                throw new IllegalStateException("No offered transfer with ID: " + transferId);
            }

            this.expectedNextSequence = 0;
            this.chunksReceived = 0;
            this.bytesReceived = 0;
            this.incrementalChecksum = new TransferChecksum.Incremental();

            // Clear any stale partial file
            storage.deletePartialFile(transferId);

            state = ReceiverState.RECEIVING;

            MessageEnvelope acceptEnvelope = new MessageEnvelope(
                    ProtocolLimits.VERSION,
                    MessageType.FILE_ACCEPT,
                    UUID.randomUUID(),
                    null,
                    new MessageBody.TransferReference(transferId)
            );
            frameSink.accept(acceptEnvelope);

            return completionFuture;
        }
    }

    /**
     * Rejects the pending offered transfer.
     *
     * @param transferId transfer UUID matching the offered transfer
     */
    public void reject(UUID transferId) {
        Objects.requireNonNull(transferId, "Transfer ID must not be null");

        synchronized (stateLock) {
            if (state != ReceiverState.OFFERED || !transferId.equals(this.transferId)) {
                throw new IllegalStateException("No offered transfer with ID: " + transferId);
            }

            sendReject(transferId);
            state = ReceiverState.REJECTED;

            if (completionFuture != null) {
                completionFuture.complete(FileTransferResult.rejected(transferId, "Rejected by receiver"));
            }
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

        synchronized (stateLock) {
            if (state != ReceiverState.RECEIVING || !chunk.transferId().equals(this.transferId)) {
                return;
            }

            // Sequence validation
            if (chunk.sequence() < expectedNextSequence) {
                // Duplicate chunk; drop without mutating state or corrupting file
                return;
            }

            if (chunk.sequence() > expectedNextSequence) {
                // Out-of-order chunk! Sequence violation
                failTransfer("Invalid chunk sequence: expected " + expectedNextSequence + " but got " + chunk.sequence());
                return;
            }

            byte[] data = chunk.data();

            // Chunk size validation
            if (data.length < 1 || data.length > ProtocolLimits.MAX_CHUNK_BYTES) {
                failTransfer("Invalid chunk size: " + data.length);
                return;
            }

            if (bytesReceived + data.length > declaredSizeBytes) {
                failTransfer("Transfer size exceeded declared bounds: " + (bytesReceived + data.length) + " > " + declaredSizeBytes);
                return;
            }

            // Protocol requirement: non-final chunk must be exactly 16,384 bytes
            boolean isFinalChunk = (bytesReceived + data.length == declaredSizeBytes);
            if (!isFinalChunk && data.length != ProtocolLimits.MAX_CHUNK_BYTES) {
                failTransfer("Non-final chunk size was " + data.length + " bytes (expected exactly " + ProtocolLimits.MAX_CHUNK_BYTES + ")");
                return;
            }

            try {
                storage.appendChunk(transferId, data);
                incrementalChecksum.update(data);
                expectedNextSequence++;
                chunksReceived++;
                bytesReceived += data.length;

                if (progressListener != null) {
                    try {
                        progressListener.accept(new FileTransferProgress(transferId, bytesReceived, declaredSizeBytes, chunk.sequence()));
                    } catch (Exception ignored) {
                    }
                }
            } catch (IOException | RuntimeException e) {
                failTransfer("Disk append error: " + e.getMessage());
            }
        }
    }

    private void handleFileComplete(MessageEnvelope envelope) {
        if (!(envelope.body() instanceof MessageBody.FileComplete complete)) {
            return;
        }

        if (state != ReceiverState.RECEIVING || !complete.transferId().equals(this.transferId)) {
            return;
        }

        workerPool.submit(() -> verifyAndFinalize(complete));
    }

    private void verifyAndFinalize(MessageBody.FileComplete complete) {
        synchronized (stateLock) {
            if (state != ReceiverState.RECEIVING) {
                return;
            }

            // Validate chunk count
            if (complete.chunkCount() != chunksReceived) {
                failTransfer("Chunk count mismatch: expected " + complete.chunkCount() + " but received " + chunksReceived);
                return;
            }

            // Validate byte count
            if (bytesReceived != declaredSizeBytes) {
                failTransfer("Byte count mismatch: declared " + declaredSizeBytes + " but received " + bytesReceived);
                return;
            }

            // Validate SHA-256
            String computedHash = (declaredSizeBytes == 0)
                    ? TransferChecksum.EMPTY_SHA256
                    : incrementalChecksum.digestHex();

            if (!TransferChecksum.matches(complete.sha256(), computedHash)) {
                failTransfer("Checksum mismatch: expected " + complete.sha256() + " but computed " + computedHash);
                return;
            }

            this.finalSha256 = computedHash;

            // Finalize file
            try {
                Path partFile = storage.resolvePartFile(transferId);
                finalFile = storage.rootDir().resolve(transferId + ".bin");

                if (declaredSizeBytes == 0) {
                    if (Files.exists(partFile)) {
                        Files.move(partFile, finalFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                    } else {
                        Files.createFile(finalFile);
                    }
                } else {
                    Files.move(partFile, finalFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                }

                state = ReceiverState.AWAITING_RECEIVED_ACK;

                MessageEnvelope receivedEnvelope = new MessageEnvelope(
                        ProtocolLimits.VERSION,
                        MessageType.FILE_RECEIVED,
                        UUID.randomUUID(),
                        null,
                        new MessageBody.FileReceived(transferId, finalSha256)
                );
                frameSink.accept(receivedEnvelope);
            } catch (IOException e) {
                failTransfer("Finalization I/O error: " + e.getMessage());
            }
        }
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
                storage.deletePartialFile(transferId);
                state = ReceiverState.CANCELLED;
                if (completionFuture != null) {
                    completionFuture.complete(FileTransferResult.cancelled(transferId, status.reason()));
                }
            }
            case FAILED -> {
                storage.deletePartialFile(transferId);
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
            storage.deletePartialFile(transferId);
            state = ReceiverState.FAILED;
            if (completionFuture != null) {
                completionFuture.complete(FileTransferResult.failed(transferId, error.message()));
            }
        }
    }

    private void failTransfer(String message) {
        storage.deletePartialFile(transferId);
        state = ReceiverState.FAILED;

        if (transferId != null) {
            try {
                frameSink.accept(new MessageEnvelope(
                        ProtocolLimits.VERSION,
                        MessageType.FILE_CANCEL,
                        UUID.randomUUID(),
                        null,
                        new MessageBody.TransferReference(transferId)
                ));
            } catch (Exception ignored) {
            }
        }

        if (completionFuture != null) {
            completionFuture.complete(FileTransferResult.failed(transferId, message));
        }
    }

    /**
     * Cancels the active transfer.
     */
    public void cancel() {
        synchronized (stateLock) {
            if (!isActive()) {
                return;
            }

            UUID tid = transferId;
            storage.deletePartialFile(tid);
            state = ReceiverState.CANCELLED;

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

            if (completionFuture != null) {
                completionFuture.complete(FileTransferResult.cancelled(tid, "Cancelled by receiver"));
            }
        }
    }

    @Override
    public void close() {
        cancel();
        if (ownsWorkerPool) {
            workerPool.close();
        }
    }
}
