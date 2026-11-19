package com.netsync.transfer;

import com.netsync.common.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Client-side file sender implementing the stop-and-wait chunk protocol.
 * <p>
 * Reads a local source file in bounded chunks (at most 16 KiB each), computes incremental
 * SHA-256 checksums, and coordinates with the server through {@link MessageEnvelope}
 * controls and binary {@link FileChunk} frames.
 */
public final class FileSender implements AutoCloseable {

    public enum SenderState {
        IDLE,
        OFFERING,
        AWAITING_ACCEPTANCE,
        SENDING_CHUNKS,
        AWAITING_CHUNK_ACK,
        AWAITING_COMPLETE_ACK,
        AWAITING_FINAL_STATUS,
        COMPLETED,
        REJECTED,
        CANCELLED,
        FAILED
    }

    private final Consumer<OutboundFrame> frameSink;
    private final TransferWorkerPool workerPool;
    private final boolean ownsWorkerPool;

    private final Object stateLock = new Object();
    private volatile SenderState state = SenderState.IDLE;

    private Path sourceFile;
    private long fileSizeBytes;
    private UUID recipientClientId;
    private UUID transferId;
    private UUID offerMessageId;
    private UUID pendingChunkMessageId;
    private UUID completeMessageId;

    private InputStream inputStream;
    private TransferChecksum.Incremental incrementalChecksum;
    private int chunkSequence;
    private long bytesSent;
    private String finalSha256;

    private CompletableFuture<FileTransferResult> completionFuture;
    private Consumer<FileTransferProgress> progressListener;

    public FileSender(Consumer<OutboundFrame> frameSink) {
        this(frameSink, new TransferWorkerPool(2, 32), true);
    }

    public FileSender(Consumer<OutboundFrame> frameSink, TransferWorkerPool workerPool) {
        this(frameSink, workerPool, false);
    }

    private FileSender(Consumer<OutboundFrame> frameSink, TransferWorkerPool workerPool, boolean ownsWorkerPool) {
        this.frameSink = Objects.requireNonNull(frameSink, "Frame sink must not be null");
        this.workerPool = Objects.requireNonNull(workerPool, "Worker pool must not be null");
        this.ownsWorkerPool = ownsWorkerPool;
    }

    public void setProgressListener(Consumer<FileTransferProgress> listener) {
        this.progressListener = listener;
    }

    public SenderState state() {
        return state;
    }

    public UUID transferId() {
        return transferId;
    }

    public boolean isActive() {
        return state == SenderState.OFFERING
                || state == SenderState.AWAITING_ACCEPTANCE
                || state == SenderState.SENDING_CHUNKS
                || state == SenderState.AWAITING_CHUNK_ACK
                || state == SenderState.AWAITING_COMPLETE_ACK
                || state == SenderState.AWAITING_FINAL_STATUS;
    }

    /**
     * Initiates a file transfer offer to the specified recipient.
     *
     * @param recipientClientId recipient client UUID
     * @param sourceFile        path to source file to send
     * @return a future that completes with the final {@link FileTransferResult}
     */
    public CompletableFuture<FileTransferResult> startTransfer(UUID recipientClientId, Path sourceFile) {
        Objects.requireNonNull(recipientClientId, "Recipient client ID must not be null");
        Objects.requireNonNull(sourceFile, "Source file must not be null");

        synchronized (stateLock) {
            if (isActive()) {
                throw new IllegalStateException("A transfer is already active for this sender: " + transferId);
            }

            if (!Files.exists(sourceFile) || Files.isDirectory(sourceFile) || !Files.isReadable(sourceFile)) {
                throw new IllegalArgumentException("Source file does not exist, is a directory, or is not readable: " + sourceFile);
            }

            long size;
            try {
                size = Files.size(sourceFile);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to read file size: " + sourceFile, e);
            }

            if (size > ProtocolLimits.MAX_FILE_BYTES) {
                throw new IllegalArgumentException("File size exceeds maximum protocol limit ("
                        + ProtocolLimits.MAX_FILE_BYTES + " bytes): " + size);
            }

            String fileName = sourceFile.getFileName() != null ? sourceFile.getFileName().toString() : "file";
            SafeFileStorage.validateSafeFileName(fileName);

            this.sourceFile = sourceFile;
            this.fileSizeBytes = size;
            this.recipientClientId = recipientClientId;
            this.chunkSequence = 0;
            this.bytesSent = 0;
            this.finalSha256 = null;
            this.pendingChunkMessageId = null;
            this.completeMessageId = null;
            this.completionFuture = new CompletableFuture<>();

            this.offerMessageId = UUID.randomUUID();
            this.state = SenderState.OFFERING;

            MessageEnvelope offerEnvelope = new MessageEnvelope(
                    ProtocolLimits.VERSION,
                    MessageType.FILE_OFFER,
                    offerMessageId,
                    null,
                    new MessageBody.FileOffer(recipientClientId, fileName, size)
            );
            frameSink.accept(offerEnvelope);

            return completionFuture;
        }
    }

    /**
     * Handles incoming control message envelopes from the server.
     *
     * @param envelope message envelope
     */
    public void handleControl(MessageEnvelope envelope) {
        Objects.requireNonNull(envelope, "MessageEnvelope must not be null");

        synchronized (stateLock) {
            if (!isActive()) {
                return;
            }

            switch (envelope.type()) {
                case ACK -> handleAck(envelope);
                case FILE_STATUS -> handleFileStatus(envelope);
                case ERROR -> handleError(envelope);
                default -> {
                    // Ignore unrelated message types
                }
            }
        }
    }

    private void handleAck(MessageEnvelope envelope) {
        if (!(envelope.body() instanceof MessageBody.Ack ack)) {
            return;
        }

        switch (ack.operation()) {
            case FILE_OFFER -> {
                if (state == SenderState.OFFERING && envelope.replyTo() != null && envelope.replyTo().equals(offerMessageId)) {
                    this.transferId = ack.transferId();
                    this.state = SenderState.AWAITING_ACCEPTANCE;
                }
            }
            case FILE_CHUNK -> {
                if (state == SenderState.AWAITING_CHUNK_ACK && ack.status() == AckStatus.STORED) {
                    if (envelope.replyTo() != null && envelope.replyTo().equals(pendingChunkMessageId)) {
                        state = SenderState.SENDING_CHUNKS;
                        workerPool.submit(this::readAndSendNextChunk);
                    }
                }
            }
            case FILE_COMPLETE -> {
                if (state == SenderState.AWAITING_COMPLETE_ACK && ack.status() == AckStatus.ACCEPTED) {
                    state = SenderState.AWAITING_FINAL_STATUS;
                }
            }
            default -> {
                // Ignore other ACKs
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
            case ACCEPTED -> {
                if (state == SenderState.AWAITING_ACCEPTANCE) {
                    state = SenderState.SENDING_CHUNKS;
                    workerPool.submit(this::readAndSendNextChunk);
                }
            }
            case REJECTED -> {
                cleanupStreams();
                state = SenderState.REJECTED;
                if (completionFuture != null) {
                    completionFuture.complete(FileTransferResult.rejected(transferId, status.reason()));
                }
            }
            case COMPLETED -> {
                cleanupStreams();
                state = SenderState.COMPLETED;
                if (completionFuture != null) {
                    completionFuture.complete(FileTransferResult.completed(transferId, sourceFile, fileSizeBytes, finalSha256));
                }
            }
            case CANCELLED -> {
                cleanupStreams();
                state = SenderState.CANCELLED;
                if (completionFuture != null) {
                    completionFuture.complete(FileTransferResult.cancelled(transferId, status.reason()));
                }
            }
            case FAILED -> {
                cleanupStreams();
                state = SenderState.FAILED;
                if (completionFuture != null) {
                    completionFuture.complete(FileTransferResult.failed(transferId, status.reason()));
                }
            }
        }
    }

    private void handleError(MessageEnvelope envelope) {
        if (!(envelope.body() instanceof MessageBody.Error error)) {
            return;
        }

        boolean matches = (offerMessageId != null && offerMessageId.equals(envelope.replyTo()))
                || (pendingChunkMessageId != null && pendingChunkMessageId.equals(envelope.replyTo()))
                || (completeMessageId != null && completeMessageId.equals(envelope.replyTo()))
                || (transferId != null && transferId.equals(error.transferId()));

        if (matches) {
            cleanupStreams();
            state = SenderState.FAILED;
            if (completionFuture != null) {
                completionFuture.complete(FileTransferResult.failed(transferId, error.message()));
            }
        }
    }

    /**
     * Reads the next chunk from disk and emits it to the frame sink.
     * Executed strictly on worker pool threads.
     */
    private void readAndSendNextChunk() {
        synchronized (stateLock) {
            if (state != SenderState.SENDING_CHUNKS) {
                return;
            }

            try {
                if (fileSizeBytes == 0) {
                    // Empty file handling: 0 chunks, SHA-256 of empty bytes
                    finalSha256 = TransferChecksum.EMPTY_SHA256;
                    completeMessageId = UUID.randomUUID();
                    MessageEnvelope completeEnvelope = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_COMPLETE,
                            completeMessageId,
                            null,
                            new MessageBody.FileComplete(transferId, 0, finalSha256)
                    );
                    state = SenderState.AWAITING_COMPLETE_ACK;
                    frameSink.accept(completeEnvelope);
                    return;
                }

                if (inputStream == null) {
                    inputStream = Files.newInputStream(sourceFile);
                    incrementalChecksum = new TransferChecksum.Incremental();
                    chunkSequence = 0;
                    bytesSent = 0;
                }

                byte[] buffer = new byte[ProtocolLimits.MAX_CHUNK_BYTES];
                int bytesRead = inputStream.read(buffer);

                if (bytesRead > 0) {
                    byte[] chunkData = (bytesRead == buffer.length) ? buffer : Arrays.copyOf(buffer, bytesRead);
                    incrementalChecksum.update(chunkData);

                    pendingChunkMessageId = UUID.randomUUID();
                    FileChunk chunk = new FileChunk(pendingChunkMessageId, transferId, chunkSequence, chunkData);

                    chunkSequence++;
                    bytesSent += bytesRead;
                    state = SenderState.AWAITING_CHUNK_ACK;

                    frameSink.accept(chunk);

                    if (progressListener != null) {
                        try {
                            progressListener.accept(new FileTransferProgress(transferId, bytesSent, fileSizeBytes, chunkSequence - 1));
                        } catch (Exception ignored) {
                        }
                    }
                } else {
                    // EOF reached
                    cleanupStreams();
                    finalSha256 = incrementalChecksum.digestHex();
                    completeMessageId = UUID.randomUUID();
                    MessageEnvelope completeEnvelope = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_COMPLETE,
                            completeMessageId,
                            null,
                            new MessageBody.FileComplete(transferId, chunkSequence, finalSha256)
                    );
                    state = SenderState.AWAITING_COMPLETE_ACK;
                    frameSink.accept(completeEnvelope);
                }
            } catch (IOException | RuntimeException e) {
                cleanupStreams();
                state = SenderState.FAILED;
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
                    completionFuture.complete(FileTransferResult.failed(transferId, "Read error: " + e.getMessage()));
                }
            }
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

            cleanupStreams();
            UUID tid = transferId;
            state = SenderState.CANCELLED;

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
                completionFuture.complete(FileTransferResult.cancelled(tid, "Cancelled by sender"));
            }
        }
    }

    private void cleanupStreams() {
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException ignored) {
            }
            inputStream = null;
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
