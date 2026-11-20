package com.netsync.transfer;

import com.netsync.common.*;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Client-side file transfer service that coordinates {@link FileSender} and {@link FileReceiver}.
 * <p>
 * Enforces the single-active-transfer-per-client protocol invariant, dispatches outbound frames
 * via injected sink, demultiplexes incoming control messages and binary chunks, manages safe local
 * storage, and notifies registered listeners of transfer events without blocking network loops.
 */
public final class FileTransferClientService implements Closeable {

    /**
     * Listener interface for UI and CLI integration.
     */
    public interface FileTransferEventListener {
        default void onOfferReceived(UUID transferId, UUID senderClientId, String fileName, long sizeBytes) {}
        default void onProgress(FileTransferProgress progress) {}
        default void onCompleted(FileTransferResult result) {}
        default void onFailed(UUID transferId, String reason) {}
        default void onCancelled(UUID transferId, String reason) {}
        default void onRejected(UUID transferId, String reason) {}
    }

    private final Consumer<OutboundFrame> frameSink;
    private final Supplier<Path> downloadDirSupplier;
    private final TransferWorkerPool workerPool;
    private final boolean ownsWorkerPool;

    private final Object lock = new Object();
    private volatile FileTransferEventListener eventListener;
    private FileSender activeSender;
    private FileReceiver activeReceiver;
    private boolean closed;

    /**
     * Creates a new client transfer service with an internally owned bounded worker pool.
     *
     * @param frameSink           sink for emitting outbound network frames
     * @param downloadDirSupplier supplier for resolving destination directory for downloads
     */
    public FileTransferClientService(
            Consumer<OutboundFrame> frameSink,
            Supplier<Path> downloadDirSupplier
    ) {
        this(frameSink, downloadDirSupplier, new TransferWorkerPool(ProtocolLimits.WORKER_THREADS, ProtocolLimits.WORKER_QUEUE_SIZE), true);
    }

    /**
     * Creates a new client transfer service sharing an externally managed worker pool.
     *
     * @param frameSink           sink for emitting outbound network frames
     * @param downloadDirSupplier supplier for resolving destination directory for downloads
     * @param workerPool          shared worker pool
     */
    public FileTransferClientService(
            Consumer<OutboundFrame> frameSink,
            Supplier<Path> downloadDirSupplier,
            TransferWorkerPool workerPool
    ) {
        this(frameSink, downloadDirSupplier, workerPool, false);
    }

    private FileTransferClientService(
            Consumer<OutboundFrame> frameSink,
            Supplier<Path> downloadDirSupplier,
            TransferWorkerPool workerPool,
            boolean ownsWorkerPool
    ) {
        this.frameSink = Objects.requireNonNull(frameSink, "Frame sink must not be null");
        this.downloadDirSupplier = Objects.requireNonNull(downloadDirSupplier, "Download directory supplier must not be null");
        this.workerPool = Objects.requireNonNull(workerPool, "Worker pool must not be null");
        this.ownsWorkerPool = ownsWorkerPool;
    }

    /**
     * Sets the listener for file transfer events and progress updates.
     *
     * @param listener event listener
     */
    public void setEventListener(FileTransferEventListener listener) {
        this.eventListener = listener;
    }

    /**
     * Initiates an outgoing file upload to the specified recipient.
     *
     * @param recipientClientId recipient client UUID
     * @param sourceFile        local path of file to upload
     * @return completion future containing the final {@link FileTransferResult}
     * @throws IllegalStateException    if a transfer is already active or service is closed
     * @throws IllegalArgumentException if file validation fails
     */
    public CompletableFuture<FileTransferResult> sendFile(UUID recipientClientId, Path sourceFile) {
        Objects.requireNonNull(recipientClientId, "Recipient client ID must not be null");
        Objects.requireNonNull(sourceFile, "Source file must not be null");

        FileSender sender;
        synchronized (lock) {
            checkNotClosed();
            if (isTransferActiveLocked()) {
                throw new IllegalStateException("A transfer is already active: " + activeTransferIdLocked().orElse(null));
            }

            sender = new FileSender(frameSink, workerPool);
            sender.setProgressListener(progress -> {
                FileTransferEventListener listener = this.eventListener;
                if (listener != null) {
                    try {
                        listener.onProgress(progress);
                    } catch (Exception ignored) {
                    }
                }
            });
            this.activeSender = sender;
        }

        CompletableFuture<FileTransferResult> future;
        try {
            future = sender.startTransfer(recipientClientId, sourceFile);
        } catch (RuntimeException e) {
            synchronized (lock) {
                if (this.activeSender == sender) {
                    this.activeSender = null;
                }
            }
            throw e;
        }

        future.whenComplete((result, ex) -> {
            FileTransferEventListener listener;
            synchronized (lock) {
                if (this.activeSender == sender) {
                    this.activeSender = null;
                }
                listener = this.eventListener;
            }

            if (listener != null) {
                try {
                    if (result != null) {
                        if (result.success()) {
                            listener.onCompleted(result);
                        } else if (result.message() != null && result.message().toLowerCase().contains("reject")) {
                            listener.onRejected(result.transferId(), result.message());
                        } else if (result.message() != null && result.message().toLowerCase().contains("cancel")) {
                            listener.onCancelled(result.transferId(), result.message());
                        } else {
                            listener.onFailed(result.transferId(), result.message());
                        }
                    } else if (ex != null) {
                        listener.onFailed(sender.transferId(), ex.getMessage());
                    }
                } catch (Exception ignored) {
                }
            }
        });

        return future;
    }

    /**
     * Accepts a pending incoming file offer.
     *
     * @param transferId transfer UUID
     * @return completion future containing the final {@link FileTransferResult}
     * @throws IllegalStateException if no offer is currently pending with the given ID
     */
    public CompletableFuture<FileTransferResult> acceptOffer(UUID transferId) {
        Objects.requireNonNull(transferId, "Transfer ID must not be null");

        FileReceiver receiver;
        synchronized (lock) {
            checkNotClosed();
            if (activeReceiver == null || activeReceiver.state() != FileReceiver.ReceiverState.OFFERED
                    || !transferId.equals(activeReceiver.transferId())) {
                throw new IllegalStateException("No pending offer with transfer ID: " + transferId);
            }
            receiver = activeReceiver;
        }

        CompletableFuture<FileTransferResult> future;
        try {
            future = receiver.accept(transferId);
        } catch (RuntimeException e) {
            synchronized (lock) {
                if (this.activeReceiver == receiver) {
                    this.activeReceiver = null;
                }
            }
            throw e;
        }

        future.whenComplete((result, ex) -> {
            FileTransferEventListener listener;
            synchronized (lock) {
                if (this.activeReceiver == receiver) {
                    this.activeReceiver = null;
                }
                listener = this.eventListener;
            }

            if (listener != null) {
                try {
                    if (result != null) {
                        if (result.success()) {
                            listener.onCompleted(result);
                        } else if (result.message() != null && result.message().toLowerCase().contains("reject")) {
                            listener.onRejected(result.transferId(), result.message());
                        } else if (result.message() != null && result.message().toLowerCase().contains("cancel")) {
                            listener.onCancelled(result.transferId(), result.message());
                        } else {
                            listener.onFailed(result.transferId(), result.message());
                        }
                    } else if (ex != null) {
                        listener.onFailed(transferId, ex.getMessage());
                    }
                } catch (Exception ignored) {
                }
            }
        });

        return future;
    }

    /**
     * Rejects a pending incoming file offer.
     *
     * @param transferId transfer UUID
     * @throws IllegalStateException if no offer is currently pending with the given ID
     */
    public void rejectOffer(UUID transferId) {
        Objects.requireNonNull(transferId, "Transfer ID must not be null");

        FileReceiver receiver;
        FileTransferEventListener listener;
        synchronized (lock) {
            checkNotClosed();
            if (activeReceiver == null || activeReceiver.state() != FileReceiver.ReceiverState.OFFERED
                    || !transferId.equals(activeReceiver.transferId())) {
                throw new IllegalStateException("No pending offer with transfer ID: " + transferId);
            }
            receiver = activeReceiver;
            this.activeReceiver = null;
            listener = this.eventListener;
        }

        receiver.reject(transferId);
        if (listener != null) {
            try {
                listener.onRejected(transferId, "Rejected by user");
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Demultiplexes and handles an incoming control message envelope.
     *
     * @param envelope incoming message envelope
     */
    public void handleControl(MessageEnvelope envelope) {
        Objects.requireNonNull(envelope, "Message envelope must not be null");

        OutboundFrame rejectFrameToSend = null;
        UUID rejectOfferId = null;
        FileTransferEventListener offerListener = null;
        MessageBody.FileOffered offerDetails = null;

        FileSender targetSender = null;
        FileReceiver targetReceiver = null;
        boolean cancelTargetSender = false;
        boolean cancelTargetReceiver = false;

        synchronized (lock) {
            if (closed) {
                return;
            }

            switch (envelope.type()) {
                case FILE_OFFERED -> {
                    if (!(envelope.body() instanceof MessageBody.FileOffered offered)) {
                        return;
                    }
                    if (isTransferActiveLocked()) {
                        // Busy: reject offer immediately per single-transfer rule
                        rejectOfferId = offered.transferId();
                        rejectFrameToSend = new MessageEnvelope(
                                ProtocolLimits.VERSION,
                                MessageType.FILE_REJECT,
                                UUID.randomUUID(),
                                null,
                                new MessageBody.TransferReference(offered.transferId())
                        );
                    } else {
                        Path downloadDir = downloadDirSupplier.get();
                        if (downloadDir == null) {
                            rejectOfferId = offered.transferId();
                            rejectFrameToSend = new MessageEnvelope(
                                    ProtocolLimits.VERSION,
                                    MessageType.FILE_REJECT,
                                    UUID.randomUUID(),
                                    null,
                                    new MessageBody.TransferReference(offered.transferId())
                            );
                        } else {
                            SafeFileStorage storage = new SafeFileStorage(downloadDir);
                            FileReceiver receiver = new FileReceiver(frameSink, storage, workerPool);
                            receiver.setProgressListener(progress -> {
                                FileTransferEventListener l = this.eventListener;
                                if (l != null) {
                                    try {
                                        l.onProgress(progress);
                                    } catch (Exception ignored) {
                                    }
                                }
                            });
                            receiver.handleControl(envelope);
                            if (receiver.state() == FileReceiver.ReceiverState.OFFERED) {
                                this.activeReceiver = receiver;
                                offerListener = this.eventListener;
                                offerDetails = offered;
                            }
                        }
                    }
                }
                case FILE_COMPLETE -> {
                    if (envelope.body() instanceof MessageBody.FileComplete complete) {
                        if (activeReceiver != null && complete.transferId().equals(activeReceiver.transferId())) {
                            targetReceiver = activeReceiver;
                        }
                    }
                }
                case ACK -> {
                    if (envelope.body() instanceof MessageBody.Ack ack) {
                        if (ack.operation() == MessageType.FILE_OFFER
                                || ack.operation() == MessageType.FILE_CHUNK
                                || ack.operation() == MessageType.FILE_COMPLETE) {
                            if (activeSender != null) {
                                if (ack.transferId() == null || activeSender.transferId() == null
                                        || ack.transferId().equals(activeSender.transferId())) {
                                    targetSender = activeSender;
                                }
                            }
                        } else if (ack.operation() == MessageType.FILE_RECEIVED) {
                            if (activeReceiver != null) {
                                if (ack.transferId() == null || activeReceiver.transferId() == null
                                        || ack.transferId().equals(activeReceiver.transferId())) {
                                    targetReceiver = activeReceiver;
                                }
                            }
                        }
                    }
                }
                case FILE_STATUS -> {
                    if (envelope.body() instanceof MessageBody.FileStatus status) {
                        if (activeSender != null && status.transferId().equals(activeSender.transferId())) {
                            targetSender = activeSender;
                        } else if (activeReceiver != null && status.transferId().equals(activeReceiver.transferId())) {
                            targetReceiver = activeReceiver;
                        }
                    }
                }
                case ERROR -> {
                    if (envelope.body() instanceof MessageBody.Error error) {
                        if (activeSender != null) {
                            if (error.transferId() == null || activeSender.transferId() == null
                                    || error.transferId().equals(activeSender.transferId())) {
                                targetSender = activeSender;
                            }
                        }
                        if (activeReceiver != null && error.transferId() != null
                                && error.transferId().equals(activeReceiver.transferId())) {
                            targetReceiver = activeReceiver;
                        }
                    }
                }
                case FILE_CANCEL -> {
                    if (envelope.body() instanceof MessageBody.TransferReference ref) {
                        if (activeSender != null && ref.transferId().equals(activeSender.transferId())) {
                            targetSender = activeSender;
                            cancelTargetSender = true;
                        }
                        if (activeReceiver != null && ref.transferId().equals(activeReceiver.transferId())) {
                            targetReceiver = activeReceiver;
                            cancelTargetReceiver = true;
                        }
                    }
                }
                default -> {
                    // Ignore non-transfer control envelopes
                }
            }
        }

        // Invoke callbacks and emit frames strictly outside the lock
        if (rejectFrameToSend != null) {
            frameSink.accept(rejectFrameToSend);
            FileTransferEventListener listener = this.eventListener;
            if (listener != null) {
                try {
                    listener.onRejected(rejectOfferId, "Transfer offer rejected: client is busy with another transfer");
                } catch (Exception ignored) {
                }
            }
        }

        if (offerListener != null && offerDetails != null) {
            try {
                offerListener.onOfferReceived(offerDetails.transferId(), offerDetails.senderClientId(), offerDetails.fileName(), offerDetails.sizeBytes());
            } catch (Exception ignored) {
            }
        }

        if (targetSender != null) {
            if (cancelTargetSender) {
                targetSender.cancel();
            } else {
                targetSender.handleControl(envelope);
            }
            if (!targetSender.isActive()) {
                synchronized (lock) {
                    if (this.activeSender == targetSender) {
                        this.activeSender = null;
                    }
                }
            }
        }

        if (targetReceiver != null) {
            boolean wasOffered = (targetReceiver.state() == FileReceiver.ReceiverState.OFFERED);
            if (cancelTargetReceiver) {
                targetReceiver.cancel();
            } else {
                targetReceiver.handleControl(envelope);
            }
            if (!targetReceiver.isActive()) {
                boolean cleared = false;
                synchronized (lock) {
                    if (this.activeReceiver == targetReceiver) {
                        this.activeReceiver = null;
                        cleared = true;
                    }
                }
                if (cleared && wasOffered) {
                    FileTransferEventListener listener = this.eventListener;
                    if (listener != null) {
                        try {
                            if (targetReceiver.state() == FileReceiver.ReceiverState.CANCELLED) {
                                listener.onCancelled(targetReceiver.transferId(), "Offer cancelled by sender");
                            } else if (targetReceiver.state() == FileReceiver.ReceiverState.FAILED) {
                                listener.onFailed(targetReceiver.transferId(), "Offer failed");
                            }
                        } catch (Exception ignored) {
                        }
                    }
                }
            }
        }
    }

    /**
     * Demultiplexes and handles an incoming binary file chunk frame.
     *
     * @param chunk incoming binary file chunk
     */
    public void handleChunk(FileChunk chunk) {
        Objects.requireNonNull(chunk, "FileChunk must not be null");

        FileReceiver receiver = null;
        synchronized (lock) {
            if (closed) {
                return;
            }
            if (activeReceiver != null && chunk.transferId().equals(activeReceiver.transferId())) {
                receiver = activeReceiver;
            }
        }

        if (receiver != null) {
            receiver.handleChunk(chunk);
        }
    }

    /**
     * Cancels any active transfer (upload or download).
     */
    public void cancelActiveTransfer() {
        FileSender sender;
        FileReceiver receiver;
        boolean receiverWasOffered;
        synchronized (lock) {
            sender = activeSender;
            receiver = activeReceiver;
            receiverWasOffered = (activeReceiver != null && activeReceiver.state() == FileReceiver.ReceiverState.OFFERED);
            activeSender = null;
            activeReceiver = null;
        }

        if (sender != null) {
            sender.cancel();
        }
        if (receiver != null) {
            receiver.cancel();
            if (receiverWasOffered) {
                FileTransferEventListener listener = this.eventListener;
                if (listener != null) {
                    try {
                        listener.onCancelled(receiver.transferId(), "Offer cancelled");
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    /**
     * Cancels a specific transfer by its transfer UUID.
     *
     * @param transferId transfer UUID
     */
    public void cancelTransfer(UUID transferId) {
        if (transferId == null) {
            return;
        }
        FileSender sender = null;
        FileReceiver receiver = null;
        boolean receiverWasOffered = false;
        synchronized (lock) {
            if (activeSender != null && transferId.equals(activeSender.transferId())) {
                sender = activeSender;
                activeSender = null;
            }
            if (activeReceiver != null && transferId.equals(activeReceiver.transferId())) {
                receiver = activeReceiver;
                receiverWasOffered = (activeReceiver.state() == FileReceiver.ReceiverState.OFFERED);
                activeReceiver = null;
            }
        }

        if (sender != null) {
            sender.cancel();
        }
        if (receiver != null) {
            receiver.cancel();
            if (receiverWasOffered) {
                FileTransferEventListener listener = this.eventListener;
                if (listener != null) {
                    try {
                        listener.onCancelled(transferId, "Offer cancelled");
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    /**
     * Invoked when the client TCP connection disconnects. Cancels active transfers and cleans up partial files.
     */
    public void onDisconnect() {
        FileSender sender;
        FileReceiver receiver;
        synchronized (lock) {
            sender = activeSender;
            receiver = activeReceiver;
            activeSender = null;
            activeReceiver = null;
        }

        if (sender != null) {
            try {
                sender.cancel();
            } catch (Exception ignored) {
            }
        }
        if (receiver != null) {
            try {
                receiver.cancel();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Checks whether an upload or download is currently active (including pending offers).
     */
    public boolean isTransferActive() {
        synchronized (lock) {
            return isTransferActiveLocked();
        }
    }

    private boolean isTransferActiveLocked() {
        return (activeSender != null && activeSender.isActive())
                || (activeReceiver != null && activeReceiver.isActive());
    }

    /**
     * Returns the active transfer ID, if one exists.
     */
    public Optional<UUID> activeTransferId() {
        synchronized (lock) {
            return activeTransferIdLocked();
        }
    }

    private Optional<UUID> activeTransferIdLocked() {
        if (activeSender != null && activeSender.isActive()) {
            return Optional.ofNullable(activeSender.transferId());
        }
        if (activeReceiver != null && activeReceiver.isActive()) {
            return Optional.ofNullable(activeReceiver.transferId());
        }
        return Optional.empty();
    }

    public Optional<FileSender.SenderState> senderState() {
        synchronized (lock) {
            return Optional.ofNullable(activeSender).map(FileSender::state);
        }
    }

    public Optional<FileReceiver.ReceiverState> receiverState() {
        synchronized (lock) {
            return Optional.ofNullable(activeReceiver).map(FileReceiver::state);
        }
    }

    public boolean isClosed() {
        synchronized (lock) {
            return closed;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("FileTransferClientService is closed");
        }
    }

    @Override
    public void close() {
        FileSender sender;
        FileReceiver receiver;
        boolean closePool;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            sender = activeSender;
            receiver = activeReceiver;
            activeSender = null;
            activeReceiver = null;
            closePool = ownsWorkerPool;
        }

        if (sender != null) {
            try {
                sender.close();
            } catch (Exception ignored) {
            }
        }
        if (receiver != null) {
            try {
                receiver.close();
            } catch (Exception ignored) {
            }
        }
        if (closePool) {
            workerPool.close();
        }
    }
}
