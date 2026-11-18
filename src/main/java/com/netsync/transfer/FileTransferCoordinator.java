package com.netsync.transfer;

import com.netsync.common.*;

import java.util.Objects;
import java.util.UUID;

/**
 * Top-level File Transfer Coordinator implementing the shared {@link TransferCoordinator}
 * contract for NIO server integration.
 * <p>
 * Receives control messages and binary file chunks dispatched by the NIO server on the
 * Selector event loop, delegating business logic, state progression, and thread-boundary
 * coordination to {@link FileTransferService}.
 */
public final class FileTransferCoordinator implements TransferCoordinator {

    private final FileTransferService service;

    /**
     * Constructs a coordinator backed by the specified {@link FileTransferService}.
     *
     * @param service transfer service instance
     */
    public FileTransferCoordinator(FileTransferService service) {
        this.service = Objects.requireNonNull(service, "FileTransferService must not be null");
    }

    /**
     * Convenience constructor assembling the default {@link FileTransferService}.
     */
    public FileTransferCoordinator(
            TransferRegistry registry,
            SafeFileStorage storage,
            TransferWorkerPool workerPool,
            OutboundSink outboundSink,
            SelectorTasks selectorTasks,
            SessionRegistry sessionRegistry,
            AuthorizationService authorizationService
    ) {
        this(new FileTransferService(
                registry,
                storage,
                workerPool,
                outboundSink,
                selectorTasks,
                sessionRegistry,
                authorizationService
        ));
    }

    @Override
    public void handleControl(ConnectionContext context, MessageEnvelope message) {
        service.handleControl(context, message);
    }

    @Override
    public void handleChunk(ConnectionContext context, FileChunk chunk) {
        service.handleChunk(context, chunk);
    }

    @Override
    public void onDisconnected(UUID connectionId) {
        service.onDisconnected(connectionId);
    }

    public FileTransferService service() {
        return service;
    }
}
