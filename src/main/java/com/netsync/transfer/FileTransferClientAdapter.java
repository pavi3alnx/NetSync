package com.netsync.transfer;

import com.netsync.client.*;
import com.netsync.common.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Adapter integrating {@link FileTransferClientService} with the NetSync client core infrastructure
 * ({@link TcpClient}, {@link ClientDispatcher}, and {@link CommandRegistry}).
 * <p>
 * Manages dispatch subscriptions for inbound file-transfer control envelopes and binary chunks,
 * forwards outbound frames via {@link ClientOutbound#sendOrThrow}, resolves download paths lazily
 * against the authenticated {@link SessionView}, and registers user-facing CLI commands.
 */
public final class FileTransferClientAdapter implements Closeable {

    private final TcpClient client;
    private final CommandRegistry commands;
    private final Consumer<String> output;
    private final Path baseDownloadDir;
    private final FileTransferClientService service;
    private final List<ClientDispatcher.Registration> registrations = new ArrayList<>();
    private final Object lock = new Object();
    private Thread monitorThread;
    private boolean closed;

    /**
     * Creates an adapter using the default {@code downloads} folder.
     *
     * @param client   TCP client transport
     * @param commands CLI command registry
     * @param output   terminal message consumer
     */
    public FileTransferClientAdapter(TcpClient client, CommandRegistry commands, Consumer<String> output) {
        this(client, commands, output, Paths.get("downloads"), (TransferWorkerPool) null);
    }

    /**
     * Creates an adapter using a {@link PrintWriter} output (e.g. from {@link NetSyncClient}).
     *
     * @param client   TCP client transport
     * @param commands CLI command registry
     * @param output   terminal print writer
     */
    public FileTransferClientAdapter(TcpClient client, CommandRegistry commands, PrintWriter output) {
        this(client, commands, toConsumer(output), Paths.get("downloads"), (TransferWorkerPool) null);
    }

    /**
     * Creates an adapter bound to an interactive {@link NetSyncClient} shell.
     *
     * @param cli    NetSync client shell
     * @param output terminal message consumer
     */
    public FileTransferClientAdapter(NetSyncClient cli, Consumer<String> output) {
        this(cli.transport(), cli.commands(), output, Paths.get("downloads"), (TransferWorkerPool) null);
    }

    /**
     * Creates an adapter bound to an interactive {@link NetSyncClient} shell with a {@link PrintWriter}.
     *
     * @param cli    NetSync client shell
     * @param output terminal print writer
     */
    public FileTransferClientAdapter(NetSyncClient cli, PrintWriter output) {
        this(cli.transport(), cli.commands(), toConsumer(output), Paths.get("downloads"), (TransferWorkerPool) null);
    }

    /**
     * Creates an adapter with a customized base download directory.
     *
     * @param client          TCP client transport
     * @param commands        CLI command registry
     * @param output          terminal message consumer
     * @param baseDownloadDir base directory for incoming file downloads
     */
    public FileTransferClientAdapter(
            TcpClient client,
            CommandRegistry commands,
            Consumer<String> output,
            Path baseDownloadDir
    ) {
        this(client, commands, output, baseDownloadDir, (TransferWorkerPool) null);
    }

    /**
     * Creates an adapter with an optional shared worker pool.
     *
     * @param client          TCP client transport
     * @param commands        CLI command registry
     * @param output          terminal message consumer
     * @param baseDownloadDir base directory for incoming file downloads
     * @param workerPool      optional shared transfer worker pool
     */
    public FileTransferClientAdapter(
            TcpClient client,
            CommandRegistry commands,
            Consumer<String> output,
            Path baseDownloadDir,
            TransferWorkerPool workerPool
    ) {
        this(client, commands, output, baseDownloadDir,
                () -> {
                    Optional<SessionView> session = client.session();
                    if (session.isEmpty()) {
                        return null;
                    }
                    String username = session.get().username();
                    try {
                        SafeFileStorage.validateSafeFileName(username);
                    } catch (Exception e) {
                        return null;
                    }
                    Path resolved = baseDownloadDir.resolve(username).normalize();
                    if (!resolved.startsWith(baseDownloadDir.toAbsolutePath().normalize())
                            && !resolved.startsWith(baseDownloadDir.normalize())) {
                        return null;
                    }
                    return resolved;
                },
                workerPool);
    }

    /**
     * Creates an adapter with a custom download directory supplier.
     *
     * @param client              TCP client transport
     * @param commands            CLI command registry
     * @param output              terminal message consumer
     * @param downloadDirSupplier supplier for download destination directory
     */
    public FileTransferClientAdapter(
            TcpClient client,
            CommandRegistry commands,
            Consumer<String> output,
            Supplier<Path> downloadDirSupplier
    ) {
        this(client, commands, output, Paths.get("downloads"), downloadDirSupplier, null);
    }

    /**
     * Creates an adapter with a custom download directory supplier and shared worker pool.
     *
     * @param client              TCP client transport
     * @param commands            CLI command registry
     * @param output              terminal message consumer
     * @param downloadDirSupplier supplier for download destination directory
     * @param workerPool          optional shared transfer worker pool
     */
    public FileTransferClientAdapter(
            TcpClient client,
            CommandRegistry commands,
            Consumer<String> output,
            Supplier<Path> downloadDirSupplier,
            TransferWorkerPool workerPool
    ) {
        this(client, commands, output, Paths.get("downloads"), downloadDirSupplier, workerPool);
    }

    /**
     * Creates an adapter with a custom download directory supplier and shared worker pool.
     *
     * @param client              TCP client transport
     * @param commands            CLI command registry
     * @param output              terminal message consumer
     * @param baseDownloadDir     base download directory reference
     * @param downloadDirSupplier supplier for download destination directory
     * @param workerPool          optional shared transfer worker pool
     */
    public FileTransferClientAdapter(
            TcpClient client,
            CommandRegistry commands,
            Consumer<String> output,
            Path baseDownloadDir,
            Supplier<Path> downloadDirSupplier,
            TransferWorkerPool workerPool
    ) {
        this.client = Objects.requireNonNull(client, "TcpClient must not be null");
        this.commands = Objects.requireNonNull(commands, "CommandRegistry must not be null");
        this.output = Objects.requireNonNull(output, "Output consumer must not be null");
        this.baseDownloadDir = Objects.requireNonNull(baseDownloadDir, "Base download directory must not be null");
        Objects.requireNonNull(downloadDirSupplier, "Download directory supplier must not be null");

        Consumer<OutboundFrame> frameSink = frame -> {
            try {
                this.client.sendOrThrow(frame);
            } catch (RejectedExecutionException e) {
                if (!this.client.isConnected()) {
                    onDisconnect();
                }
                // Do not propagate exception if an automatic reject frame could not be sent while disconnected
                if (frame instanceof MessageEnvelope env && env.type() == MessageType.FILE_REJECT && !this.client.isConnected()) {
                    return;
                }
                throw e;
            }
        };

        if (workerPool != null) {
            this.service = new FileTransferClientService(frameSink, downloadDirSupplier, workerPool);
        } else {
            this.service = new FileTransferClientService(frameSink, downloadDirSupplier);
        }

        initialize();
    }

    /**
     * Creates an adapter wrapping an existing {@link FileTransferClientService} (primarily for testing).
     *
     * @param client   TCP client transport
     * @param commands CLI command registry
     * @param output   terminal message consumer
     * @param service  pre-configured file transfer client service
     */
    public FileTransferClientAdapter(
            TcpClient client,
            CommandRegistry commands,
            Consumer<String> output,
            FileTransferClientService service
    ) {
        this.client = Objects.requireNonNull(client, "TcpClient must not be null");
        this.commands = Objects.requireNonNull(commands, "CommandRegistry must not be null");
        this.output = Objects.requireNonNull(output, "Output consumer must not be null");
        this.baseDownloadDir = Paths.get("downloads");
        this.service = Objects.requireNonNull(service, "FileTransferClientService must not be null");

        initialize();
    }

    private static Consumer<String> toConsumer(PrintWriter writer) {
        Objects.requireNonNull(writer, "PrintWriter must not be null");
        return message -> {
            synchronized (writer) {
                writer.println(message);
                writer.flush();
            }
        };
    }

    private void initialize() {
        try {
            registerDispatcherHandlers();
            registerCommands();
            configureEventListener();
            startConnectionMonitor();
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    private void startConnectionMonitor() {
        monitorThread = new Thread(this::monitorConnection, "netsync-transfer-connection-monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    private void monitorConnection() {
        while (true) {
            synchronized (lock) {
                if (closed) {
                    return;
                }
            }
            synchronized (client) {
                try {
                    client.wait(500);
                } catch (InterruptedException e) {
                    return;
                }
            }
            synchronized (lock) {
                if (closed) {
                    return;
                }
            }
            if (!client.isConnected()) {
                onDisconnect();
            }
        }
    }

    private void registerDispatcherHandlers() {
        ClientDispatcher dispatcher = client.dispatcher();
        Consumer<MessageEnvelope> controlHandler = service::handleControl;

        registrations.add(dispatcher.register(MessageType.FILE_OFFERED, controlHandler));
        registrations.add(dispatcher.register(MessageType.FILE_COMPLETE, controlHandler));
        registrations.add(dispatcher.register(MessageType.ACK, controlHandler));
        registrations.add(dispatcher.register(MessageType.FILE_STATUS, controlHandler));
        registrations.add(dispatcher.register(MessageType.ERROR, controlHandler));
        registrations.add(dispatcher.register(MessageType.FILE_CANCEL, controlHandler));

        registrations.add(dispatcher.registerBinary(service::handleChunk));
    }

    private void configureEventListener() {
        service.setEventListener(new FileTransferClientService.FileTransferEventListener() {
            @Override
            public void onOfferReceived(UUID transferId, UUID senderClientId, String fileName, long sizeBytes) {
                print(String.format(Locale.ROOT,
                        "[Transfer] Incoming file offer from %s: %s (%s). Transfer ID: %s. Type /accept %s or /reject %s",
                        senderClientId, fileName, formatBytes(sizeBytes), transferId, transferId, transferId));
            }

            @Override
            public void onProgress(FileTransferProgress progress) {
                print(String.format(Locale.ROOT,
                        "[Transfer] Progress [%s]: %.1f%% (%d/%d bytes, chunk #%d)",
                        progress.transferId(), progress.percentage(), progress.bytesTransferred(), progress.totalBytes(), progress.chunkSequence()));
            }

            @Override
            public void onCompleted(FileTransferResult result) {
                if (result.localFile() != null) {
                    print(String.format(Locale.ROOT,
                            "[Transfer] Completed [%s]: Saved to %s (%d bytes)",
                            result.transferId(), result.localFile(), result.bytesTransferred()));
                } else {
                    print(String.format(Locale.ROOT,
                            "[Transfer] Completed [%s]: %d bytes sent successfully",
                            result.transferId(), result.bytesTransferred()));
                }
            }

            @Override
            public void onFailed(UUID transferId, String reason) {
                print(String.format(Locale.ROOT, "[Transfer] Failed [%s]: %s", transferId, reason));
            }

            @Override
            public void onCancelled(UUID transferId, String reason) {
                print(String.format(Locale.ROOT, "[Transfer] Cancelled [%s]: %s", transferId, reason));
            }

            @Override
            public void onRejected(UUID transferId, String reason) {
                print(String.format(Locale.ROOT, "[Transfer] Rejected [%s]: %s", transferId, reason));
            }
        });
    }

    private void registerCommands() {
        commands.register("/sendfile", "/sendfile <recipientClientId> <filePath>", "Send a file to a recipient", this::executeSendFile);
        commands.register("/accept", "/accept <transferId>", "Accept an incoming file offer", this::executeAccept);
        commands.register("/reject", "/reject <transferId>", "Reject an incoming file offer", this::executeReject);
        commands.register("/cancel", "/cancel [transferId]", "Cancel the active file transfer or a specific transfer", this::executeCancel);
    }

    private void executeSendFile(List<String> args) {
        if (args.size() != 2) {
            throw new CommandRegistry.InputException("Usage: /sendfile <recipientClientId> <filePath>");
        }

        UUID recipientClientId;
        try {
            recipientClientId = UUID.fromString(args.get(0));
        } catch (IllegalArgumentException e) {
            throw new CommandRegistry.InputException("Invalid recipient client ID format: " + args.get(0));
        }

        if (!client.isConnected()) {
            throw new CommandRegistry.InputException("Not connected to server. Use /connect first.");
        }

        Optional<SessionView> session = client.session();
        if (session.isEmpty()) {
            throw new CommandRegistry.InputException("Not authenticated. Please log in before sending files.");
        }

        if (session.get().clientId().equals(recipientClientId)) {
            throw new CommandRegistry.InputException("Cannot send file to yourself.");
        }

        Path sourceFile = Paths.get(args.get(1));
        if (!Files.exists(sourceFile)) {
            throw new CommandRegistry.InputException("Source file does not exist: " + sourceFile);
        }
        if (Files.isDirectory(sourceFile)) {
            throw new CommandRegistry.InputException("Source path is a directory, not a regular file: " + sourceFile);
        }
        if (!Files.isReadable(sourceFile)) {
            throw new CommandRegistry.InputException("Source file is not readable: " + sourceFile);
        }

        long sizeBytes;
        try {
            sizeBytes = Files.size(sourceFile);
        } catch (IOException e) {
            throw new CommandRegistry.InputException("Failed to read source file size: " + e.getMessage());
        }

        if (sizeBytes > ProtocolLimits.MAX_FILE_BYTES) {
            throw new CommandRegistry.InputException("File size exceeds maximum protocol limit ("
                    + ProtocolLimits.MAX_FILE_BYTES + " bytes): " + sizeBytes);
        }

        if (service.isTransferActive()) {
            throw new CommandRegistry.InputException("A file transfer is already active: " + service.activeTransferId().orElse(null));
        }

        try {
            service.sendFile(recipientClientId, sourceFile);
            print(String.format(Locale.ROOT, "[Transfer] Offering file %s (%s) to %s...",
                    sourceFile.getFileName(), formatBytes(sizeBytes), recipientClientId));
        } catch (IllegalStateException e) {
            throw new CommandRegistry.InputException(e.getMessage());
        } catch (Exception e) {
            throw new CommandRegistry.InputException("Failed to initiate file transfer: " + e.getMessage());
        }
    }

    private void executeAccept(List<String> args) {
        if (args.size() != 1) {
            throw new CommandRegistry.InputException("Usage: /accept <transferId>");
        }

        UUID transferId;
        try {
            transferId = UUID.fromString(args.get(0));
        } catch (IllegalArgumentException e) {
            throw new CommandRegistry.InputException("Invalid transfer ID format: " + args.get(0));
        }

        if (!client.isConnected()) {
            throw new CommandRegistry.InputException("Not connected to server. Use /connect first.");
        }

        Optional<SessionView> session = client.session();
        if (session.isEmpty()) {
            throw new CommandRegistry.InputException("Not authenticated. Please log in before accepting file transfers.");
        }

        if (service.receiverState().isEmpty()
                || service.receiverState().get() != FileReceiver.ReceiverState.OFFERED
                || !transferId.equals(service.activeTransferId().orElse(null))) {
            throw new CommandRegistry.InputException("No pending file offer with transfer ID: " + transferId);
        }

        try {
            service.acceptOffer(transferId);
            print("[Transfer] Accepted offer for transfer " + transferId + ". Starting download...");
        } catch (IllegalStateException e) {
            throw new CommandRegistry.InputException(e.getMessage());
        } catch (Exception e) {
            throw new CommandRegistry.InputException("Failed to accept file transfer: " + e.getMessage());
        }
    }

    private void executeReject(List<String> args) {
        if (args.size() != 1) {
            throw new CommandRegistry.InputException("Usage: /reject <transferId>");
        }

        UUID transferId;
        try {
            transferId = UUID.fromString(args.get(0));
        } catch (IllegalArgumentException e) {
            throw new CommandRegistry.InputException("Invalid transfer ID format: " + args.get(0));
        }

        if (service.receiverState().isEmpty()
                || service.receiverState().get() != FileReceiver.ReceiverState.OFFERED
                || !transferId.equals(service.activeTransferId().orElse(null))) {
            throw new CommandRegistry.InputException("No pending file offer with transfer ID: " + transferId);
        }

        try {
            service.rejectOffer(transferId);
            print("[Transfer] Rejected offer for transfer " + transferId + ".");
        } catch (IllegalStateException e) {
            throw new CommandRegistry.InputException(e.getMessage());
        } catch (Exception e) {
            throw new CommandRegistry.InputException("Failed to reject file transfer: " + e.getMessage());
        }
    }

    private void executeCancel(List<String> args) {
        if (args.size() > 1) {
            throw new CommandRegistry.InputException("Usage: /cancel [transferId]");
        }

        UUID transferId = null;
        if (args.size() == 1) {
            try {
                transferId = UUID.fromString(args.get(0));
            } catch (IllegalArgumentException e) {
                throw new CommandRegistry.InputException("Invalid transfer ID format: " + args.get(0));
            }
        }

        if (!service.isTransferActive()) {
            throw new CommandRegistry.InputException("No active file transfer to cancel.");
        }

        if (transferId != null) {
            if (!transferId.equals(service.activeTransferId().orElse(null))) {
                throw new CommandRegistry.InputException("Active transfer ID ("
                        + service.activeTransferId().orElse(null) + ") does not match: " + transferId);
            }

            service.cancelTransfer(transferId);
            print("[Transfer] Cancelled transfer " + transferId + ".");
        } else {
            Optional<UUID> activeId = service.activeTransferId();
            service.cancelActiveTransfer();
            print("[Transfer] Cancelled active transfer" + activeId.map(id -> " " + id).orElse("") + ".");
        }
    }

    /**
     * Invoked when the client TCP connection disconnects. Cancels active transfers and cleans up partial files.
     */
    public void onDisconnect() {
        boolean wasActive = service.isTransferActive();
        service.onDisconnect();
        if (wasActive) {
            print("[Transfer] Disconnected from server. Active transfer cancelled.");
        }
    }

    public FileTransferClientService transferService() {
        return service;
    }

    public TcpClient client() {
        return client;
    }

    public CommandRegistry commandRegistry() {
        return commands;
    }

    public Path baseDownloadDir() {
        return baseDownloadDir;
    }

    public boolean isClosed() {
        synchronized (lock) {
            return closed;
        }
    }

    private void print(String message) {
        try {
            output.accept(message);
        } catch (Exception ignored) {
        }
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format(Locale.ROOT, "%.1f KiB", bytes / 1024.0);
        return String.format(Locale.ROOT, "%.1f MiB", bytes / (1024.0 * 1024.0));
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            if (monitorThread != null) {
                monitorThread.interrupt();
            }
        }

        for (ClientDispatcher.Registration reg : registrations) {
            try {
                reg.close();
            } catch (Exception ignored) {
            }
        }
        registrations.clear();

        service.close();
    }
}
