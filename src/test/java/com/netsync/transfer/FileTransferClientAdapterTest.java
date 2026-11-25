package com.netsync.transfer;

import com.netsync.client.*;
import com.netsync.common.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class FileTransferClientAdapterTest {

    private static final FrameCodec CODEC = new FrameCodec(new MessageCodec());

    @TempDir
    Path tempDir;

    private Path downloadsDir;
    private Path sourceDir;
    private List<String> outputLines;
    private TransferWorkerPool sharedWorkerPool;

    @BeforeEach
    void setUp() throws IOException {
        downloadsDir = tempDir.resolve("downloads");
        sourceDir = tempDir.resolve("source");
        Files.createDirectories(downloadsDir);
        Files.createDirectories(sourceDir);

        outputLines = new CopyOnWriteArrayList<>();
        sharedWorkerPool = new TransferWorkerPool(2, 16);
    }

    @AfterEach
    void tearDown() {
        if (sharedWorkerPool != null && !sharedWorkerPool.isShutdown()) {
            sharedWorkerPool.close();
        }
    }

    private Path createTestFile(String name, int sizeBytes) throws IOException {
        Path file = sourceDir.resolve(name);
        byte[] content = new byte[sizeBytes];
        for (int i = 0; i < sizeBytes; i++) {
            content[i] = (byte) ((i % 127) + 1);
        }
        Files.write(file, content);
        return file;
    }

    private static void sendWireFrame(Socket peer, OutboundFrame frame) throws Exception {
        byte[] bytes = CODEC.encode(frame).array();
        peer.getOutputStream().write(bytes);
        peer.getOutputStream().flush();
    }

    @Test
    void test1_adapterConstructionAndCommandRegistration() {
        try (TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {
                assertSame(client, adapter.client());
                assertSame(registry, adapter.commandRegistry());
                assertEquals(downloadsDir, adapter.baseDownloadDir());
                assertNotNull(adapter.transferService());
                assertFalse(adapter.isClosed());

                List<CommandRegistry.Description> descriptions = registry.descriptions();
                assertTrue(descriptions.stream().anyMatch(d -> d.name().equals("/sendfile")));
                assertTrue(descriptions.stream().anyMatch(d -> d.name().equals("/accept")));
                assertTrue(descriptions.stream().anyMatch(d -> d.name().equals("/reject")));
                assertTrue(descriptions.stream().anyMatch(d -> d.name().equals("/cancel")));
            }
        }
    }

    @Test
    void test2_netSyncClientWrapperConstructors() {
        try (TcpClient client = new TcpClient()) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw, true);
            NetSyncClient cli = new NetSyncClient(client, pw);

            try (cli) {
                FileTransferClientAdapter adapter = cli.fileTransfer();
                assertSame(client, adapter.client());
                assertSame(cli.commands(), adapter.commandRegistry());
                assertFalse(adapter.isClosed());
                assertTrue(cli.commands().descriptions().stream().anyMatch(d -> d.name().equals("/sendfile")));
            }
        }
    }

    @Test
    void test3_sendFileArgumentValidation() throws Exception {
        try (TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {

                // 1. Wrong argument count
                CommandRegistry.InputException ex1 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/sendfile"));
                assertTrue(ex1.getMessage().contains("Usage: /sendfile"));

                CommandRegistry.InputException ex2 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/sendfile singleArg"));
                assertTrue(ex2.getMessage().contains("Usage: /sendfile"));

                CommandRegistry.InputException ex3 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/sendfile arg1 arg2 arg3"));
                assertTrue(ex3.getMessage().contains("Usage: /sendfile"));

                // 2. Invalid recipient UUID
                CommandRegistry.InputException ex4 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/sendfile not-a-uuid somefile.txt"));
                assertTrue(ex4.getMessage().contains("Invalid recipient client ID format"));

                UUID validRecipient = UUID.randomUUID();

                // 3. Disconnected client
                CommandRegistry.InputException ex5 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/sendfile " + validRecipient + " somefile.txt"));
                assertTrue(ex5.getMessage().contains("Not connected to server"));

                // Connect client to a local test server
                try (ServerSocket server = new ServerSocket(0)) {
                    client.connect("127.0.0.1", server.getLocalPort());
                    try (Socket ignored = server.accept()) {

                        // 4. Connected but unauthenticated
                        CommandRegistry.InputException ex6 = assertThrows(CommandRegistry.InputException.class,
                                () -> registry.execute("/sendfile " + validRecipient + " somefile.txt"));
                        assertTrue(ex6.getMessage().contains("Not authenticated"));

                        // Supply session
                        UUID myClientId = UUID.randomUUID();
                        UUID connectionId = client.connectionId().orElseThrow();
                        client.supplySession(connectionId, new SessionView(myClientId, "alice", connectionId));

                        // 5. Attempt self-transfer
                        CommandRegistry.InputException ex7 = assertThrows(CommandRegistry.InputException.class,
                                () -> registry.execute("/sendfile " + myClientId + " somefile.txt"));
                        assertTrue(ex7.getMessage().contains("Cannot send file to yourself"));

                        // 6. Non-existent file
                        Path nonExistent = sourceDir.resolve("missing.txt");
                        CommandRegistry.InputException ex8 = assertThrows(CommandRegistry.InputException.class,
                                () -> registry.execute("/sendfile " + validRecipient + " \"" + nonExistent.toString().replace('\\', '/') + "\""));
                        assertTrue(ex8.getMessage().contains("Source file does not exist"));

                        // 7. Directory path
                        CommandRegistry.InputException ex9 = assertThrows(CommandRegistry.InputException.class,
                                () -> registry.execute("/sendfile " + validRecipient + " \"" + sourceDir.toString().replace('\\', '/') + "\""));
                        assertTrue(ex9.getMessage().contains("directory"));

                        // 8. File exceeding 10 MB limit
                        Path oversized = sourceDir.resolve("oversized.dat");
                        long size = ProtocolLimits.MAX_FILE_BYTES + 1;
                        try (var channel = java.nio.channels.FileChannel.open(
                                oversized,
                                java.nio.file.StandardOpenOption.CREATE,
                                java.nio.file.StandardOpenOption.WRITE)) {
                            channel.position(size - 1);
                            channel.write(java.nio.ByteBuffer.wrap(new byte[]{1}));
                        }
                        CommandRegistry.InputException ex10 = assertThrows(CommandRegistry.InputException.class,
                                () -> registry.execute("/sendfile " + validRecipient + " \"" + oversized.toString().replace('\\', '/') + "\""));
                        assertTrue(ex10.getMessage().contains("exceeds maximum protocol limit"));
                    }
                }
            }
        }
    }

    @Test
    void test4_acceptAndRejectArgumentValidation() {
        try (TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {

                // /accept argument count
                CommandRegistry.InputException ex1 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/accept"));
                assertTrue(ex1.getMessage().contains("Usage: /accept"));

                CommandRegistry.InputException ex2 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/accept arg1 arg2"));
                assertTrue(ex2.getMessage().contains("Usage: /accept"));

                // /accept invalid UUID
                CommandRegistry.InputException ex3 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/accept not-a-uuid"));
                assertTrue(ex3.getMessage().contains("Invalid transfer ID format"));

                // /accept when disconnected
                UUID randomTid = UUID.randomUUID();
                CommandRegistry.InputException ex4 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/accept " + randomTid));
                assertTrue(ex4.getMessage().contains("Not connected"));

                // /reject argument count
                CommandRegistry.InputException ex5 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/reject"));
                assertTrue(ex5.getMessage().contains("Usage: /reject"));

                CommandRegistry.InputException ex6 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/reject arg1 arg2"));
                assertTrue(ex6.getMessage().contains("Usage: /reject"));

                // /reject invalid UUID
                CommandRegistry.InputException ex7 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/reject not-a-uuid"));
                assertTrue(ex7.getMessage().contains("Invalid transfer ID format"));

                // /reject when no offer pending
                CommandRegistry.InputException ex8 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/reject " + randomTid));
                assertTrue(ex8.getMessage().contains("No pending file offer"));
            }
        }
    }

    @Test
    void test5_cancelArgumentValidation() {
        try (TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {

                // Too many arguments
                CommandRegistry.InputException ex1 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/cancel id1 id2"));
                assertTrue(ex1.getMessage().contains("Usage: /cancel"));

                // Invalid UUID
                CommandRegistry.InputException ex2 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/cancel not-a-uuid"));
                assertTrue(ex2.getMessage().contains("Invalid transfer ID format"));

                // No active transfer
                CommandRegistry.InputException ex3 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/cancel"));
                assertTrue(ex3.getMessage().contains("No active file transfer to cancel"));

                CommandRegistry.InputException ex4 = assertThrows(CommandRegistry.InputException.class,
                        () -> registry.execute("/cancel " + UUID.randomUUID()));
                assertTrue(ex4.getMessage().contains("No active file transfer to cancel"));
            }
        }
    }

    @Test
    void test6_sendFileExecutionAndOutboundForwarding() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {

                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    UUID connId = client.connectionId().orElseThrow();
                    UUID myClientId = UUID.randomUUID();
                    client.supplySession(connId, new SessionView(myClientId, "bob", connId));

                    Path source = createTestFile("hello.txt", 1024);
                    UUID recipient = UUID.randomUUID();

                    registry.execute("/sendfile " + recipient + " \"" + source.toString().replace('\\', '/') + "\"");

                    // Verify output acknowledged
                    assertTrue(outputLines.stream().anyMatch(l -> l.contains("Offering file hello.txt")));
                    assertTrue(adapter.transferService().isTransferActive());

                    // Attempting second /sendfile while active fails
                    CommandRegistry.InputException ex = assertThrows(CommandRegistry.InputException.class,
                            () -> registry.execute("/sendfile " + recipient + " \"" + source.toString().replace('\\', '/') + "\""));
                    assertTrue(ex.getMessage().contains("already active"));

                    // Peer receives FILE_OFFER frame
                    peer.setSoTimeout(3000);
                    java.io.DataInputStream dis = new java.io.DataInputStream(peer.getInputStream());
                    int length = dis.readInt();
                    byte[] payload = dis.readNBytes(length);
                    OutboundFrame frame = CODEC.decodePayload(java.nio.ByteBuffer.wrap(payload));
                    assertInstanceOf(MessageEnvelope.class, frame);
                    MessageEnvelope env = (MessageEnvelope) frame;
                    assertEquals(MessageType.FILE_OFFER, env.type());
                }
            }
        }
    }

    @Test
    void test7_inboundRoutingAndOfferNotifications() {
        try (TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, () -> downloadsDir, sharedWorkerPool)) {

                UUID transferId = UUID.randomUUID();
                UUID senderId = UUID.randomUUID();

                MessageEnvelope offerEnv = new MessageEnvelope(
                        ProtocolLimits.VERSION,
                        MessageType.FILE_OFFERED,
                        UUID.randomUUID(),
                        null,
                        new MessageBody.FileOffered(transferId, senderId, "document.pdf", 4096)
                );

                adapter.transferService().handleControl(offerEnv);

                // Verify adapter listener formatted message and active state
                assertTrue(adapter.transferService().isTransferActive());
                assertEquals(Optional.of(FileReceiver.ReceiverState.OFFERED), adapter.transferService().receiverState());
                assertTrue(outputLines.stream().anyMatch(l ->
                        l.contains("Incoming file offer from " + senderId) && l.contains("document.pdf")));

                // Forwarding binary chunk routes to receiver
                FileChunk chunk = new FileChunk(UUID.randomUUID(), transferId, 0, new byte[100]);
                assertDoesNotThrow(() -> adapter.transferService().handleChunk(chunk));
            }
        }
    }

    @Test
    void test8_acceptAndRejectOfferExecution() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {

                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    UUID connId = client.connectionId().orElseThrow();
                    client.supplySession(connId, new SessionView(UUID.randomUUID(), "charlie", connId));

                    UUID transferId = UUID.randomUUID();
                    MessageEnvelope offerEnv = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_OFFERED,
                            UUID.randomUUID(),
                            null,
                            new MessageBody.FileOffered(transferId, UUID.randomUUID(), "notes.txt", 2048)
                    );
                    adapter.transferService().handleControl(offerEnv);
                    assertTrue(adapter.transferService().isTransferActive());

                    // Execute /accept
                    registry.execute("/accept " + transferId);
                    assertTrue(outputLines.stream().anyMatch(l -> l.contains("Accepted offer for transfer " + transferId)));
                    assertEquals(Optional.of(FileReceiver.ReceiverState.RECEIVING), adapter.transferService().receiverState());
                }
            }
        }

        // Test rejection on another adapter
        try (TcpClient client2 = new TcpClient()) {
            CommandRegistry registry2 = new CommandRegistry();
            try (FileTransferClientAdapter adapter2 = new FileTransferClientAdapter(client2, registry2, outputLines::add, () -> downloadsDir, sharedWorkerPool)) {
                UUID transferId2 = UUID.randomUUID();
                MessageEnvelope offerEnv2 = new MessageEnvelope(
                        ProtocolLimits.VERSION,
                        MessageType.FILE_OFFERED,
                        UUID.randomUUID(),
                        null,
                        new MessageBody.FileOffered(transferId2, UUID.randomUUID(), "rejected.txt", 1024)
                );
                adapter2.transferService().handleControl(offerEnv2);
                assertTrue(adapter2.transferService().isTransferActive());

                registry2.execute("/reject " + transferId2);
                assertTrue(outputLines.stream().anyMatch(l -> l.contains("Rejected offer for transfer " + transferId2)));
                assertFalse(adapter2.transferService().isTransferActive());
            }
        }
    }

    @Test
    void test9_cancelExecution() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {

                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    UUID connId = client.connectionId().orElseThrow();
                    client.supplySession(connId, new SessionView(UUID.randomUUID(), "dan", connId));

                    // 1. Cancel with matching ID
                    UUID transferId = UUID.randomUUID();
                    MessageEnvelope offerEnv = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_OFFERED,
                            UUID.randomUUID(),
                            null,
                            new MessageBody.FileOffered(transferId, UUID.randomUUID(), "cancel_id.txt", 1024)
                    );
                    adapter.transferService().handleControl(offerEnv);
                    assertTrue(adapter.transferService().isTransferActive());

                    // Mismatched ID throws InputException
                    UUID wrongId = UUID.randomUUID();
                    CommandRegistry.InputException ex = assertThrows(CommandRegistry.InputException.class,
                            () -> registry.execute("/cancel " + wrongId));
                    assertTrue(ex.getMessage().contains("does not match"));

                    // Matching ID cancels
                    registry.execute("/cancel " + transferId);
                    assertFalse(adapter.transferService().isTransferActive());
                    assertTrue(outputLines.stream().anyMatch(l -> l.contains("Cancelled transfer " + transferId)));

                    // 2. Cancel active without argument
                    UUID transferId2 = UUID.randomUUID();
                    MessageEnvelope offerEnv2 = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_OFFERED,
                            UUID.randomUUID(),
                            null,
                            new MessageBody.FileOffered(transferId2, UUID.randomUUID(), "cancel_active.txt", 1024)
                    );
                    adapter.transferService().handleControl(offerEnv2);
                    assertTrue(adapter.transferService().isTransferActive());

                    registry.execute("/cancel");
                    assertFalse(adapter.transferService().isTransferActive());
                    assertTrue(outputLines.stream().anyMatch(l -> l.contains("Cancelled active transfer " + transferId2)));
                }
            }
        }
    }

    @Test
    void test10_disconnectCleanup() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {

                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    UUID connId = client.connectionId().orElseThrow();
                    client.supplySession(connId, new SessionView(UUID.randomUUID(), "eve", connId));

                    UUID transferId = UUID.randomUUID();
                    MessageEnvelope offerEnv = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_OFFERED,
                            UUID.randomUUID(),
                            null,
                            new MessageBody.FileOffered(transferId, UUID.randomUUID(), "disconnect.txt", 2048)
                    );
                    adapter.transferService().handleControl(offerEnv);
                    assertTrue(adapter.transferService().isTransferActive());

                    adapter.onDisconnect();
                    assertFalse(adapter.transferService().isTransferActive());
                }
            }
        }
    }

    @Test
    void test11_registrationCleanupAndIdempotentClose() {
        try (TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool);

            assertFalse(adapter.isClosed());
            adapter.close();
            assertTrue(adapter.isClosed());

            // Idempotent close
            assertDoesNotThrow(adapter::close);
        }
    }

    @Test
    void test12_lazyDownloadDirectorySupplierWithSession() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {

                // While unauthenticated, incoming offer is automatically rejected
                UUID transferId = UUID.randomUUID();
                MessageEnvelope unauthOffer = new MessageEnvelope(
                        ProtocolLimits.VERSION,
                        MessageType.FILE_OFFERED,
                        UUID.randomUUID(),
                        null,
                        new MessageBody.FileOffered(transferId, UUID.randomUUID(), "test.txt", 1024)
                );
                adapter.transferService().handleControl(unauthOffer);
                // Service rejects offer because downloadDirSupplier returns null
                assertFalse(adapter.transferService().isTransferActive());

                // Now connect and authenticate
                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    UUID connId = client.connectionId().orElseThrow();
                    client.supplySession(connId, new SessionView(UUID.randomUUID(), "frank", connId));

                    // Now incoming offer is accepted into OFFERED state
                    UUID transferId2 = UUID.randomUUID();
                    MessageEnvelope authOffer = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_OFFERED,
                            UUID.randomUUID(),
                            null,
                            new MessageBody.FileOffered(transferId2, UUID.randomUUID(), "test2.txt", 1024)
                    );
                    adapter.transferService().handleControl(authOffer);
                    assertTrue(adapter.transferService().isTransferActive());
                    assertEquals(Optional.of(FileReceiver.ReceiverState.OFFERED), adapter.transferService().receiverState());
                }
            }
        }
    }

    @Test
    void test13_eventListenerFormattedOutputInspection() {
        try (TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, () -> downloadsDir, sharedWorkerPool)) {

                UUID transferId = UUID.randomUUID();

                // Trigger offer
                MessageEnvelope offerEnv = new MessageEnvelope(
                        ProtocolLimits.VERSION,
                        MessageType.FILE_OFFERED,
                        UUID.randomUUID(),
                        null,
                        new MessageBody.FileOffered(transferId, UUID.randomUUID(), "stats.csv", 5000)
                );
                adapter.transferService().handleControl(offerEnv);
                assertTrue(outputLines.stream().anyMatch(l -> l.contains("stats.csv") && l.contains("4.9 KiB")));

                // Trigger reject of pending offer
                adapter.transferService().rejectOffer(transferId);
                assertTrue(outputLines.stream().anyMatch(l -> l.contains("Rejected") && l.contains("Rejected by user")));

                // Trigger cancel on a second offer
                UUID transferId2 = UUID.randomUUID();
                MessageEnvelope offerEnv2 = new MessageEnvelope(
                        ProtocolLimits.VERSION,
                        MessageType.FILE_OFFERED,
                        UUID.randomUUID(),
                        null,
                        new MessageBody.FileOffered(transferId2, UUID.randomUUID(), "report.pdf", 2048)
                );
                adapter.transferService().handleControl(offerEnv2);
                MessageEnvelope statusCancel = new MessageEnvelope(
                        ProtocolLimits.VERSION,
                        MessageType.FILE_STATUS,
                        UUID.randomUUID(),
                        null,
                        new MessageBody.FileStatus(transferId2, FileStatusState.CANCELLED, "Cancelled by sender")
                );
                adapter.transferService().handleControl(statusCancel);
                assertTrue(outputLines.stream().anyMatch(l -> l.contains("Cancelled") && l.contains("Offer cancelled by sender")));
            }
        }
    }

    @Test
    void test14_loopbackWireDispatchingThroughTcpClient() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {

                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    UUID connId = client.connectionId().orElseThrow();
                    client.supplySession(connId, new SessionView(UUID.randomUUID(), "grace", connId));

                    UUID transferId = UUID.randomUUID();
                    MessageEnvelope offer = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_OFFERED,
                            UUID.randomUUID(),
                            null,
                            new MessageBody.FileOffered(transferId, UUID.randomUUID(), "wire_test.bin", 1024)
                    );

                    // Send over actual TCP wire socket
                    sendWireFrame(peer, offer);

                    // Await dispatch to adapter's service and output logging
                    long deadline = System.currentTimeMillis() + 3000;
                    while ((!adapter.transferService().isTransferActive() || outputLines.isEmpty()) && System.currentTimeMillis() < deadline) {
                        Thread.sleep(10);
                    }

                    assertTrue(adapter.transferService().isTransferActive());
                    assertEquals(Optional.of(FileReceiver.ReceiverState.OFFERED), adapter.transferService().receiverState());
                    assertTrue(outputLines.stream().anyMatch(l -> l.contains("wire_test.bin")));
                }
            }
        }
    }

    @Test
    void test15_remoteDisconnectDuringActiveTransferDetectsDisconnect() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {

                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    UUID connId = client.connectionId().orElseThrow();
                    client.supplySession(connId, new SessionView(UUID.randomUUID(), "remote_user", connId));

                    UUID transferId = UUID.randomUUID();
                    MessageEnvelope offer = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_OFFERED,
                            UUID.randomUUID(),
                            null,
                            new MessageBody.FileOffered(transferId, UUID.randomUUID(), "disconnect_mon.bin", 2048)
                    );
                    adapter.transferService().handleControl(offer);
                    assertTrue(adapter.transferService().isTransferActive());

                    // Close peer socket abruptly to simulate remote disconnect
                    peer.close();

                    // Monitor thread should detect client disconnection and invoke onDisconnect()
                    long deadline = System.currentTimeMillis() + 3000;
                    while (adapter.transferService().isTransferActive() && System.currentTimeMillis() < deadline) {
                        Thread.sleep(20);
                    }

                    assertFalse(adapter.transferService().isTransferActive());
                    assertTrue(outputLines.stream().anyMatch(l -> l.contains("Disconnected from server. Active transfer cancelled.")));
                }
            }
        }
    }

    @Test
    void test16_acceptUnauthenticatedThrowsInputException() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, () -> downloadsDir, sharedWorkerPool)) {

                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    // Connected but NOT authenticated (no session supplied)
                    UUID transferId = UUID.randomUUID();
                    MessageEnvelope offer = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_OFFERED,
                            UUID.randomUUID(),
                            null,
                            new MessageBody.FileOffered(transferId, UUID.randomUUID(), "unauth.txt", 1024)
                    );
                    adapter.transferService().handleControl(offer);
                    assertTrue(adapter.transferService().isTransferActive());

                    CommandRegistry.InputException ex = assertThrows(CommandRegistry.InputException.class,
                            () -> registry.execute("/accept " + transferId));
                    assertTrue(ex.getMessage().contains("Not authenticated"));
                }
            }
        }
    }

    @Test
    void test17_pathTraversalInUsernameRejectsOfferAndProtectsFilesystem() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {

                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    UUID connId = client.connectionId().orElseThrow();
                    // Attempt path traversal via username
                    client.supplySession(connId, new SessionView(UUID.randomUUID(), "../traversal_user", connId));

                    UUID transferId = UUID.randomUUID();
                    MessageEnvelope offer = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_OFFERED,
                            UUID.randomUUID(),
                            null,
                            new MessageBody.FileOffered(transferId, UUID.randomUUID(), "traversal.txt", 1024)
                    );
                    adapter.transferService().handleControl(offer);

                    // Since username attempted path traversal, lazy supplier returned null and offer was rejected
                    assertFalse(adapter.transferService().isTransferActive());

                    // Verify no traversal directory created in parent folder
                    Path escapedPath = downloadsDir.getParent().resolve("traversal_user");
                    assertFalse(Files.exists(escapedPath));
                }
            }
        }
    }

    @Test
    void test18_outboundQueueRejectionHandledGracefully() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            // Create an adapter whose service throws or rejects
            FileTransferClientService mockService = new FileTransferClientService(
                    frame -> {
                        throw new RejectedExecutionException("Simulated queue saturation");
                    },
                    () -> downloadsDir
            );
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, mockService)) {
                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    UUID connId = client.connectionId().orElseThrow();
                    client.supplySession(connId, new SessionView(UUID.randomUUID(), "test_user", connId));

                    Path file = createTestFile("queued.txt", 1024);
                    UUID recipient = UUID.randomUUID();

                    CommandRegistry.InputException ex = assertThrows(CommandRegistry.InputException.class,
                            () -> registry.execute("/sendfile " + recipient + " \"" + file.toString().replace('\\', '/') + "\""));
                    assertTrue(ex.getMessage().contains("Simulated queue saturation") || ex.getMessage().contains("Failed to initiate"));
                }
            }
        }
    }

    @Test
    void test19_adapterCloseWhileTransferActiveCleansUp() {
        try (TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, () -> downloadsDir, sharedWorkerPool);

            UUID transferId = UUID.randomUUID();
            MessageEnvelope offer = new MessageEnvelope(
                    ProtocolLimits.VERSION,
                    MessageType.FILE_OFFERED,
                    UUID.randomUUID(),
                    null,
                    new MessageBody.FileOffered(transferId, UUID.randomUUID(), "active_close.bin", 2048)
            );
            adapter.transferService().handleControl(offer);
            assertTrue(adapter.transferService().isTransferActive());

            // Close adapter
            adapter.close();
            assertTrue(adapter.isClosed());
            assertFalse(adapter.transferService().isTransferActive());
        }
    }

    @Test
    void test20_duplicateAdapterRegistrationFailsWithoutLeaking() {
        try (TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter1 = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {
                assertFalse(adapter1.isClosed());

                // Attempting to construct second adapter on same CommandRegistry fails
                IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                        () -> new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool));
                assertTrue(ex.getMessage().contains("Command already registered"));

                // First adapter is still intact and functional
                assertFalse(adapter1.isClosed());
                assertTrue(adapter1.commandRegistry().descriptions().stream().anyMatch(d -> d.name().equals("/sendfile")));
            }
        }
    }

    @Test
    void test21_concurrentCommandAndInboundOfferRaceCondition() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(client, registry, outputLines::add, downloadsDir, sharedWorkerPool)) {

                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    UUID connId = client.connectionId().orElseThrow();
                    UUID myClientId = UUID.randomUUID();
                    client.supplySession(connId, new SessionView(myClientId, "racer", connId));

                    Path file = createTestFile("race.txt", 1024);
                    UUID peerRecipient = UUID.randomUUID();

                    ExecutorService executor = Executors.newFixedThreadPool(2);
                    CountDownLatch readyLatch = new CountDownLatch(2);
                    CountDownLatch startLatch = new CountDownLatch(1);

                    Future<Boolean> offerFuture = executor.submit(() -> {
                        readyLatch.countDown();
                        startLatch.await();
                        MessageEnvelope offer = new MessageEnvelope(
                                ProtocolLimits.VERSION,
                                MessageType.FILE_OFFERED,
                                UUID.randomUUID(),
                                null,
                                new MessageBody.FileOffered(UUID.randomUUID(), UUID.randomUUID(), "inbound.txt", 1024)
                        );
                        adapter.transferService().handleControl(offer);
                        return adapter.transferService().receiverState()
                                .filter(state -> state == FileReceiver.ReceiverState.OFFERED)
                                .isPresent();
                    });

                    Future<Boolean> sendFuture = executor.submit(() -> {
                        readyLatch.countDown();
                        startLatch.await();
                        try {
                            registry.execute("/sendfile " + peerRecipient + " \"" + file.toString().replace('\\', '/') + "\"");
                            return true;
                        } catch (CommandRegistry.InputException e) {
                            assertTrue(e.getMessage().contains("already active"));
                            return false;
                        }
                    });

                    assertTrue(readyLatch.await(5, TimeUnit.SECONDS), "Both race tasks must be ready");
                    startLatch.countDown();

                    boolean inboundOfferAccepted = offerFuture.get(5, TimeUnit.SECONDS);
                    boolean outboundSendStarted = sendFuture.get(5, TimeUnit.SECONDS);
                    executor.shutdown();

                    assertNotEquals(outboundSendStarted, inboundOfferAccepted,
                            "Exactly one of the simultaneous transfers must be accepted");
                    assertTrue(adapter.transferService().isTransferActive());

                    boolean senderIsOffering = adapter.transferService().senderState()
                            .filter(state -> state == FileSender.SenderState.OFFERING)
                            .isPresent();
                    boolean receiverHasOffer = adapter.transferService().receiverState()
                            .filter(state -> state == FileReceiver.ReceiverState.OFFERED)
                            .isPresent();
                    assertNotEquals(senderIsOffering, receiverHasOffer,
                            "The client must not hold an outbound and inbound transfer simultaneously");
                    assertEquals(outboundSendStarted, senderIsOffering);
                    assertEquals(inboundOfferAccepted, receiverHasOffer);

                    if (outboundSendStarted) {
                        assertTrue(outputLines.stream().anyMatch(line -> line.contains("Offering file race.txt")));
                        assertTrue(outputLines.stream().anyMatch(line ->
                                line.contains("Rejected") && line.contains("busy")));
                    } else {
                        assertTrue(outputLines.stream().anyMatch(line -> line.contains("Incoming file offer")
                                && line.contains("inbound.txt")));
                    }
                }
            }
        }
    }

    @RepeatedTest(20)
    void oldGenerationSenderReadAndStaleCallbacksCannotAffectNewGenerationAfterReconnect() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            CountDownLatch workerEntered = new CountDownLatch(1);
            CountDownLatch releaseOldWorker = new CountDownLatch(1);
            CountDownLatch oldWorkerFinished = new CountDownLatch(1);
            java.util.concurrent.atomic.AtomicBoolean interceptNextWorker = new java.util.concurrent.atomic.AtomicBoolean(false);
            TransferWorkerPool controlledPool = new TransferWorkerPool(2, 32);

            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(
                    client, registry, (text, gen) -> outputLines.add(gen + ":" + text),
                    downloadsDir, () -> client.session().map(s -> downloadsDir.resolve(s.clientId().toString())).orElse(null),
                    controlledPool)) {

                // Connect Generation 1
                client.connect("127.0.0.1", server.getLocalPort());
                Socket peer1 = server.accept();
                UUID gen1 = client.connectionId().orElseThrow();
                UUID clientId1 = UUID.randomUUID();
                client.supplySession(gen1, new SessionView(clientId1, "alice", gen1));

                Path source1 = createTestFile("gen1.bin", 1024);
                UUID recipient = UUID.randomUUID();
                registry.execute("/sendfile " + recipient + " \"" + source1.toString().replace('\\', '/') + "\"");

                // Read FILE_OFFER from peer1
                peer1.setSoTimeout(3000);
                java.io.DataInputStream dis1 = new java.io.DataInputStream(peer1.getInputStream());
                int len1 = dis1.readInt();
                MessageEnvelope offer1 = (MessageEnvelope) CODEC.decodePayload(java.nio.ByteBuffer.wrap(dis1.readNBytes(len1)));
                assertEquals(MessageType.FILE_OFFER, offer1.type());

                UUID oldTid = UUID.randomUUID();
                // Send ACK(FILE_OFFER) and FILE_STATUS(ACCEPTED)
                sendWireFrame(peer1, new MessageEnvelope(1, MessageType.ACK, UUID.randomUUID(), offer1.messageId(),
                        new MessageBody.Ack(MessageType.FILE_OFFER, AckStatus.ACCEPTED, oldTid)));

                // Occupy both worker threads so the sender chunk read task is queued until we reconnect
                CountDownLatch occupyEntered = new CountDownLatch(2);
                for (int i = 0; i < 2; i++) {
                    controlledPool.submit(() -> {
                        occupyEntered.countDown();
                        try { releaseOldWorker.await(5, TimeUnit.SECONDS); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    });
                }
                assertTrue(occupyEntered.await(2, TimeUnit.SECONDS));

                sendWireFrame(peer1, new MessageEnvelope(1, MessageType.FILE_STATUS, UUID.randomUUID(), null,
                        new MessageBody.FileStatus(oldTid, FileStatusState.ACCEPTED, "accepted")));

                // Wait until FILE_STATUS(ACCEPTED) is dispatched and queues readAndSendNextChunk
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (controlledPool.queueSize() < 1 && System.nanoTime() < deadline) Thread.sleep(5);

                // Disconnect Generation 1 and Reconnect Generation 2
                client.disconnect();
                peer1.close();
                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer2 = server.accept()) {
                    UUID gen2 = client.connectionId().orElseThrow();
                    assertNotEquals(gen1, gen2);
                    UUID clientId2 = UUID.randomUUID();
                    client.supplySession(gen2, new SessionView(clientId2, "alice", gen2));

                    // Release old workers so queued old-generation sender read runs now
                    releaseOldWorker.countDown();
                    long waitIdle = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                    while ((controlledPool.activeThreadCount() > 0 || controlledPool.queueSize() > 0)
                            && System.nanoTime() < waitIdle) Thread.sleep(5);

                    // Inject stale ACK and ERROR frames referencing oldTid over peer2 wire
                    sendWireFrame(peer2, new MessageEnvelope(1, MessageType.ACK, UUID.randomUUID(),
                            UUID.randomUUID(), new MessageBody.Ack(MessageType.FILE_CHUNK, AckStatus.STORED, oldTid)));
                    sendWireFrame(peer2, new MessageEnvelope(1, MessageType.ERROR, UUID.randomUUID(),
                            UUID.randomUUID(), new MessageBody.Error(ErrorCode.TRANSFER_FAILED, "stale error", oldTid)));

                    // Start a fresh transfer on Generation 2
                    Path source2 = createTestFile("gen2.bin", 512);
                    registry.execute("/sendfile " + recipient + " \"" + source2.toString().replace('\\', '/') + "\"");

                    // Peer2 must ONLY receive the new FILE_OFFER, never any old-generation chunk or cancel frame
                    peer2.setSoTimeout(3000);
                    java.io.DataInputStream dis2 = new java.io.DataInputStream(peer2.getInputStream());
                    int len2 = dis2.readInt();
                    OutboundFrame firstOnGen2 = CODEC.decodePayload(java.nio.ByteBuffer.wrap(dis2.readNBytes(len2)));
                    assertInstanceOf(MessageEnvelope.class, firstOnGen2);
                    MessageEnvelope offer2 = (MessageEnvelope) firstOnGen2;
                    assertEquals(MessageType.FILE_OFFER, offer2.type());
                    assertEquals("gen2.bin", ((MessageBody.FileOffer) offer2.body()).fileName());
                }
            } finally {
                releaseOldWorker.countDown();
                controlledPool.close();
            }
        }
    }

    @RepeatedTest(20)
    void oldGenerationReceiverWriteFinalizationAndCleanupDoNotAffectNewGenerationFiles() throws Exception {
        try (ServerSocket server = new ServerSocket(0); TcpClient client = new TcpClient()) {
            CommandRegistry registry = new CommandRegistry();
            CountDownLatch releaseOldWorkers = new CountDownLatch(1);
            TransferWorkerPool controlledPool = new TransferWorkerPool(2, 32);

            try (FileTransferClientAdapter adapter = new FileTransferClientAdapter(
                    client, registry, (text, gen) -> outputLines.add(gen + ":" + text),
                    downloadsDir, () -> client.session().map(s -> downloadsDir.resolve(s.clientId().toString())).orElse(null),
                    controlledPool)) {

                client.connect("127.0.0.1", server.getLocalPort());
                Socket peer1 = server.accept();
                UUID gen1 = client.connectionId().orElseThrow();
                UUID clientId = UUID.randomUUID();
                client.supplySession(gen1, new SessionView(clientId, "bob", gen1));

                UUID oldTid = UUID.randomUUID();
                byte[] oldData = "old-generation-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                sendWireFrame(peer1, new MessageEnvelope(1, MessageType.FILE_OFFERED, UUID.randomUUID(), null,
                        new MessageBody.FileOffered(oldTid, UUID.randomUUID(), "old.bin", oldData.length)));

                long waitOffer = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (!adapter.transferService().isTransferActive() && System.nanoTime() < waitOffer) Thread.sleep(5);

                registry.execute("/accept " + oldTid);
                peer1.setSoTimeout(3000);
                java.io.DataInputStream dis1 = new java.io.DataInputStream(peer1.getInputStream());
                int acceptLen = dis1.readInt();
                MessageEnvelope acceptEnv = (MessageEnvelope) CODEC.decodePayload(java.nio.ByteBuffer.wrap(dis1.readNBytes(acceptLen)));
                assertEquals(MessageType.FILE_ACCEPT, acceptEnv.type());

                // Occupy worker pool so chunk write and finalization are queued across disconnect
                CountDownLatch occupied = new CountDownLatch(2);
                for (int i = 0; i < 2; i++) {
                    controlledPool.submit(() -> {
                        occupied.countDown();
                        try { releaseOldWorkers.await(5, TimeUnit.SECONDS); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    });
                }
                assertTrue(occupied.await(2, TimeUnit.SECONDS));

                // Deliver old chunk and FILE_COMPLETE before disconnect
                sendWireFrame(peer1, new FileChunk(UUID.randomUUID(), oldTid, 0, oldData));
                sendWireFrame(peer1, new MessageEnvelope(1, MessageType.FILE_COMPLETE, UUID.randomUUID(), null,
                        new MessageBody.FileComplete(oldTid, 1, TransferChecksum.computeDigestHex(oldData))));

                long waitQueued = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (controlledPool.queueSize() < 1 && System.nanoTime() < waitQueued) Thread.sleep(5);

                // Disconnect Gen 1 and connect Gen 2 with the same clientId directory
                client.disconnect();
                peer1.close();
                client.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer2 = server.accept()) {
                    UUID gen2 = client.connectionId().orElseThrow();
                    client.supplySession(gen2, new SessionView(clientId, "bob", gen2));

                    // Release old workers
                    releaseOldWorkers.countDown();
                    long waitIdle = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                    while ((controlledPool.activeThreadCount() > 0 || controlledPool.queueSize() > 0)
                            && System.nanoTime() < waitIdle) Thread.sleep(5);

                    // Inject stale binary frame for oldTid over peer2 wire
                    sendWireFrame(peer2, new FileChunk(UUID.randomUUID(), oldTid, 0, oldData));

                    // Now run a complete new-generation download on Gen 2
                    UUID newTid = UUID.randomUUID();
                    byte[] newData = "new-generation-verified-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    String newSha = TransferChecksum.computeDigestHex(newData);

                    sendWireFrame(peer2, new MessageEnvelope(1, MessageType.FILE_OFFERED, UUID.randomUUID(), null,
                            new MessageBody.FileOffered(newTid, UUID.randomUUID(), "new.bin", newData.length)));
                    long waitNewOffer = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                    while (!newTid.equals(adapter.transferService().activeTransferId().orElse(null))
                            && System.nanoTime() < waitNewOffer) Thread.sleep(5);

                    registry.execute("/accept " + newTid);
                    peer2.setSoTimeout(3000);
                    java.io.DataInputStream dis2 = new java.io.DataInputStream(peer2.getInputStream());
                    int lenA = dis2.readInt();
                    MessageEnvelope newAccept = (MessageEnvelope) CODEC.decodePayload(java.nio.ByteBuffer.wrap(dis2.readNBytes(lenA)));
                    assertEquals(MessageType.FILE_ACCEPT, newAccept.type());
                    assertEquals(newTid, ((MessageBody.TransferReference) newAccept.body()).transferId());

                    sendWireFrame(peer2, new FileChunk(UUID.randomUUID(), newTid, 0, newData));
                    sendWireFrame(peer2, new MessageEnvelope(1, MessageType.FILE_COMPLETE, UUID.randomUUID(), null,
                            new MessageBody.FileComplete(newTid, 1, newSha)));

                    int lenR = dis2.readInt();
                    MessageEnvelope newReceived = (MessageEnvelope) CODEC.decodePayload(java.nio.ByteBuffer.wrap(dis2.readNBytes(lenR)));
                    assertEquals(MessageType.FILE_RECEIVED, newReceived.type());
                    assertEquals(newTid, ((MessageBody.FileReceived) newReceived.body()).transferId());

                    sendWireFrame(peer2, new MessageEnvelope(1, MessageType.ACK, UUID.randomUUID(), newReceived.messageId(),
                            new MessageBody.Ack(MessageType.FILE_RECEIVED, AckStatus.ACCEPTED, newTid)));

                    long waitComplete = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                    while (adapter.transferService().isTransferActive() && System.nanoTime() < waitComplete) Thread.sleep(5);

                    Path clientDir = downloadsDir.resolve(clientId.toString());
                    assertFalse(Files.exists(clientDir.resolve(oldTid + ".bin")), "Old generation file must not be published");
                    assertFalse(Files.exists(clientDir.resolve(oldTid + ".part")), "Old generation partial file must be cleaned up");
                    assertTrue(Files.exists(clientDir.resolve(newTid + ".bin")), "New generation file must exist");
                    assertArrayEquals(newData, Files.readAllBytes(clientDir.resolve(newTid + ".bin")));
                }
            } finally {
                releaseOldWorkers.countDown();
                controlledPool.close();
            }
        }
    }
}
