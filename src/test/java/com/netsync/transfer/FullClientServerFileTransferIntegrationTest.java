package com.netsync.transfer;

import com.netsync.client.ClientDispatcher;
import com.netsync.client.CommandRegistry;
import com.netsync.client.TcpClient;
import com.netsync.common.*;
import com.netsync.security.AuthService;
import com.netsync.security.AuthorizationServiceImpl;
import com.netsync.security.CredentialStore;
import com.netsync.security.LoginHandler;
import com.netsync.security.LogoutHandler;
import com.netsync.security.SessionRegistryImpl;
import com.netsync.security.SessionTokenGenerator;
import com.netsync.server.MessageDispatcher;
import com.netsync.server.NioServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration test verifying complete client-server file transfers.
 * <p>
 * Boots a live {@link NioServer} wired with real {@link FileTransferCoordinator},
 * {@link SessionRegistryImpl}, and {@link AuthorizationServiceImpl}.
 * Connects two real {@link TcpClient} instances that perform full network-based
 * authentication and execute file transfers via {@link FileTransferClientAdapter}
 * using CLI commands ({@code /sendfile}, {@code /accept}, {@code /reject}, {@code /cancel}).
 */
@Timeout(30)
class FullClientServerFileTransferIntegrationTest {

    @TempDir
    Path tempDir;

    private Path serverStorageDir;
    private Path downloadsDirA;
    private Path downloadsDirB;
    private Path sourceDir;

    private List<String> outputLinesA;
    private List<String> outputLinesB;

    private TransferWorkerPool serverWorkerPool;
    private TransferWorkerPool clientWorkerPool;

    private NioServer server;
    private SessionRegistryImpl serverSessionRegistry;
    private AuthService serverAuthService;

    private TcpClient clientA;
    private TcpClient clientB;
    private CommandRegistry registryA;
    private CommandRegistry registryB;
    private FileTransferClientAdapter adapterA;
    private FileTransferClientAdapter adapterB;
    private SessionView sessionA;
    private SessionView sessionB;

    @BeforeEach
    void setUp() throws Exception {
        serverStorageDir = tempDir.resolve("server_storage");
        downloadsDirA = tempDir.resolve("downloads_a");
        downloadsDirB = tempDir.resolve("downloads_b");
        sourceDir = tempDir.resolve("source");

        Files.createDirectories(serverStorageDir);
        Files.createDirectories(downloadsDirA);
        Files.createDirectories(downloadsDirB);
        Files.createDirectories(sourceDir);

        outputLinesA = new CopyOnWriteArrayList<>();
        outputLinesB = new CopyOnWriteArrayList<>();

        serverWorkerPool = new TransferWorkerPool(2, 32);
        clientWorkerPool = new TransferWorkerPool(2, 16);

        // 1. Configure live server with authentication and file-transfer coordination
        serverSessionRegistry = new SessionRegistryImpl();
        CredentialStore credentialStore = new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("alice", "pass-alice".toCharArray()),
                new CredentialStore.DemoAccountSpec("bob", "pass-bob".toCharArray())
        ));
        serverAuthService = new AuthService(credentialStore, serverSessionRegistry, new SessionTokenGenerator());
        AuthorizationServiceImpl authorizationService = new AuthorizationServiceImpl(serverSessionRegistry);

        TransferRegistry transferRegistry = new TransferRegistry();
        SafeFileStorage serverStorage = new SafeFileStorage(serverStorageDir);

        AtomicReference<NioServer> serverRef = new AtomicReference<>();
        OutboundSink outboundSink = (connId, frame) -> {
            NioServer s = serverRef.get();
            return s != null ? s.send(connId, frame) : EnqueueResult.REJECTED;
        };

        AtomicReference<SelectorTasks> selectorTasksRef = new AtomicReference<>();
        SelectorTasks selectorTasks = new SelectorTasks() {
            @Override
            public boolean submit(Runnable task) {
                SelectorTasks t = selectorTasksRef.get();
                return t != null && t.submit(task);
            }

            @Override
            public Optional<Reservation> tryReserveCompletion() {
                SelectorTasks t = selectorTasksRef.get();
                return t != null ? t.tryReserveCompletion() : Optional.empty();
            }
        };

        FileTransferService transferService = new FileTransferService(
                transferRegistry,
                serverStorage,
                serverWorkerPool,
                outboundSink,
                selectorTasks,
                serverSessionRegistry,
                authorizationService
        );
        FileTransferCoordinator coordinator = new FileTransferCoordinator(transferService);

        server = new NioServer(0, nio -> {
            serverRef.set(nio);
            selectorTasksRef.set(nio.selectorTasks());
            nio.withSessionRegistry(serverSessionRegistry);

            LoginHandler login = new LoginHandler(
                    serverAuthService,
                    serverWorkerPool::submit,
                    nio.selectorTasks(),
                    nio,
                    nio,
                    res -> {},
                    sess -> {}
            );
            LogoutHandler logout = new LogoutHandler(
                    serverAuthService,
                    authorizationService,
                    nio,
                    nio,
                    sess -> {}
            );
            nio.addDisconnectListener(login::onDisconnected);
            nio.addDisconnectListener(coordinator::onDisconnected);

            return MessageDispatcher.builder()
                    .register(MessageType.LOGIN_REQUEST, login)
                    .register(MessageType.LOGOUT_REQUEST, logout)
                    .register(MessageType.FILE_OFFER, coordinator::handleControl)
                    .register(MessageType.FILE_ACCEPT, coordinator::handleControl)
                    .register(MessageType.FILE_REJECT, coordinator::handleControl)
                    .register(MessageType.FILE_COMPLETE, coordinator::handleControl)
                    .register(MessageType.FILE_RECEIVED, coordinator::handleControl)
                    .register(MessageType.FILE_CANCEL, coordinator::handleControl)
                    .build(nio);
        }, coordinator::handleChunk);

        server.start();

        // 2. Connect and authenticate Client A (Alice)
        clientA = new TcpClient();
        clientA.connect("127.0.0.1", server.port());
        sessionA = authenticateClient(clientA, "alice", "pass-alice");
        registryA = new CommandRegistry();
        adapterA = new FileTransferClientAdapter(clientA, registryA, outputLinesA::add, downloadsDirA, clientWorkerPool);

        // 3. Connect and authenticate Client B (Bob)
        clientB = new TcpClient();
        clientB.connect("127.0.0.1", server.port());
        sessionB = authenticateClient(clientB, "bob", "pass-bob");
        registryB = new CommandRegistry();
        adapterB = new FileTransferClientAdapter(clientB, registryB, outputLinesB::add, downloadsDirB, clientWorkerPool);
    }

    @AfterEach
    void tearDown() {
        if (adapterA != null) try { adapterA.close(); } catch (Exception ignored) {}
        if (adapterB != null) try { adapterB.close(); } catch (Exception ignored) {}
        if (clientA != null) try { clientA.close(); } catch (Exception ignored) {}
        if (clientB != null) try { clientB.close(); } catch (Exception ignored) {}
        if (server != null) try { server.close(); } catch (Exception ignored) {}
        if (serverWorkerPool != null) try { serverWorkerPool.close(); } catch (Exception ignored) {}
        if (clientWorkerPool != null) try { clientWorkerPool.close(); } catch (Exception ignored) {}
    }

    private SessionView authenticateClient(TcpClient client, String username, String password) throws Exception {
        UUID connId = client.connectionId().orElseThrow();
        UUID reqId = UUID.randomUUID();
        CompletableFuture<SessionView> sessionFuture = new CompletableFuture<>();

        ClientDispatcher.Registration reg = client.dispatcher().register(MessageType.LOGIN_RESPONSE, env -> {
            if (reqId.equals(env.replyTo()) && env.body() instanceof MessageBody.LoginResponse res) {
                sessionFuture.complete(new SessionView(res.clientId(), username, connId));
            }
        });

        try {
            client.sendOrThrow(new MessageEnvelope(
                    ProtocolLimits.VERSION,
                    MessageType.LOGIN_REQUEST,
                    reqId,
                    null,
                    new MessageBody.LoginRequest(username, password)
            ));
            SessionView session = sessionFuture.get(5, TimeUnit.SECONDS);
            client.supplySession(connId, session);
            return session;
        } finally {
            reg.close();
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

    private static void awaitCondition(BooleanSupplier condition, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new AssertionError("Condition not met within " + timeoutMs + " ms");
            }
            Thread.sleep(20);
        }
    }

    @Test
    void test1_endToEndFileTransferSuccessful() throws Exception {
        // 1. Alice creates test file (24 KiB = 6 chunks of 4 KiB each)
        Path sourceFile = createTestFile("payload.dat", 24 * 1024);
        byte[] expectedBytes = Files.readAllBytes(sourceFile);
        String expectedChecksum = TransferChecksum.computeFileDigestHex(sourceFile);

        // 2. Alice sends file offer to Bob via CLI command
        registryA.execute("/sendfile " + sessionB.clientId() + " \"" + sourceFile.toString().replace('\\', '/') + "\"");

        // 3. Wait until Bob receives the offer
        awaitCondition(() -> adapterB.transferService().isTransferActive(), 5000);
        UUID transferId = adapterB.transferService().activeTransferId().orElseThrow();

        // 4. Bob accepts the offer via CLI command
        registryB.execute("/accept " + transferId);

        // 5. Wait until transfer completes on both clients
        awaitCondition(() -> !adapterA.transferService().isTransferActive()
                && !adapterB.transferService().isTransferActive(), 10000);

        // 6. Verify received file on disk
        Path receivedFile = downloadsDirB.resolve("bob").resolve(transferId + ".bin");
        assertTrue(Files.exists(receivedFile), "Received file must exist in downloads folder: " + outputLinesB);
        assertEquals(expectedBytes.length, Files.size(receivedFile), "File sizes must match");

        byte[] actualBytes = Files.readAllBytes(receivedFile);
        assertArrayEquals(expectedBytes, actualBytes, "File bytes must match original exactly");

        String actualChecksum = TransferChecksum.computeFileDigestHex(receivedFile);
        assertEquals(expectedChecksum, actualChecksum, "SHA-256 checksums must match");

        // 7. Verify no partial (.part) files remain
        try (var stream = Files.list(downloadsDirB.resolve("bob"))) {
            assertTrue(stream.noneMatch(p -> p.toString().endsWith(".part")), "No .part files should remain");
        }
    }

    @Test
    void test2_offerRejectionWorkflow() throws Exception {
        Path sourceFile = createTestFile("reject_me.txt", 1024);

        // Alice sends offer
        registryA.execute("/sendfile " + sessionB.clientId() + " \"" + sourceFile.toString().replace('\\', '/') + "\"");

        // Bob receives offer
        awaitCondition(() -> adapterB.transferService().isTransferActive(), 5000);
        UUID transferId = adapterB.transferService().activeTransferId().orElseThrow();

        // Bob rejects offer via CLI
        registryB.execute("/reject " + transferId);

        // Both clients transition back to idle
        awaitCondition(() -> !adapterA.transferService().isTransferActive()
                && !adapterB.transferService().isTransferActive(), 5000);

        // Verify rejection messages in terminal logs
        assertTrue(outputLinesB.stream().anyMatch(l -> l.contains("Rejected offer for transfer " + transferId)));
        assertTrue(outputLinesA.stream().anyMatch(l -> l.contains("Rejected") && l.contains(transferId.toString())));

        // No downloaded file exists
        assertFalse(Files.exists(downloadsDirB.resolve("bob").resolve("reject_me.txt")));
    }

    @Test
    void test3_transferCancellationBySender() throws Exception {
        // Larger file (64 KiB) to give time for in-flight cancellation
        Path sourceFile = createTestFile("cancel_me.bin", 64 * 1024);

        registryA.execute("/sendfile " + sessionB.clientId() + " \"" + sourceFile.toString().replace('\\', '/') + "\"");
        awaitCondition(() -> adapterB.transferService().isTransferActive(), 5000);
        UUID transferId = adapterB.transferService().activeTransferId().orElseThrow();

        // Bob accepts
        registryB.execute("/accept " + transferId);

        // Alice cancels the transfer
        registryA.execute("/cancel " + transferId);

        // Both clients return to idle
        awaitCondition(() -> !adapterA.transferService().isTransferActive()
                && !adapterB.transferService().isTransferActive(), 5000);

        // Verify partial files deleted
        Path destDir = downloadsDirB.resolve("bob");
        if (Files.exists(destDir)) {
            try (var stream = Files.list(destDir)) {
                assertTrue(stream.noneMatch(p -> p.toString().endsWith(".part")), "Partial files must be cleaned up");
            }
        }
    }

    @Test
    void test4_disconnectDuringActiveTransferCleansUpPartialFile() throws Exception {
        // 256 KiB file
        Path sourceFile = createTestFile("disconnect_me.dat", 256 * 1024);

        registryA.execute("/sendfile " + sessionB.clientId() + " \"" + sourceFile.toString().replace('\\', '/') + "\"");
        awaitCondition(() -> adapterB.transferService().isTransferActive(), 5000);
        UUID transferId = adapterB.transferService().activeTransferId().orElseThrow();

        registryB.execute("/accept " + transferId);

        // Wait until Bob is actively receiving chunks
        awaitCondition(() -> adapterB.transferService().receiverState()
                .filter(s -> s == FileReceiver.ReceiverState.RECEIVING).isPresent(), 5000);

        // Abruptly disconnect Alice
        clientA.disconnect();

        // Bob's connection monitor or server abort cleans up Bob's active transfer
        awaitCondition(() -> !adapterB.transferService().isTransferActive(), 5000);

        // Verify partial files deleted on Bob's side
        Path destDir = downloadsDirB.resolve("bob");
        if (Files.exists(destDir)) {
            try (var stream = Files.list(destDir)) {
                assertTrue(stream.noneMatch(p -> p.toString().endsWith(".part")), "Partial file must be deleted on disconnect");
            }
        }
    }

    @Test
    void test5_newTransferSucceedsAfterReconnection() throws Exception {
        // Disconnect Alice and reconnect
        clientA.disconnect();
        awaitCondition(() -> !clientA.isConnected(), 3000);

        // Reconnect and re-authenticate Alice
        clientA.connect("127.0.0.1", server.port());
        sessionA = authenticateClient(clientA, "alice", "pass-alice");

        // Alice sends a new file to Bob
        Path sourceFile = createTestFile("recovered.txt", 8 * 1024);
        byte[] expectedBytes = Files.readAllBytes(sourceFile);

        registryA.execute("/sendfile " + sessionB.clientId() + " \"" + sourceFile.toString().replace('\\', '/') + "\"");

        awaitCondition(() -> adapterB.transferService().isTransferActive(), 5000);
        UUID transferId = adapterB.transferService().activeTransferId().orElseThrow();

        registryB.execute("/accept " + transferId);

        awaitCondition(() -> !adapterA.transferService().isTransferActive()
                && !adapterB.transferService().isTransferActive(), 10000);

        // Verify downloaded file
        Path receivedFile = downloadsDirB.resolve("bob").resolve(transferId + ".bin");
        assertTrue(Files.exists(receivedFile), "Recovered transfer file must exist");
        assertArrayEquals(expectedBytes, Files.readAllBytes(receivedFile), "File content must match after reconnect");
    }
}
