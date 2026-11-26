package com.netsync.app;

import com.netsync.client.NetSyncClient;
import com.netsync.client.TcpClient;
import com.netsync.common.*;
import com.netsync.presence.HeartbeatSender;
import com.netsync.security.CredentialStore;
import com.netsync.transfer.SafeFileStorage;
import com.netsync.transfer.TransferChecksum;
import com.netsync.transfer.TransferWorkerPool;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 6B Unified Production Integration Verification Suite.
 * <p>
 * Exercises the complete production composition ({@link NetSyncServerApplication} and {@link NetSyncClient})
 * across authentication, UDP presence, private TCP chat, consent-based file transfer, lifecycle transitions,
 * failure isolation, and shutdown with pending offers and sustained selector task saturation.
 */
@Timeout(30)
class UnifiedApplicationProductionIntegrationTest {
    private static final FrameCodec CODEC = new FrameCodec(new MessageCodec());

    @TempDir
    Path temp;

    /**
     * B2: Complete Unified End-to-End Scenario with three production clients (Alice, Bob, Carol)
     * demonstrating genuine overlapping operations (chat + UDP presence + Carol responsiveness
     * while an actual multi-chunk file transfer disk write is held open).
     */
    @Test
    void unifiedThreeClientEndToEndScenarioWithOverlappingTransferChatAndPresence() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob", "carol"), 0);
        CredentialStore credentials = DemoAccounts.provision(config, u -> ("disposable-" + u).toCharArray());
        CountDownLatch chunk0Entered = new CountDownLatch(1);
        CountDownLatch releaseChunk0 = new CountDownLatch(1);
        AtomicBoolean holdFirstChunk = new AtomicBoolean(true);

        SafeFileStorage controlledStorage = new SafeFileStorage(temp.resolve("unified-server")) {
            @Override
            public void appendChunk(UUID id, byte[] data, int offset, int length) throws IOException {
                if (holdFirstChunk.compareAndSet(true, false)) {
                    chunk0Entered.countDown();
                    try {
                        if (!releaseChunk0.await(10, TimeUnit.SECONDS)) {
                            throw new IOException("Timed out waiting to release held chunk");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                }
                super.appendChunk(id, data, offset, length);
            }
        };

        try (NetSyncServerApplication app = new NetSyncServerApplication(
                config, credentials, com.netsync.server.NioServer::new, TransferWorkerPool::new,
                System::nanoTime, () -> controlledStorage)) {
            app.start();
            assertTrue(app.isRunning());

            Path aliceDownloads = temp.resolve("alice-downloads");
            Path bobDownloads = temp.resolve("bob-downloads");
            Path carolDownloads = temp.resolve("carol-downloads");

            try (Client alice = client(app, "alice", aliceDownloads);
                 Client bob = client(app, "bob", bobDownloads);
                 Client carol = client(app, "carol", carolDownloads)) {

                // Steps 2-4: Connect and authenticate all three clients; obtain authenticated client IDs
                login(alice, app, "alice");
                login(bob, app, "bob");
                login(carol, app, "carol");

                UUID aliceId = alice.clientId();
                UUID bobId = bob.clientId();
                UUID carolId = carol.clientId();
                assertNotEquals(aliceId, bobId);
                assertNotEquals(bobId, carolId);

                // Step 5: Verify presence queries and notifications
                alice.client.execute("/presence");
                carol.client.execute("/presence");
                await(() -> alice.text().contains("Presence snapshot:")
                        && alice.text().contains("alice [" + aliceId + "]: ONLINE")
                        && alice.text().contains("bob [" + bobId + "]: ONLINE")
                        && alice.text().contains("carol [" + carolId + "]: ONLINE")
                        && carol.text().contains("Presence snapshot:"));

                // Steps 6-8: Alice sends private chat to Bob; Bob replies; Carol receives neither
                String msgAliceToBob = "private-pre-transfer-alice-to-bob-" + UUID.randomUUID();
                String msgBobToAlice = "private-pre-transfer-bob-to-alice-" + UUID.randomUUID();
                alice.client.execute("/chat " + bobId + " " + msgAliceToBob);
                await(() -> bob.text().contains(msgAliceToBob)
                        && alice.text().contains("Queued for recipient " + bobId));
                bob.client.execute("/chat " + aliceId + " " + msgBobToAlice);
                await(() -> alice.text().contains(msgBobToAlice)
                        && bob.text().contains("Queued for recipient " + aliceId));
                assertFalse(carol.text().contains(msgAliceToBob));
                assertFalse(carol.text().contains(msgBobToAlice));

                // Steps 9-11: Alice offers a multi-chunk file to Bob; verify no data transferred before consent
                Path sourceFile = temp.resolve("unified-multi-chunk.bin");
                byte[] sourceBytes = new byte[ProtocolLimits.MAX_CHUNK_BYTES * 2 + 517];
                for (int i = 0; i < sourceBytes.length; i++) {
                    sourceBytes[i] = (byte) ((i * 41 + 13) & 0xFF);
                }
                Files.write(sourceFile, sourceBytes);
                String expectedSha256 = TransferChecksum.computeDigestHex(sourceBytes);

                alice.client.execute("/sendfile " + bobId + " \"" + sourceFile.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID transferId = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();

                assertFalse(controlledStorage.exists(transferId), "No server .part file before recipient consent");
                assertFalse(Files.exists(bobDownloads.resolve(bobId.toString()).resolve(transferId + ".part")));
                assertFalse(Files.exists(bobDownloads.resolve(bobId.toString()).resolve(transferId + ".bin")));

                // Steps 12-13: Bob accepts; hold actual transfer write operation outstanding on worker
                bob.client.execute("/accept " + transferId);
                assertTrue(chunk0Entered.await(5, TimeUnit.SECONDS), "Server chunk 0 write must be held open");

                // Steps 14-16: Exchange chat messages, verify UDP heartbeats, and verify Carol responsiveness during held transfer
                long heartbeatsDuringHeldWrite = app.decodedHeartbeatsForTest();
                String overlapAliceToBob = "overlap-alice-to-bob-" + UUID.randomUUID();
                String overlapCarolToAlice = "overlap-carol-to-alice-" + UUID.randomUUID();

                alice.client.execute("/chat " + bobId + " " + overlapAliceToBob);
                carol.client.execute("/chat " + aliceId + " " + overlapCarolToAlice);
                carol.client.execute("/presence");

                await(() -> bob.text().contains(overlapAliceToBob)
                        && alice.text().contains(overlapCarolToAlice)
                        && carol.text().contains("Queued for recipient " + aliceId));
                assertFalse(carol.text().contains(overlapAliceToBob));
                assertFalse(bob.text().contains(overlapCarolToAlice));
                await(() -> app.decodedHeartbeatsForTest() > heartbeatsDuringHeldWrite);

                // Steps 17-24: Release outstanding transfer write, complete transfer, verify bytes, SHA-256, cleanup, and availability
                releaseChunk0.countDown();
                await(() -> alice.text().contains("[Transfer] Completed [" + transferId)
                        && bob.text().contains("[Transfer] Completed [" + transferId));

                Path finalFile = bobDownloads.resolve(bobId.toString()).resolve(transferId + ".bin");
                assertTrue(Files.exists(finalFile), "Completed receiver file must remain available");
                assertArrayEquals(sourceBytes, Files.readAllBytes(finalFile));
                assertEquals(expectedSha256, TransferChecksum.computeFileDigestHex(finalFile));

                assertEquals(0, app.transferCoordinator().service().registry().activeCount());
                await(() -> !controlledStorage.exists(transferId));
                assertFalse(Files.exists(bobDownloads.resolve(bobId.toString()).resolve(transferId + ".part")));

                // Step 25: Verify shared ACK and ERROR ownership (no generic shell duplicate rendering)
                for (Client c : List.of(alice, bob, carol)) {
                    assertFalse(c.text().contains("Received ACK ["));
                    assertFalse(c.text().contains("Received ERROR ["));
                    assertFalse(c.text().contains("Received CHAT_DELIVER ["));
                    assertFalse(c.text().contains("Received FILE_"));
                }
            } finally {
                releaseChunk0.countDown();
            }
        }
    }

    /**
     * B3: Unified Authentication and Session Transitions covering invalid login, valid login,
     * duplicate login rejection, pending chat and transfer cleanup on logout/disconnect,
     * protected operation rejection after logout, token revocation, reconnect, and fresh chat/transfer.
     */
    @Test
    void unifiedAuthenticationAndSessionTransitionsCleanStateAndIsolateGenerations() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob"), 0);
        CredentialStore credentials = DemoAccounts.provision(config, u -> ("disposable-" + u).toCharArray());

        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, TransferWorkerPool::new,
                System::nanoTime, () -> new SafeFileStorage(temp.resolve("transitions-server")))) {
            app.start();

            // 1. Invalid login fails with generic AUTH_FAILED and closes connection
            try (Client badAlice = clientWithPassword(app, "wrong-password", temp.resolve("bad-alice"))) {
                badAlice.client.execute("/connect 127.0.0.1 " + app.port() + " " + app.udpPort());
                badAlice.client.execute("/login alice");
                await(() -> badAlice.text().contains("AUTH_FAILED") && !badAlice.client.transport().isConnected());
                assertTrue(app.sessions().activeSessions().isEmpty());
            }

            try (Client alice = client(app, "alice", temp.resolve("alice-trans"));
                 Client duplicateAlice = client(app, "alice", temp.resolve("dup-alice"));
                 Client bob = client(app, "bob", temp.resolve("bob-trans"))) {

                // 2. Successful login for Alice and Bob
                login(alice, app, "alice");
                login(bob, app, "bob");
                var aliceCreds1 = alice.client.authentication().presenceCredentials().orElseThrow();
                UUID aliceId1 = aliceCreds1.session().clientId();
                UUID aliceGen1 = aliceCreds1.session().connectionId();
                String aliceToken1 = aliceCreds1.presenceToken();
                UUID bobId = bob.clientId();

                // 3. Duplicate login for active username "alice" is rejected without disturbing original Alice session
                duplicateAlice.client.execute("/connect 127.0.0.1 " + app.port() + " " + app.udpPort());
                duplicateAlice.client.execute("/login alice");
                await(() -> duplicateAlice.text().contains("AUTH_FAILED") && !duplicateAlice.client.transport().isConnected());
                assertTrue(app.sessions().findByClientId(aliceId1).isPresent());

                // 4. Create pending transfer offer from Alice to Bob, then Alice logs out
                Path sample = temp.resolve("logout-cleanup.bin");
                Files.write(sample, "logout-cleanup-bytes".getBytes(StandardCharsets.UTF_8));
                alice.client.execute("/sendfile " + bobId + " \"" + sample.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().isTransferActive());

                alice.client.execute("/logout");
                await(() -> alice.text().contains("Logged out.") && !alice.client.transport().isConnected());

                // Verify token revocation, session removal, and pending transfer cleanup on server and Bob
                assertFalse(app.authentication().isPresenceTokenValid(aliceId1, aliceToken1));
                assertTrue(app.sessions().findByClientId(aliceId1).isEmpty());
                await(() -> app.transferCoordinator().service().registry().activeCount() == 0
                        && !bob.client.fileTransfer().transferService().isTransferActive());

                // 5. Protected operations rejected after logout
                alice.client.execute("/chat " + bobId + " after logout");
                alice.client.execute("/sendfile " + bobId + " \"" + sample.toString().replace('\\', '/') + "\"");
                await(() -> alice.text().contains("Log in before sending chat messages.")
                        && alice.text().contains("Not connected to server."));

                // 6. Reconnect, reauthenticate, disconnect, reconnect again, and perform valid chat + file transfer
                login(alice, app, "alice");
                UUID aliceGen2 = alice.client.transport().connectionId().orElseThrow();
                assertNotEquals(aliceGen1, aliceGen2);
                alice.client.execute("/disconnect");
                await(() -> !alice.client.transport().isConnected());

                login(alice, app, "alice");
                var aliceCreds3 = alice.client.authentication().presenceCredentials().orElseThrow();
                assertNotEquals(aliceId1, aliceCreds3.session().clientId());
                assertNotEquals(aliceToken1, aliceCreds3.presenceToken());

                String freshChat = "fresh-session-chat-" + UUID.randomUUID();
                alice.client.execute("/chat " + bobId + " " + freshChat);
                await(() -> bob.text().contains(freshChat));

                alice.client.execute("/sendfile " + bobId + " \"" + sample.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID newTid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                bob.client.execute("/accept " + newTid);
                await(() -> alice.text().contains("[Transfer] Completed [" + newTid)
                        && bob.text().contains("[Transfer] Completed [" + newTid));
            }
        }
    }

    /**
     * B4: Presence Expiry During Active TCP Operations and Presence Recovery when Heartbeats Resume.
     */
    @Test
    void unifiedPresenceExpiryDuringActiveTcpOperationsAndRecoveryWhenHeartbeatsResume() throws Exception {
        AtomicLong presenceClock = new AtomicLong(1_000_000_000L);
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob"), 0);
        CredentialStore credentials = DemoAccounts.provision(config, u -> ("disposable-" + u).toCharArray());

        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, TransferWorkerPool::new,
                presenceClock::get, () -> new SafeFileStorage(temp.resolve("presence-recovery-server")))) {
            app.start();

            try (Client alice = client(app, "alice", temp.resolve("pres-rec-alice"));
                 Client bob = client(app, "bob", temp.resolve("pres-rec-bob"))) {
                login(alice, app, "alice");
                login(bob, app, "bob");
                var aliceCreds = alice.client.authentication().presenceCredentials().orElseThrow();
                UUID aliceId = aliceCreds.session().clientId();
                UUID bobId = bob.clientId();

                // Stop automatic client heartbeat sender and advance virtual presence clock past 15s timeout
                alice.client.presence().close();
                presenceClock.addAndGet(TimeUnit.SECONDS.toNanos(ProtocolLimits.PRESENCE_TIMEOUT_SECONDS + 2));
                app.presenceTracker().checkTimeouts();
                assertEquals(PresenceState.OFFLINE, app.presenceTracker().getPresenceState(aliceId).orElseThrow());
                await(() -> bob.text().contains("Presence: alice [" + aliceId + "] is OFFLINE."));

                // Verify TCP authentication, private chat, and file transfer remain functional while OFFLINE in UDP tracker
                String chatWhileOffline = "chat-while-udp-offline-" + UUID.randomUUID();
                alice.client.execute("/chat " + bobId + " " + chatWhileOffline);
                await(() -> bob.text().contains(chatWhileOffline));

                Path file = temp.resolve("transfer-while-udp-offline.bin");
                byte[] expected = "transfer-while-udp-offline-bytes".getBytes(StandardCharsets.UTF_8);
                Files.write(file, expected);
                alice.client.execute("/sendfile " + bobId + " \"" + file.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID tid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                bob.client.execute("/accept " + tid);
                await(() -> alice.text().contains("[Transfer] Completed [" + tid)
                        && bob.text().contains("[Transfer] Completed [" + tid));
                assertArrayEquals(expected, Files.readAllBytes(
                        temp.resolve("pres-rec-bob").resolve(bobId.toString()).resolve(tid + ".bin")));

                // Resume heartbeat activity with higher sequences and verify ONLINE recovery
                try (HeartbeatSender resumed = new HeartbeatSender(
                        new InetSocketAddress("127.0.0.1", app.udpPort()), aliceId, aliceCreds.presenceToken())) {
                    // Send a sequence strictly higher than any earlier client heartbeat
                    for (int i = 0; i < 5; i++) {
                        app.presenceTracker().processHeartbeat(new Heartbeat(
                                ProtocolLimits.VERSION, "HEARTBEAT", aliceId, aliceCreds.presenceToken(), 100 + i));
                    }
                    resumed.start();
                    assertEquals(PresenceState.ONLINE, app.presenceTracker().getPresenceState(aliceId).orElseThrow());
                    await(() -> bob.text().contains("Presence: alice [" + aliceId + "] is ONLINE."));
                }
            }
        }
    }

    /**
     * B5: Cross-Module Failure Isolation — one misbehaving client sending duplicate IDs,
     * unauthorized requests, third-party transfer injection, and fatal malformed frames
     * is isolated and closed without disrupting an active transfer and chat between healthy clients.
     */
    @Test
    void unifiedFailureIsolationProtectsHealthyClientsFromMisbehavingPeer() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob", "carol"), 0);
        CredentialStore credentials = DemoAccounts.provision(config, u -> ("disposable-" + u).toCharArray());

        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, TransferWorkerPool::new,
                System::nanoTime, () -> new SafeFileStorage(temp.resolve("isolation-server")))) {
            app.start();

            try (Client alice = client(app, "alice", temp.resolve("iso-alice"));
                 Client bob = client(app, "bob", temp.resolve("iso-bob"));
                 Socket attacker = new Socket("127.0.0.1", app.port())) {
                attacker.setSoTimeout(4000);
                attacker.setTcpNoDelay(true);

                login(alice, app, "alice");
                login(bob, app, "bob");
                UUID bobId = bob.clientId();

                // 1. Attacker sends unauthenticated request -> UNAUTHENTICATED
                UUID unauthId = UUID.randomUUID();
                writeRawFrame(attacker, new MessageEnvelope(1, MessageType.PRESENCE_QUERY, unauthId, null, new MessageBody.Empty()));
                assertEquals(ErrorCode.UNAUTHENTICATED, ((MessageBody.Error) readUntilReply(attacker, unauthId).body()).code());

                // 2. Attacker authenticates as Carol, then sends duplicate message ID -> DUPLICATE_MESSAGE
                UUID loginId = UUID.randomUUID();
                writeRawFrame(attacker, new MessageEnvelope(1, MessageType.LOGIN_REQUEST, loginId, null,
                        new MessageBody.LoginRequest("carol", "disposable-carol")));
                assertEquals(MessageType.LOGIN_RESPONSE, readUntilReply(attacker, loginId).type());

                UUID dupQueryId = UUID.randomUUID();
                MessageEnvelope queryEnv = new MessageEnvelope(1, MessageType.PRESENCE_QUERY, dupQueryId, null, new MessageBody.Empty());
                writeRawFrame(attacker, queryEnv);
                assertEquals(MessageType.PRESENCE_SNAPSHOT, readUntilReply(attacker, dupQueryId).type());
                writeRawFrame(attacker, queryEnv);
                assertEquals(ErrorCode.DUPLICATE_MESSAGE, ((MessageBody.Error) readUntilReply(attacker, dupQueryId).body()).code());

                // 3. Alice offers a file to Bob; Attacker attempts third-party cancel and binary injection, then sends malformed JSON
                Path source = temp.resolve("isolated-transfer.bin");
                byte[] payload = "healthy-transfer-isolated-from-attacker".getBytes(StandardCharsets.UTF_8);
                Files.write(source, payload);
                alice.client.execute("/sendfile " + bobId + " \"" + source.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID tid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();

                UUID hijackCancelId = UUID.randomUUID();
                writeRawFrame(attacker, new MessageEnvelope(1, MessageType.FILE_CANCEL, hijackCancelId, null,
                        new MessageBody.TransferReference(tid)));
                assertEquals(ErrorCode.FORBIDDEN, ((MessageBody.Error) readUntilReply(attacker, hijackCancelId).body()).code());

                // Attacker sends fatal malformed JSON frame -> receives MALFORMED_MESSAGE and is disconnected
                attacker.getOutputStream().write(new byte[]{0, 0, 0, 2, 1, '{'});
                attacker.getOutputStream().flush();

                // 4. Alice and Bob's pending file offer and chat complete without disruption!
                bob.client.execute("/accept " + tid);
                String healthyChat = "healthy-chat-after-attacker-kicked-" + UUID.randomUUID();
                alice.client.execute("/chat " + bobId + " " + healthyChat);

                await(() -> alice.text().contains("[Transfer] Completed [" + tid)
                        && bob.text().contains("[Transfer] Completed [" + tid)
                        && bob.text().contains(healthyChat));
                assertArrayEquals(payload, Files.readAllBytes(
                        temp.resolve("iso-bob").resolve(bobId.toString()).resolve(tid + ".bin")));
            }
        }
    }

    /**
     * A7 / B7: Shutdown with Pending Unanswered File Offers and Sustained Selector Task Rejection.
     */
    @RepeatedTest(20)
    void unifiedShutdownWithPendingUnansweredFileOfferAndSustainedPresenceTaskRejectionTerminatesCleanly() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob"), 0);
        CredentialStore credentials = DemoAccounts.provision(config, u -> ("disposable-" + u).toCharArray());
        Path storageRoot = temp.resolve("pending-offer-shutdown-server");

        NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, TransferWorkerPool::new,
                System::nanoTime, () -> new SafeFileStorage(storageRoot));
        app.start();

        UUID pendingOfferTid;
        Client alice = client(app, "alice", temp.resolve("pend-alice"));
        Client bob = client(app, "bob", temp.resolve("pend-bob"));
        try {
            login(alice, app, "alice");
            login(bob, app, "bob");
            UUID bobId = bob.clientId();

            Path source = temp.resolve("unanswered-offer.bin");
            Files.write(source, "unanswered-offer-payload".getBytes(StandardCharsets.UTF_8));
            alice.client.execute("/sendfile " + bobId + " \"" + source.toString().replace('\\', '/') + "\"");
            await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
            pendingOfferTid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
            assertEquals(1, app.transferCoordinator().service().registry().activeCount());

            // Hold selector thread briefly and fill selector task queue to trigger sustained task rejection right as shutdown begins
            CountDownLatch selectorEntered = new CountDownLatch(1);
            CountDownLatch releaseSelector = new CountDownLatch(1);
            assertTrue(app.serverForTest().selectorTasks().submit(() -> {
                selectorEntered.countDown();
                try { releaseSelector.await(2, TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }));
            assertTrue(selectorEntered.await(2, TimeUnit.SECONDS));
            while (app.serverForTest().selectorTasks().submit(() -> {})) {
                // saturate selector task queue
            }

            // Trigger presence change while selector task queue is saturated
            app.presenceTracker().revokeSession(bobId);

            // Initiate application shutdown while offer is pending and selector queue was saturated
            CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(app::close);
            releaseSelector.countDown();
            closeFuture.get(5, TimeUnit.SECONDS);

            // Verify all server resources and pending offer state are cleaned up without false completion
            assertFalse(app.isRunning());
            assertEquals(0, app.transferCoordinator().service().registry().activeCount());
            assertTrue(app.sessions().activeSessions().isEmpty());
            assertTrue(app.workers().isTerminated());
            assertTrue(app.transferTimeoutsForTest().isShutdown());
            assertFalse(app.transferStorage().exists(pendingOfferTid));

            await(() -> !alice.client.transport().isConnected() && !bob.client.transport().isConnected());
            assertFalse(alice.text().contains("[Transfer] Completed"));
            assertFalse(bob.text().contains("[Transfer] Completed"));
        } finally {
            alice.close();
            bob.close();
            app.close();
        }
    }

    private static Client client(NetSyncServerApplication app, String username, Path downloads) {
        return clientWithPassword(app, "disposable-" + username, downloads);
    }

    private static Client clientWithPassword(NetSyncServerApplication app, String password, Path downloads) {
        StringWriter writer = new StringWriter();
        NetSyncClient client = new NetSyncClient(new TcpClient(), new PrintWriter(writer, true),
                password::toCharArray, app.udpPort(), downloads);
        return new Client(client, writer);
    }

    private static void login(Client client, NetSyncServerApplication app, String username) {
        client.client.execute("/connect 127.0.0.1 " + app.port() + " " + app.udpPort());
        client.client.execute("/login " + username);
        await(() -> client.client.authentication().presenceCredentials().isPresent(), client::text);
    }

    private static void writeRawFrame(Socket socket, OutboundFrame frame) throws Exception {
        ByteBuffer buf = CODEC.encode(frame);
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        socket.getOutputStream().write(bytes);
        socket.getOutputStream().flush();
    }

    private static MessageEnvelope readUntilReply(Socket socket, UUID replyTo) throws Exception {
        DataInputStream in = new DataInputStream(socket.getInputStream());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            int len = CODEC.decodeLength(ByteBuffer.wrap(in.readNBytes(4)));
            byte[] payload = in.readNBytes(len);
            assertEquals(len, payload.length);
            OutboundFrame frame = CODEC.decodePayload(ByteBuffer.wrap(payload));
            if (frame instanceof MessageEnvelope env && replyTo.equals(env.replyTo())) {
                return env;
            }
        }
        throw new AssertionError("Timed out waiting for replyTo " + replyTo);
    }

    private static void await(BooleanSupplier condition) {
        await(condition, () -> "");
    }

    private static void await(BooleanSupplier condition, Supplier<String> detail) {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (!condition.getAsBoolean() && System.nanoTime() < until) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted");
            }
        }
        assertTrue(condition.getAsBoolean(), "Timed out waiting for condition: " + detail.get());
    }

    private record Client(NetSyncClient client, StringWriter writer) implements AutoCloseable {
        String text() {
            synchronized (writer) {
                return writer.toString();
            }
        }

        UUID clientId() {
            return client.authentication().presenceCredentials().orElseThrow().session().clientId();
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
