package com.netsync.app;

import com.netsync.client.NetSyncClient;
import com.netsync.client.TcpClient;
import com.netsync.common.*;
import com.netsync.security.CredentialStore;
import com.netsync.transfer.SafeFileStorage;
import com.netsync.transfer.TransferChecksum;
import com.netsync.transfer.TransferWorkerPool;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class FileTransferProductionIntegrationTest {
    private static final FrameCodec CODEC = new FrameCodec(new MessageCodec());
    @TempDir Path temp;

    @Test
    void productionClientsTransferMultiChunkFileWithConsentAndVerifiedBytes() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob"), 0);
        CredentialStore credentials = DemoAccounts.provision(config,
                username -> ("disposable-" + username).toCharArray());
        Path aliceDownloads = temp.resolve("alice");
        Path bobDownloads = temp.resolve("bob");
        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials)) {
            app.start();
            try (var alice = client(app, "alice", aliceDownloads);
                 var bob = client(app, "bob", bobDownloads)) {
                login(alice, app, "alice");
                login(bob, app, "bob");
                UUID bobId = bob.client.authentication().presenceCredentials().orElseThrow().session().clientId();
                Path source = temp.resolve("multi chunk sample.bin");
                byte[] content = new byte[ProtocolLimits.MAX_CHUNK_BYTES * 2 + 37];
                for (int i = 0; i < content.length; i++) content[i] = (byte) (i * 31 + 7);
                Files.write(source, content);

                alice.client.execute("/sendfile " + bobId + " \"" + source.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent(),
                        () -> "alice=" + alice.text() + " bob=" + bob.text());
                UUID transferId = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                assertFalse(Files.exists(bobDownloads.resolve(bobId.toString()).resolve(transferId + ".bin")),
                        "The recipient must consent before a completed file appears");

                bob.client.execute("/accept " + transferId);
                await(() -> alice.text().contains("[Transfer] Completed [" + transferId)
                        && bob.text().contains("[Transfer] Completed [" + transferId));

                Path received = bobDownloads.resolve(bobId.toString()).resolve(transferId + ".bin");
                assertArrayEquals(content, Files.readAllBytes(received));
                assertEquals(app.transferCoordinator().service().registry().activeCount(), 0);
                assertFalse(Files.exists(app.transferStorage().resolvePartFile(transferId)));
                assertTrue(alice.client.commands().descriptions().stream().anyMatch(c -> c.name().equals("/sendfile")));
            }
        }
    }

    @Test
    void productionChatPresenceAndThirdClientStayResponsiveDuringBlockedTransferWrite() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob", "carol"), 0);
        CredentialStore credentials = DemoAccounts.provision(config,
                username -> ("disposable-" + username).toCharArray());
        CountDownLatch writeEntered = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        SafeFileStorage blockingStorage = new SafeFileStorage(temp.resolve("blocked-server")) {
            @Override public void appendChunk(UUID id, byte[] data, int offset, int length) throws IOException {
                if (writeEntered.getCount() > 0) {
                    writeEntered.countDown();
                    try {
                        if (!releaseWrite.await(8, TimeUnit.SECONDS)) throw new IOException("controlled write timed out");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt(); throw new IOException(interrupted);
                    }
                }
                super.appendChunk(id, data, offset, length);
            }
        };
        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, com.netsync.transfer.TransferWorkerPool::new,
                System::nanoTime, () -> blockingStorage)) {
            app.start();
            try (var alice = client(app, "alice", temp.resolve("alice-root"));
                 var bob = client(app, "bob", temp.resolve("bob-root"));
                 var carol = client(app, "carol", temp.resolve("carol-root"))) {
                login(alice, app, "alice"); login(bob, app, "bob"); login(carol, app, "carol");
                UUID bobId = bob.client.authentication().presenceCredentials().orElseThrow().session().clientId();
                Path source = temp.resolve("overlap.bin");
                byte[] content = new byte[ProtocolLimits.MAX_CHUNK_BYTES * 2 + 11];
                java.util.Arrays.fill(content, (byte) 37);
                Files.write(source, content);
                alice.client.execute("/sendfile " + bobId + " \"" + source.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID transferId = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                bob.client.execute("/accept " + transferId);
                assertTrue(writeEntered.await(5, TimeUnit.SECONDS), "The real server worker write did not block");

                long heartbeatsBefore = app.decodedHeartbeatsForTest();
                String chatMarker = "chat-during-transfer-" + UUID.randomUUID();
                alice.client.execute("/chat " + bobId + " " + chatMarker);
                alice.client.execute("/presence");
                carol.client.execute("/presence");
                await(() -> bob.text().contains(chatMarker)
                        && alice.text().contains("Presence snapshot:")
                        && carol.text().contains("Presence snapshot:"));
                await(() -> app.decodedHeartbeatsForTest() > heartbeatsBefore);

                releaseWrite.countDown();
                await(() -> alice.text().contains("[Transfer] Completed [" + transferId)
                        && bob.text().contains("[Transfer] Completed [" + transferId));
                UUID bobIdentity = bob.client.authentication().presenceCredentials().orElseThrow().session().clientId();
                Path received = temp.resolve("bob-root").resolve(bobIdentity.toString()).resolve(transferId + ".bin");
                assertArrayEquals(content, Files.readAllBytes(received));
            } finally { releaseWrite.countDown(); }
        }
    }

    @Test
    void productionTimeoutSchedulerExpiresAnUnansweredOffer() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob"), 0);
        CredentialStore credentials = DemoAccounts.provision(config,
                username -> ("disposable-" + username).toCharArray());
        AtomicLong transferClock = new AtomicLong(100_000L);
        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, com.netsync.transfer.TransferWorkerPool::new,
                System::nanoTime, () -> new SafeFileStorage(temp.resolve("timeout-server")), transferClock::get)) {
            app.start();
            try (var alice = client(app, "alice", temp.resolve("timeout-alice"));
                 var bob = client(app, "bob", temp.resolve("timeout-bob"))) {
                login(alice, app, "alice"); login(bob, app, "bob");
                UUID bobId = bob.client.authentication().presenceCredentials().orElseThrow().session().clientId();
                Path source = temp.resolve("timeout-offer.bin");
                Files.write(source, new byte[]{1, 2, 3});
                alice.client.execute("/sendfile " + bobId + " " + source.toString().replace('\\', '/') );
                await(() -> app.transferCoordinator().service().registry().activeCount() == 1);
                transferClock.addAndGet(TimeUnit.SECONDS.toMillis(ProtocolLimits.TRANSFER_TIMEOUT_SECONDS));
                await(() -> app.transferCoordinator().service().registry().activeCount() == 0
                        && alice.text().contains("[Transfer] Failed")
                        && bob.text().contains("[Transfer] Failed"));
            }
        }
    }

    @RepeatedTest(30)
    void productionReconnectDoesNotReviveAnOldBlockedTransferWrite() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob"), 0);
        CredentialStore credentials = DemoAccounts.provision(config,
                username -> ("disposable-" + username).toCharArray());
        CountDownLatch writeEntered = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        CountDownLatch cleanupFinished = new CountDownLatch(1);
        AtomicBoolean blockFirstWrite = new AtomicBoolean(true);
        SafeFileStorage storage = new SafeFileStorage(temp.resolve("reconnect-server")) {
            @Override public void appendChunk(UUID id, byte[] data, int offset, int length) throws IOException {
                if (blockFirstWrite.compareAndSet(true, false)) {
                    writeEntered.countDown();
                    try {
                        if (!releaseWrite.await(8, TimeUnit.SECONDS)) throw new IOException("controlled write timed out");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt(); throw new IOException(interrupted);
                    }
                }
                super.appendChunk(id, data, offset, length);
            }
            @Override public boolean deletePartialFile(UUID id) {
                boolean deleted = super.deletePartialFile(id);
                if (deleted) cleanupFinished.countDown();
                return deleted;
            }
        };
        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, com.netsync.transfer.TransferWorkerPool::new,
                System::nanoTime, () -> storage)) {
            app.start();
            try (var alice = client(app, "alice", temp.resolve("reconnect-alice"));
                 var bob = client(app, "bob", temp.resolve("reconnect-bob"))) {
                login(alice, app, "alice"); login(bob, app, "bob");
                UUID bobId = bob.client.authentication().presenceCredentials().orElseThrow().session().clientId();
                UUID oldAliceId = alice.client.authentication().presenceCredentials().orElseThrow().session().clientId();
                Path first = temp.resolve("generation-a.bin");
                Files.write(first, new byte[ProtocolLimits.MAX_CHUNK_BYTES + 7]);
                alice.client.execute("/sendfile " + bobId + " " + first.toString().replace('\\', '/'));
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID oldTransferId = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                bob.client.execute("/accept " + oldTransferId);
                assertTrue(writeEntered.await(5, TimeUnit.SECONDS));

                alice.client.execute("/disconnect");
                await(() -> app.transferCoordinator().service().registry().activeCount() == 0);
                await(() -> app.sessions().findByClientId(oldAliceId).isEmpty());
                await(() -> !bob.client.fileTransfer().transferService().isTransferActive());
                await(() -> alice.client.transport().state() == TcpClient.State.DISCONNECTED);
                alice.client.execute("/connect 127.0.0.1 " + app.port() + " " + app.udpPort());
                alice.client.execute("/login alice");
                await(() -> alice.client.authentication().presenceCredentials().isPresent(), () -> alice.text());

                releaseWrite.countDown();
                assertTrue(cleanupFinished.await(5, TimeUnit.SECONDS),
                        "Old blocked worker cleanup did not complete");
                await(() -> !storage.exists(oldTransferId));
                assertEquals(0, app.transferCoordinator().service().registry().activeCount());

                Path second = temp.resolve("generation-b.bin");
                byte[] expected = "fresh connection transfer".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                Files.write(second, expected);
                alice.client.execute("/sendfile " + bobId + " " + second.toString().replace('\\', '/'));
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID newTransferId = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                bob.client.execute("/accept " + newTransferId);
                await(() -> alice.text().contains("[Transfer] Completed [" + newTransferId)
                        && bob.text().contains("[Transfer] Completed [" + newTransferId));
                UUID bobIdentity = bob.client.authentication().presenceCredentials().orElseThrow().session().clientId();
                assertArrayEquals(expected, Files.readAllBytes(temp.resolve("reconnect-bob")
                        .resolve(bobIdentity.toString()).resolve(newTransferId + ".bin")));
            } finally { releaseWrite.countDown(); }
        }
    }

    @Test
    void productionConsentRejectionCancellationDisconnectAndLogoutScenarios() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob"), 0);
        CredentialStore credentials = DemoAccounts.provision(config,
                username -> ("disposable-" + username).toCharArray());
        CountDownLatch blockWrite = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        AtomicBoolean holdNextWrite = new AtomicBoolean(false);
        SafeFileStorage serverStorage = new SafeFileStorage(temp.resolve("consent-server")) {
            @Override public void appendChunk(UUID id, byte[] data, int offset, int length) throws IOException {
                if (holdNextWrite.compareAndSet(true, false)) {
                    blockWrite.countDown();
                    try { releaseWrite.await(5, TimeUnit.SECONDS); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); }
                }
                super.appendChunk(id, data, offset, length);
            }
        };
        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, TransferWorkerPool::new, System::nanoTime, () -> serverStorage)) {
            app.start();
            Path aliceRoot = temp.resolve("consent-alice");
            Path bobRoot = temp.resolve("consent-bob");
            try (var alice = client(app, "alice", aliceRoot);
                 var bob = client(app, "bob", bobRoot)) {
                login(alice, app, "alice");
                login(bob, app, "bob");
                UUID bobId = bob.client.authentication().presenceCredentials().orElseThrow().session().clientId();
                Path source = temp.resolve("consent-sample.bin");
                byte[] content = new byte[ProtocolLimits.MAX_CHUNK_BYTES * 2 + 19];
                Files.write(source, content);

                // 1. No binary transfer before consent & Recipient rejection
                alice.client.execute("/sendfile " + bobId + " \"" + source.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID rejectTid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                assertFalse(serverStorage.exists(rejectTid), "No server partial file may exist before recipient consent");
                assertFalse(Files.exists(bobRoot.resolve(bobId.toString()).resolve(rejectTid + ".part")));

                bob.client.execute("/reject " + rejectTid);
                await(() -> alice.text().contains("[Transfer] Rejected [" + rejectTid)
                        && bob.text().contains("Rejected offer for transfer " + rejectTid)
                        && app.transferCoordinator().service().registry().activeCount() == 0);
                assertFalse(Files.exists(bobRoot.resolve(bobId.toString()).resolve(rejectTid + ".bin")));

                // 2. Sender cancellation during active transfer
                holdNextWrite.set(true);
                alice.client.execute("/sendfile " + bobId + " \"" + source.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent()
                        && alice.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID senderCancelTid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                bob.client.execute("/accept " + senderCancelTid);
                assertTrue(blockWrite.await(5, TimeUnit.SECONDS));
                alice.client.execute("/cancel " + senderCancelTid);
                await(() -> app.transferCoordinator().service().registry().activeCount() == 0);
                releaseWrite.countDown();
                await(() -> alice.text().contains("Cancelled transfer " + senderCancelTid)
                        && bob.text().contains("[Transfer] Cancelled [" + senderCancelTid)
                        && !serverStorage.exists(senderCancelTid));
                assertFalse(Files.exists(bobRoot.resolve(bobId.toString()).resolve(senderCancelTid + ".bin")));

                // 3. Recipient cancellation during active transfer
                CountDownLatch blockWrite2 = new CountDownLatch(1);
                CountDownLatch releaseWrite2 = new CountDownLatch(1);
                SafeFileStorage storageRef = serverStorage;
                alice.client.execute("/sendfile " + bobId + " \"" + source.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent()
                        && alice.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID recipientCancelTid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                bob.client.execute("/cancel " + recipientCancelTid);
                await(() -> bob.text().contains("Cancelled transfer " + recipientCancelTid)
                        && alice.text().contains("[Transfer] Cancelled [" + recipientCancelTid)
                        && app.transferCoordinator().service().registry().activeCount() == 0
                        && !storageRef.exists(recipientCancelTid));

                // 4. Recipient disconnect during active transfer
                alice.client.execute("/sendfile " + bobId + " \"" + source.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent()
                        && alice.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID recDiscTid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                bob.client.execute("/disconnect");
                await(() -> alice.text().contains("[Transfer] Failed [" + recDiscTid)
                        && app.transferCoordinator().service().registry().activeCount() == 0
                        && !storageRef.exists(recDiscTid));

                // Reconnect Bob
                await(() -> bob.client.transport().state() == TcpClient.State.DISCONNECTED);
                login(bob, app, "bob");
                UUID newBobId = bob.client.authentication().presenceCredentials().orElseThrow().session().clientId();

                // 5. Logout during active transfer
                alice.client.execute("/sendfile " + newBobId + " \"" + source.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID logoutTid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                alice.client.execute("/logout");
                await(() -> !alice.client.transport().isConnected()
                        && bob.text().contains("[Transfer] Failed [" + logoutTid)
                        && app.transferCoordinator().service().registry().activeCount() == 0
                        && !storageRef.exists(logoutTid));
                assertFalse(alice.text().contains("[Transfer] Completed"));
                assertFalse(bob.text().contains("[Transfer] Completed"));
            } finally {
                releaseWrite.countDown();
            }
        }
    }

    @Test
    void productionTimeoutCoversStalledActiveTransferDiskWriteProgressExtensionAndInvalidTraffic() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob"), 0);
        CredentialStore credentials = DemoAccounts.provision(config,
                username -> ("disposable-" + username).toCharArray());
        AtomicLong transferClock = new AtomicLong(500_000L);
        CountDownLatch writeEntered = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        AtomicBoolean blockChunk1 = new AtomicBoolean(false);
        SafeFileStorage serverStorage = new SafeFileStorage(temp.resolve("timeout-active-server")) {
            @Override public void appendChunk(UUID id, byte[] data, int offset, int length) throws IOException {
                if (blockChunk1.compareAndSet(true, false)) {
                    writeEntered.countDown();
                    try { releaseWrite.await(8, TimeUnit.SECONDS); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); }
                }
                super.appendChunk(id, data, offset, length);
            }
        };
        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, TransferWorkerPool::new,
                System::nanoTime, () -> serverStorage, transferClock::get)) {
            app.start();
            try (Socket alice = rawSocket(app); Socket bob = rawSocket(app)) {
                var aliceLogin = rawLogin(alice, "alice");
                var bobLogin = rawLogin(bob, "bob");

                // 1. Offer at t = 500_000
                UUID offerMsgId = UUID.randomUUID();
                writeFrame(alice, new MessageEnvelope(1, MessageType.FILE_OFFER, offerMsgId, null,
                        new MessageBody.FileOffer(bobLogin.clientId(), "stalled.bin", ProtocolLimits.MAX_CHUNK_BYTES * 2L)));
                MessageEnvelope offerAck = readUntilReply(alice, offerMsgId);
                UUID tid = ((MessageBody.Ack) offerAck.body()).transferId();
                MessageEnvelope offered = readUntilType(bob, MessageType.FILE_OFFERED);
                assertEquals(tid, ((MessageBody.FileOffered) offered.body()).transferId());

                // 2. Advance clock by 20s (t = 520_000) and accept -> valid progress extends deadline!
                transferClock.addAndGet(20_000L);
                UUID acceptId = UUID.randomUUID();
                writeFrame(bob, new MessageEnvelope(1, MessageType.FILE_ACCEPT, acceptId, null,
                        new MessageBody.TransferReference(tid)));
                readUntilReply(bob, acceptId);
                readUntilType(alice, MessageType.FILE_STATUS);

                // 3. Advance clock by 20s (t = 540_000, 40s since offer, 20s since accept) -> must NOT expire
                transferClock.addAndGet(20_000L);
                Thread.sleep(1200); // allow 1s scheduler tick
                assertEquals(1, app.transferCoordinator().service().registry().activeCount(),
                        "Valid accept progress must extend transfer deadline");

                // 4. Send chunk 0 at t = 540_000 -> extends deadline to 570_000
                UUID chunk0Id = UUID.randomUUID();
                writeFrame(alice, new FileChunk(chunk0Id, tid, 0, new byte[ProtocolLimits.MAX_CHUNK_BYTES]));
                readUntilReply(alice, chunk0Id);
                readRawFrame(bob); // forwarded FileChunk

                // 5. Advance clock by 20s (t = 560_000) and send INVALID traffic (e.g. invalid FILE_ACCEPT on already accepted transfer)
                transferClock.addAndGet(20_000L);
                UUID invalidAcceptId = UUID.randomUUID();
                writeFrame(bob, new MessageEnvelope(1, MessageType.FILE_ACCEPT, invalidAcceptId, null,
                        new MessageBody.TransferReference(tid)));
                MessageEnvelope err = readUntilReply(bob, invalidAcceptId);
                assertEquals(MessageType.ERROR, err.type());

                // 6. Block chunk 1 in disk write and advance clock by 11s (t = 571_000 >= 30s since chunk 0 at 540_000)
                blockChunk1.set(true);
                writeFrame(alice, new FileChunk(UUID.randomUUID(), tid, 1, new byte[ProtocolLimits.MAX_CHUNK_BYTES]));
                assertTrue(writeEntered.await(5, TimeUnit.SECONDS));
                transferClock.addAndGet(11_000L);

                // Wait for timeout scheduler to expire the transfer while disk operation is still outstanding
                await(() -> app.transferCoordinator().service().registry().activeCount() == 0);
                releaseWrite.countDown();
                await(() -> !serverStorage.exists(tid));

                MessageEnvelope aliceTimeout = readUntilType(alice, MessageType.FILE_STATUS);
                assertEquals(FileStatusState.FAILED, ((MessageBody.FileStatus) aliceTimeout.body()).state());
            } finally {
                releaseWrite.countDown();
            }
            app.close();
            assertTrue(app.transferTimeoutsForTest().isShutdown());
        }
    }

    @Test
    void productionAuthorizationAndIntegrityRejectionsDoNotMutateActiveTransfer() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob", "carol"), 0);
        CredentialStore credentials = DemoAccounts.provision(config,
                username -> ("disposable-" + username).toCharArray());
        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, TransferWorkerPool::new,
                System::nanoTime, () -> new SafeFileStorage(temp.resolve("auth-integrity-server")))) {
            app.start();
            try (Socket unauth = rawSocket(app);
                 Socket alice = rawSocket(app);
                 Socket bob = rawSocket(app);
                 Socket carol = rawSocket(app)) {

                // 1. Unauthenticated transfer control message & binary chunk
                UUID unauthOfferId = UUID.randomUUID();
                writeFrame(unauth, new MessageEnvelope(1, MessageType.FILE_OFFER, unauthOfferId, null,
                        new MessageBody.FileOffer(UUID.randomUUID(), "unauth.bin", 100)));
                MessageEnvelope unauthErr1 = readUntilReply(unauth, unauthOfferId);
                assertEquals(ErrorCode.UNAUTHENTICATED, ((MessageBody.Error) unauthErr1.body()).code());

                UUID unauthChunkId = UUID.randomUUID();
                writeFrame(unauth, new FileChunk(unauthChunkId, UUID.randomUUID(), 0, new byte[]{1, 2, 3}));
                MessageEnvelope unauthErr2 = readUntilReply(unauth, unauthChunkId);
                assertEquals(ErrorCode.UNAUTHENTICATED, ((MessageBody.Error) unauthErr2.body()).code());

                var aliceLogin = rawLogin(alice, "alice");
                var bobLogin = rawLogin(bob, "bob");
                var carolLogin = rawLogin(carol, "carol");

                // 2. Invalid transfer ID and unsafe filename rejection (".." passes wire codec and is rejected by FileTransferService with INVALID_REQUEST)
                UUID badNameId = UUID.randomUUID();
                writeFrame(alice, new MessageEnvelope(1, MessageType.FILE_OFFER, badNameId, null,
                        new MessageBody.FileOffer(bobLogin.clientId(), "..escape.bin", 100)));
                assertEquals(ErrorCode.INVALID_REQUEST, ((MessageBody.Error) readUntilReply(alice, badNameId).body()).code());

                UUID unknownTidId = UUID.randomUUID();
                writeFrame(bob, new MessageEnvelope(1, MessageType.FILE_ACCEPT, unknownTidId, null,
                        new MessageBody.TransferReference(UUID.randomUUID())));
                assertEquals(ErrorCode.INVALID_REQUEST, ((MessageBody.Error) readUntilReply(bob, unknownTidId).body()).code());

                // 3. Create valid offer Alice -> Bob
                byte[] validPayload = "verified-production-bytes".getBytes(StandardCharsets.UTF_8);
                UUID offerId = UUID.randomUUID();
                writeFrame(alice, new MessageEnvelope(1, MessageType.FILE_OFFER, offerId, null,
                        new MessageBody.FileOffer(bobLogin.clientId(), "clean.bin", validPayload.length)));
                UUID tid = ((MessageBody.Ack) readUntilReply(alice, offerId).body()).transferId();
                MessageEnvelope offeredToBob = readUntilType(bob, MessageType.FILE_OFFERED);
                // Verify sender identity is authoritative from session
                assertEquals(aliceLogin.clientId(), ((MessageBody.FileOffered) offeredToBob.body()).senderClientId());

                // 4. Third-party (Carol) acceptance and cancellation attempts -> FORBIDDEN, transfer stays intact
                UUID carolAcceptId = UUID.randomUUID();
                writeFrame(carol, new MessageEnvelope(1, MessageType.FILE_ACCEPT, carolAcceptId, null,
                        new MessageBody.TransferReference(tid)));
                assertEquals(ErrorCode.FORBIDDEN, ((MessageBody.Error) readUntilReply(carol, carolAcceptId).body()).code());

                UUID carolCancelId = UUID.randomUUID();
                writeFrame(carol, new MessageEnvelope(1, MessageType.FILE_CANCEL, carolCancelId, null,
                        new MessageBody.TransferReference(tid)));
                assertEquals(ErrorCode.FORBIDDEN, ((MessageBody.Error) readUntilReply(carol, carolCancelId).body()).code());

                // 5. Binary chunk before consent -> INVALID_STATE, transfer stays intact
                UUID earlyChunkId = UUID.randomUUID();
                writeFrame(alice, new FileChunk(earlyChunkId, tid, 0, validPayload));
                assertEquals(ErrorCode.INVALID_STATE, ((MessageBody.Error) readUntilReply(alice, earlyChunkId).body()).code());

                // Bob accepts
                UUID bobAcceptId = UUID.randomUUID();
                writeFrame(bob, new MessageEnvelope(1, MessageType.FILE_ACCEPT, bobAcceptId, null,
                        new MessageBody.TransferReference(tid)));
                readUntilReply(bob, bobAcceptId);
                readUntilType(alice, MessageType.FILE_STATUS);

                // 6. Third-party binary injection & recipient binary upload (spoofing sender) -> FORBIDDEN, transfer stays intact
                UUID carolChunkId = UUID.randomUUID();
                writeFrame(carol, new FileChunk(carolChunkId, tid, 0, validPayload));
                assertEquals(ErrorCode.FORBIDDEN, ((MessageBody.Error) readUntilReply(carol, carolChunkId).body()).code());

                UUID bobChunkId = UUID.randomUUID();
                writeFrame(bob, new FileChunk(bobChunkId, tid, 0, validPayload));
                assertEquals(ErrorCode.FORBIDDEN, ((MessageBody.Error) readUntilReply(bob, bobChunkId).body()).code());

                // Active transfer is STILL valid and can receive Alice's real chunk 0!
                UUID realChunkId = UUID.randomUUID();
                writeFrame(alice, new FileChunk(realChunkId, tid, 0, validPayload));
                MessageEnvelope chunkAck = readUntilReply(alice, realChunkId);
                assertEquals(MessageType.ACK, chunkAck.type());
                assertInstanceOf(FileChunk.class, readRawFrame(bob));

                // 7. Failed recipient integrity confirmation -> aborts transfer, no false completion
                UUID completeId = UUID.randomUUID();
                String realHash = TransferChecksum.computeDigestHex(validPayload);
                writeFrame(alice, new MessageEnvelope(1, MessageType.FILE_COMPLETE, completeId, null,
                        new MessageBody.FileComplete(tid, 1, realHash)));
                readUntilReply(alice, completeId);
                readUntilType(bob, MessageType.FILE_COMPLETE);

                UUID badReceivedId = UUID.randomUUID();
                writeFrame(bob, new MessageEnvelope(1, MessageType.FILE_RECEIVED, badReceivedId, null,
                        new MessageBody.FileReceived(tid, "0".repeat(64))));
                MessageEnvelope abortStatus = readUntilType(bob, MessageType.FILE_STATUS);
                assertEquals(FileStatusState.FAILED, ((MessageBody.FileStatus) abortStatus.body()).state());
                MessageEnvelope senderError = readUntilReply(alice, badReceivedId);
                assertEquals(MessageType.ERROR, senderError.type());
                assertEquals(0, app.transferCoordinator().service().registry().activeCount());
                await(() -> !app.transferStorage().exists(tid));

                // 8. Incorrect sequence & oversized chunk & incorrect checksum on subsequent transfers
                // 8a. Incorrect sequence
                UUID offer2 = UUID.randomUUID();
                writeFrame(alice, new MessageEnvelope(1, MessageType.FILE_OFFER, offer2, null,
                        new MessageBody.FileOffer(bobLogin.clientId(), "seq.bin", 100)));
                UUID tid2 = ((MessageBody.Ack) readUntilReply(alice, offer2).body()).transferId();
                readUntilType(bob, MessageType.FILE_OFFERED);
                UUID acc2 = UUID.randomUUID();
                writeFrame(bob, new MessageEnvelope(1, MessageType.FILE_ACCEPT, acc2, null, new MessageBody.TransferReference(tid2)));
                readUntilReply(bob, acc2);
                readUntilType(alice, MessageType.FILE_STATUS);
                UUID badSeqId = UUID.randomUUID();
                writeFrame(alice, new FileChunk(badSeqId, tid2, 1, new byte[100]));
                assertEquals(ErrorCode.INVALID_SEQUENCE, ((MessageBody.Error) readUntilReply(alice, badSeqId).body()).code());
                readUntilType(bob, MessageType.FILE_STATUS);

                // 8b. Chunk exceeding declared size
                UUID offer3 = UUID.randomUUID();
                writeFrame(alice, new MessageEnvelope(1, MessageType.FILE_OFFER, offer3, null,
                        new MessageBody.FileOffer(bobLogin.clientId(), "size.bin", 10)));
                UUID tid3 = ((MessageBody.Ack) readUntilReply(alice, offer3).body()).transferId();
                readUntilType(bob, MessageType.FILE_OFFERED);
                UUID acc3 = UUID.randomUUID();
                writeFrame(bob, new MessageEnvelope(1, MessageType.FILE_ACCEPT, acc3, null, new MessageBody.TransferReference(tid3)));
                readUntilReply(bob, acc3);
                readUntilType(alice, MessageType.FILE_STATUS);
                UUID overChunkId = UUID.randomUUID();
                writeFrame(alice, new FileChunk(overChunkId, tid3, 0, new byte[20]));
                assertEquals(ErrorCode.SIZE_LIMIT, ((MessageBody.Error) readUntilReply(alice, overChunkId).body()).code());
                readUntilType(bob, MessageType.FILE_STATUS);

                // 8c. Incorrect sender checksum in FILE_COMPLETE
                UUID offer4 = UUID.randomUUID();
                writeFrame(alice, new MessageEnvelope(1, MessageType.FILE_OFFER, offer4, null,
                        new MessageBody.FileOffer(bobLogin.clientId(), "hash.bin", 10)));
                UUID tid4 = ((MessageBody.Ack) readUntilReply(alice, offer4).body()).transferId();
                readUntilType(bob, MessageType.FILE_OFFERED);
                UUID acc4 = UUID.randomUUID();
                writeFrame(bob, new MessageEnvelope(1, MessageType.FILE_ACCEPT, acc4, null, new MessageBody.TransferReference(tid4)));
                readUntilReply(bob, acc4);
                readUntilType(alice, MessageType.FILE_STATUS);
                UUID c4 = UUID.randomUUID();
                writeFrame(alice, new FileChunk(c4, tid4, 0, new byte[10]));
                readUntilReply(alice, c4);
                readRawFrame(bob);
                UUID badCompId = UUID.randomUUID();
                writeFrame(alice, new MessageEnvelope(1, MessageType.FILE_COMPLETE, badCompId, null,
                        new MessageBody.FileComplete(tid4, 1, "f".repeat(64))));
                assertEquals(ErrorCode.TRANSFER_FAILED, ((MessageBody.Error) readUntilReply(alice, badCompId).body()).code());
                readUntilType(bob, MessageType.FILE_STATUS);
            }
        }
    }

    @Test
    void productionZeroByteAndExactChunkBoundaryTransfersSucceedAndPreserveExistingDestination() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob"), 0);
        CredentialStore credentials = DemoAccounts.provision(config,
                username -> ("disposable-" + username).toCharArray());
        Path aliceDownloads = temp.resolve("bounds-alice");
        Path bobDownloads = temp.resolve("bounds-bob");
        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, TransferWorkerPool::new,
                System::nanoTime, () -> new SafeFileStorage(temp.resolve("bounds-server")))) {
            app.start();
            try (var alice = client(app, "alice", aliceDownloads);
                 var bob = client(app, "bob", bobDownloads)) {
                login(alice, app, "alice");
                login(bob, app, "bob");
                UUID bobId = bob.client.authentication().presenceCredentials().orElseThrow().session().clientId();

                // 1. Zero-byte transfer over production sockets
                Path emptySource = temp.resolve("zero.bin");
                Files.write(emptySource, new byte[0]);
                alice.client.execute("/sendfile " + bobId + " \"" + emptySource.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID zeroTid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                bob.client.execute("/accept " + zeroTid);
                await(() -> alice.text().contains("[Transfer] Completed [" + zeroTid)
                        && bob.text().contains("[Transfer] Completed [" + zeroTid));
                Path zeroReceived = bobDownloads.resolve(bobId.toString()).resolve(zeroTid + ".bin");
                assertTrue(Files.exists(zeroReceived));
                assertEquals(0L, Files.size(zeroReceived));

                // 2. Exact chunk-boundary file (16,384 bytes)
                Path exactSource = temp.resolve("exact16k.bin");
                byte[] exactBytes = new byte[ProtocolLimits.MAX_CHUNK_BYTES];
                for (int i = 0; i < exactBytes.length; i++) exactBytes[i] = (byte) (i * 13 + 3);
                Files.write(exactSource, exactBytes);
                alice.client.execute("/sendfile " + bobId + " \"" + exactSource.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID exactTid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                bob.client.execute("/accept " + exactTid);
                await(() -> alice.text().contains("[Transfer] Completed [" + exactTid)
                        && bob.text().contains("[Transfer] Completed [" + exactTid));
                Path exactReceived = bobDownloads.resolve(bobId.toString()).resolve(exactTid + ".bin");
                assertArrayEquals(exactBytes, Files.readAllBytes(exactReceived));
                assertEquals(TransferChecksum.computeDigestHex(exactBytes), TransferChecksum.computeFileDigestHex(exactReceived));

                // 3. Existing destination preservation on finalization collision
                Path collisionSource = temp.resolve("collision.bin");
                Files.write(collisionSource, new byte[]{1, 2, 3, 4, 5});
                alice.client.execute("/sendfile " + bobId + " \"" + collisionSource.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID colTid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                Path preExisting = bobDownloads.resolve(bobId.toString()).resolve(colTid + ".bin");
                Files.createDirectories(preExisting.getParent());
                byte[] originalSentinel = "preserve-existing-destination".getBytes(StandardCharsets.UTF_8);
                Files.write(preExisting, originalSentinel);

                bob.client.execute("/accept " + colTid);
                await(() -> !bob.client.fileTransfer().transferService().isTransferActive()
                        && !alice.client.fileTransfer().transferService().isTransferActive()
                        && !Files.exists(bobDownloads.resolve(bobId.toString()).resolve(colTid + ".part")));
                assertArrayEquals(originalSentinel, Files.readAllBytes(preExisting),
                        "Existing destination file must never be overwritten on collision");
                assertFalse(Files.exists(bobDownloads.resolve(bobId.toString()).resolve(colTid + ".part")),
                        "Failed finalization must clean up partial file");
            }
        }
    }

    private static Socket rawSocket(NetSyncServerApplication app) throws Exception {
        Socket s = new Socket("127.0.0.1", app.port());
        s.setSoTimeout(4000);
        s.setTcpNoDelay(true);
        return s;
    }

    private static void writeFrame(Socket socket, OutboundFrame frame) throws Exception {
        ByteBuffer buf = CODEC.encode(frame);
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        socket.getOutputStream().write(bytes);
        socket.getOutputStream().flush();
    }

    private static OutboundFrame readRawFrame(Socket socket) throws Exception {
        DataInputStream in = new DataInputStream(socket.getInputStream());
        int len = CODEC.decodeLength(ByteBuffer.wrap(in.readNBytes(4)));
        byte[] payload = in.readNBytes(len);
        assertEquals(len, payload.length);
        return CODEC.decodePayload(ByteBuffer.wrap(payload));
    }

    private static MessageEnvelope readUntilReply(Socket socket, UUID replyTo) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            OutboundFrame frame = readRawFrame(socket);
            if (frame instanceof MessageEnvelope env && replyTo.equals(env.replyTo())) {
                return env;
            }
        }
        throw new AssertionError("Timed out waiting for replyTo " + replyTo);
    }

    private static MessageEnvelope readUntilType(Socket socket, MessageType type) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            OutboundFrame frame = readRawFrame(socket);
            if (frame instanceof MessageEnvelope env && env.type() == type) {
                return env;
            }
        }
        throw new AssertionError("Timed out waiting for MessageType " + type);
    }

    private static MessageBody.LoginResponse rawLogin(Socket socket, String user) throws Exception {
        UUID id = UUID.randomUUID();
        writeFrame(socket, new MessageEnvelope(1, MessageType.LOGIN_REQUEST, id, null,
                new MessageBody.LoginRequest(user, "disposable-" + user)));
        MessageEnvelope resp = readUntilReply(socket, id);
        assertEquals(MessageType.LOGIN_RESPONSE, resp.type());
        return (MessageBody.LoginResponse) resp.body();
    }

    @Test
    void productionBackpressureConcurrencyAndTemporarySelectorTaskRejectionRecovery() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob", "carol", "dave"), 0);
        CredentialStore credentials = DemoAccounts.provision(config,
                username -> ("disposable-" + username).toCharArray());
        AtomicLong transferClock = new AtomicLong(800_000L);
        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, TransferWorkerPool::new,
                System::nanoTime, () -> new SafeFileStorage(temp.resolve("bp-server")), transferClock::get)) {
            app.start();
            try (Socket alice = rawSocket(app);
                 Socket bob = rawSocket(app);
                 Socket carol = rawSocket(app);
                 Socket dave = rawSocket(app)) {
                var aLogin = rawLogin(alice, "alice");
                var bLogin = rawLogin(bob, "bob");
                var cLogin = rawLogin(carol, "carol");
                var dLogin = rawLogin(dave, "dave");

                // 1. Per-client concurrency limit (sender busy and recipient busy)
                UUID offer1 = UUID.randomUUID();
                writeFrame(alice, new MessageEnvelope(1, MessageType.FILE_OFFER, offer1, null,
                        new MessageBody.FileOffer(bLogin.clientId(), "first.bin", 100)));
                UUID tid1 = ((MessageBody.Ack) readUntilReply(alice, offer1).body()).transferId();
                readUntilType(bob, MessageType.FILE_OFFERED);

                // Alice tries second offer while active -> BUSY
                UUID offer2 = UUID.randomUUID();
                writeFrame(alice, new MessageEnvelope(1, MessageType.FILE_OFFER, offer2, null,
                        new MessageBody.FileOffer(cLogin.clientId(), "second.bin", 100)));
                assertEquals(ErrorCode.BUSY, ((MessageBody.Error) readUntilReply(alice, offer2).body()).code());

                // Carol tries offering to Bob while Bob has an active offer -> BUSY
                UUID offer3 = UUID.randomUUID();
                writeFrame(carol, new MessageEnvelope(1, MessageType.FILE_OFFER, offer3, null,
                        new MessageBody.FileOffer(bLogin.clientId(), "third.bin", 100)));
                assertEquals(ErrorCode.BUSY, ((MessageBody.Error) readUntilReply(carol, offer3).body()).code());

                // 2. Server worker saturation -> BUSY on chunk upload while unrelated client (Carol -> Dave chat/presence) stays responsive
                UUID accept1 = UUID.randomUUID();
                writeFrame(bob, new MessageEnvelope(1, MessageType.FILE_ACCEPT, accept1, null,
                        new MessageBody.TransferReference(tid1)));
                readUntilReply(bob, accept1);
                readUntilType(alice, MessageType.FILE_STATUS);

                CountDownLatch workersEntered = new CountDownLatch(2);
                CountDownLatch releaseWorkers = new CountDownLatch(1);
                try {
                    for (int i = 0; i < 2; i++) {
                        app.workers().submit(() -> {
                            workersEntered.countDown();
                            try { releaseWorkers.await(5, TimeUnit.SECONDS); }
                            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                        });
                    }
                    assertTrue(workersEntered.await(3, TimeUnit.SECONDS));
                    for (int i = 0; i < ProtocolLimits.WORKER_QUEUE_SIZE; i++) {
                        app.workers().submit(() -> {});
                    }
                    assertEquals(0, app.workers().queueRemainingCapacity());

                    // Chunk upload while server worker pool is saturated -> ERROR(BUSY)
                    UUID satChunkId = UUID.randomUUID();
                    writeFrame(alice, new FileChunk(satChunkId, tid1, 0, new byte[100]));
                    MessageEnvelope busyErr = readUntilReply(alice, satChunkId);
                    assertEquals(ErrorCode.BUSY, ((MessageBody.Error) busyErr.body()).code());

                    // Unrelated clients (Carol & Dave) remain responsive for chat and presence during worker saturation!
                    UUID chatId = UUID.randomUUID();
                    writeFrame(carol, new MessageEnvelope(1, MessageType.CHAT_SEND, chatId, null,
                            new MessageBody.ChatSend(dLogin.clientId(), "responsive-under-worker-saturation")));
                    MessageEnvelope chatAck = readUntilReply(carol, chatId);
                    assertEquals(MessageType.ACK, chatAck.type());
                    MessageEnvelope delivered = readUntilType(dave, MessageType.CHAT_DELIVER);
                    assertEquals("responsive-under-worker-saturation", ((MessageBody.ChatDeliver) delivered.body()).text());

                    UUID presId = UUID.randomUUID();
                    writeFrame(carol, new MessageEnvelope(1, MessageType.PRESENCE_QUERY, presId, null, new MessageBody.Empty()));
                    assertEquals(MessageType.PRESENCE_SNAPSHOT, readUntilReply(carol, presId).type());
                } finally {
                    releaseWorkers.countDown();
                }
                await(() -> app.workers().queueSize() == 0 && app.workers().activeThreadCount() == 0);

                // 3. Temporary Selector task rejection does not permanently disable timeout processing
                // Hold selector thread briefly while filling selectorTasks queue so one submit returns false
                CountDownLatch selectorHeld = new CountDownLatch(1);
                CountDownLatch releaseSelector = new CountDownLatch(1);
                assertTrue(app.serverForTest().selectorTasks().submit(() -> {
                    selectorHeld.countDown();
                    try { releaseSelector.await(3, TimeUnit.SECONDS); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }));
                assertTrue(selectorHeld.await(2, TimeUnit.SECONDS));
                try {
                    while (app.serverForTest().selectorTasks().submit(() -> {})) {
                        // fill remaining queue capacity
                    }
                    // A timeout submit while queue is full returns false without killing the scheduler
                    assertFalse(app.serverForTest().selectorTasks().submit(() -> app.transferCoordinator().service().checkTimeouts()));
                } finally {
                    releaseSelector.countDown();
                }

                // Advance transfer clock past timeout; subsequent scheduled ticks succeed and expire tid1
                transferClock.addAndGet(TimeUnit.SECONDS.toMillis(ProtocolLimits.TRANSFER_TIMEOUT_SECONDS) + 1000L);
                await(() -> app.transferCoordinator().service().registry().activeCount() == 0);
            }
        }
    }

    @RepeatedTest(20)
    void productionStartupRollbackRestartOnSamePortsAndShutdownWithActiveTransferWork() throws Exception {
        DemoServerConfig ephemeralConfig = new DemoServerConfig(true, 0, List.of("alice", "bob"), 0);
        CredentialStore credentials = DemoAccounts.provision(ephemeralConfig,
                username -> ("disposable-" + username).toCharArray());

        // 1. Invalid storage root / transfer initialization failure rolls back workers and sockets
        Path fileAsRoot = temp.resolve("not-a-directory.bin");
        Files.write(fileAsRoot, new byte[]{1});
        java.util.concurrent.atomic.AtomicReference<TransferWorkerPool> rolledBackPool = new java.util.concurrent.atomic.AtomicReference<>();
        try (NetSyncServerApplication failedApp = new NetSyncServerApplication(ephemeralConfig, credentials,
                com.netsync.server.NioServer::new,
                () -> { var pool = new TransferWorkerPool(); rolledBackPool.set(pool); return pool; },
                System::nanoTime, () -> new SafeFileStorage(fileAsRoot))) {
            assertThrows(RuntimeException.class, failedApp::start);
            assertFalse(failedApp.isRunning());
            assertNotNull(rolledBackPool.get());
            assertTrue(rolledBackPool.get().isTerminated(), "Worker pool must be rolled back on storage init failure");
            // Idempotent close
            failedApp.close();
            failedApp.close();
        }

        // 2. Start instance 1 with active offer, active transfer in disk write, and pending cleanup, then shut down and restart on SAME TCP & UDP ports
        CountDownLatch writeEntered = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        AtomicBoolean blockFirst = new AtomicBoolean(true);
        Path serverDir = temp.resolve("restart-server");
        SafeFileStorage storage1 = new SafeFileStorage(serverDir) {
            @Override public void appendChunk(UUID id, byte[] data, int offset, int length) throws IOException {
                if (blockFirst.compareAndSet(true, false)) {
                    writeEntered.countDown();
                    try { releaseWrite.await(2, TimeUnit.SECONDS); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }
                super.appendChunk(id, data, offset, length);
            }
        };

        int tcpPort;
        int udpPort;
        UUID activeTid;
        NetSyncServerApplication app1 = new NetSyncServerApplication(ephemeralConfig, credentials,
                com.netsync.server.NioServer::new, TransferWorkerPool::new, System::nanoTime, () -> storage1);
        try {
            app1.start();
            tcpPort = app1.port();
            udpPort = app1.udpPort();
            assertTrue(tcpPort > 0 && udpPort > 0);

            try (Socket alice = rawSocket(app1); Socket bob = rawSocket(app1)) {
                rawLogin(alice, "alice");
                var bobLogin = rawLogin(bob, "bob");
                UUID offerId = UUID.randomUUID();
                writeFrame(alice, new MessageEnvelope(1, MessageType.FILE_OFFER, offerId, null,
                        new MessageBody.FileOffer(bobLogin.clientId(), "shutdown-active.bin", 100)));
                activeTid = ((MessageBody.Ack) readUntilReply(alice, offerId).body()).transferId();
                readUntilType(bob, MessageType.FILE_OFFERED);
                UUID accId = UUID.randomUUID();
                writeFrame(bob, new MessageEnvelope(1, MessageType.FILE_ACCEPT, accId, null,
                        new MessageBody.TransferReference(activeTid)));
                readUntilReply(bob, accId);
                readUntilType(alice, MessageType.FILE_STATUS);

                writeFrame(alice, new FileChunk(UUID.randomUUID(), activeTid, 0, new byte[100]));
                assertTrue(writeEntered.await(5, TimeUnit.SECONDS));

                // Release write concurrently with server close
                CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(app1::close);
                Thread.sleep(20);
                releaseWrite.countDown();
                closeFuture.get(8, TimeUnit.SECONDS);
            }
        } finally {
            releaseWrite.countDown();
            app1.close();
        }

        // Verify instance 1 resources are terminated and partial file cleaned up
        assertFalse(app1.isRunning());
        assertTrue(app1.workers().isTerminated());
        assertTrue(app1.transferTimeoutsForTest().isShutdown());
        await(() -> !storage1.exists(activeTid));

        // 3. Restart a new NetSyncServerApplication reusing the exact same TCP and UDP port pair
        DemoServerConfig reuseConfig = new DemoServerConfig(true, tcpPort, List.of("alice", "bob"), udpPort);
        SafeFileStorage storage2 = new SafeFileStorage(serverDir);
        try (NetSyncServerApplication app2 = new NetSyncServerApplication(reuseConfig, credentials,
                com.netsync.server.NioServer::new, TransferWorkerPool::new, System::nanoTime, () -> storage2)) {
            app2.start();
            assertEquals(tcpPort, app2.port());
            assertEquals(udpPort, app2.udpPort());
            assertTrue(app2.isRunning());

            try (var alice = client(app2, "alice", temp.resolve("restart-alice"));
                 var bob = client(app2, "bob", temp.resolve("restart-bob"))) {
                login(alice, app2, "alice");
                login(bob, app2, "bob");
                UUID bobId = bob.client.authentication().presenceCredentials().orElseThrow().session().clientId();
                Path file = temp.resolve("after-restart.bin");
                byte[] expected = "restarted-server-transfer-ok".getBytes(StandardCharsets.UTF_8);
                Files.write(file, expected);

                alice.client.execute("/sendfile " + bobId + " \"" + file.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID newTid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                bob.client.execute("/accept " + newTid);
                await(() -> alice.text().contains("[Transfer] Completed [" + newTid)
                        && bob.text().contains("[Transfer] Completed [" + newTid));
                assertArrayEquals(expected, Files.readAllBytes(
                        temp.resolve("restart-bob").resolve(bobId.toString()).resolve(newTid + ".bin")));
            }
        }
    }

    @Test
    void productionTransferRemainsAuthorizedAfterUdpPresenceExpiryAndDoesNotInterfereWithChatOrPresence() throws Exception {
        AtomicLong presenceClock = new AtomicLong(0L);
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob"), 0);
        CredentialStore credentials = DemoAccounts.provision(config,
                username -> ("disposable-" + username).toCharArray());
        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, TransferWorkerPool::new,
                presenceClock::get, () -> new SafeFileStorage(temp.resolve("pres-expiry-server")))) {
            app.start();
            Path aliceRoot = temp.resolve("pres-alice");
            Path bobRoot = temp.resolve("pres-bob");
            try (var alice = client(app, "alice", aliceRoot);
                 var bob = client(app, "bob", bobRoot)) {
                login(alice, app, "alice");
                login(bob, app, "bob");
                UUID aliceId = alice.client.authentication().presenceCredentials().orElseThrow().session().clientId();
                UUID bobId = bob.client.authentication().presenceCredentials().orElseThrow().session().clientId();

                // Expire UDP presence for both Alice and Bob without logging out TCP
                alice.client.presence().close();
                bob.client.presence().close();
                presenceClock.addAndGet(TimeUnit.SECONDS.toNanos(ProtocolLimits.PRESENCE_TIMEOUT_SECONDS + 2));
                app.presenceTracker().checkTimeouts();
                assertEquals(PresenceState.OFFLINE, app.presenceTracker().getPresenceState(aliceId).orElseThrow());
                assertEquals(PresenceState.OFFLINE, app.presenceTracker().getPresenceState(bobId).orElseThrow());

                // File transfer must still succeed over authenticated TCP!
                Path source = temp.resolve("after-presence-expiry.bin");
                byte[] payload = "tcp-auth-survives-udp-expiry".getBytes(StandardCharsets.UTF_8);
                Files.write(source, payload);
                alice.client.execute("/sendfile " + bobId + " \"" + source.toString().replace('\\', '/') + "\"");
                await(() -> bob.client.fileTransfer().transferService().activeTransferId().isPresent());
                UUID tid = bob.client.fileTransfer().transferService().activeTransferId().orElseThrow();
                bob.client.execute("/accept " + tid);
                await(() -> alice.text().contains("[Transfer] Completed [" + tid)
                        && bob.text().contains("[Transfer] Completed [" + tid));
                assertArrayEquals(payload, Files.readAllBytes(bobRoot.resolve(bobId.toString()).resolve(tid + ".bin")));

                // Verify no generic "Received ACK" or "Received FILE_" leakage in client output
                assertFalse(alice.text().contains("Received ACK"));
                assertFalse(bob.text().contains("Received ACK"));
                assertFalse(alice.text().contains("Received FILE_"));
                assertFalse(bob.text().contains("Received FILE_"));
            }
        }
    }

    private static Client client(NetSyncServerApplication app, String username, Path downloads) {
        StringWriter writer = new StringWriter();
        NetSyncClient client = new NetSyncClient(new TcpClient(), new PrintWriter(writer, true),
                () -> ("disposable-" + username).toCharArray(), app.udpPort(), downloads);
        return new Client(client, writer);
    }

    private static void login(Client client, NetSyncServerApplication app, String username) {
        client.client.execute("/connect 127.0.0.1 " + app.port() + " " + app.udpPort());
        client.client.execute("/login " + username);
        await(() -> client.client.authentication().presenceCredentials().isPresent());
    }

    private static void await(java.util.function.BooleanSupplier condition) {
        await(condition, () -> "");
    }
    private static void await(java.util.function.BooleanSupplier condition, java.util.function.Supplier<String> detail) {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (!condition.getAsBoolean() && System.nanoTime() < until) {
            try { Thread.sleep(5); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); fail("Interrupted"); }
        }
        assertTrue(condition.getAsBoolean(), "Timed out waiting for production transfer state " + detail.get());
    }

    private record Client(NetSyncClient client, StringWriter writer) implements AutoCloseable {
        String text() { synchronized (writer) { return writer.toString(); } }
        @Override public void close() { client.close(); }
    }
}
