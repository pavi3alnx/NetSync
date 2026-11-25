package com.netsync.transfer;

import com.netsync.common.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Transfer Service Integration Test.
 * <p>
 * Demonstrates two concurrent file transfers running concurrently while chat messages
 * continue to be routed and delivered through an independent chat handler fixture.
 * <p>
 * Note on scope: This tests concurrent integration of {@link FileTransferCoordinator},
 * {@link FileTransferService}, {@link TransferWorkerPool}, and {@link SafeFileStorage}
 * against common networking contracts. Full end-to-end integration against the real NIO
 * server and authentication module remains pending Member 1 and Member 4 implementations.
 */
class FileTransferIntegrationTest {
    private void awaitStorageDeleted(SafeFileStorage storage, UUID transferId) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (storage.exists(transferId) && System.nanoTime() < deadline) {
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        assertFalse(storage.exists(transferId));
    }

    @TempDir
    Path tempDir;

    private TransferRegistry registry;
    private SafeFileStorage storage;
    private TransferWorkerPool workerPool;
    private TransferTestFixtures.TestOutboundSink outboundSink;
    private TransferTestFixtures.TestDirectSelectorTasks selectorTasks;
    private TransferTestFixtures.TestSessionRegistry sessionRegistry;
    private TransferTestFixtures.TestAuthorizationService authService;
    private TransferTestFixtures.TestChatHandler chatHandler;
    private AtomicLong clockTime;

    private FileTransferCoordinator coordinator;

    // Participants for Transfer 1: Alice -> Bob
    private TransferTestFixtures.TestConnectionContext aliceCtx;
    private TransferTestFixtures.TestConnectionContext bobCtx;
    private SessionView alice;
    private SessionView bob;

    // Participants for Transfer 2: Charlie -> Dave
    private TransferTestFixtures.TestConnectionContext charlieCtx;
    private TransferTestFixtures.TestConnectionContext daveCtx;
    private SessionView charlie;
    private SessionView dave;

    @BeforeEach
    void setUp() {
        registry = new TransferRegistry();
        storage = new SafeFileStorage(tempDir.resolve("server"));
        workerPool = new TransferWorkerPool(2, 32);
        outboundSink = new TransferTestFixtures.TestOutboundSink();
        selectorTasks = new TransferTestFixtures.TestDirectSelectorTasks();
        sessionRegistry = new TransferTestFixtures.TestSessionRegistry();
        authService = new TransferTestFixtures.TestAuthorizationService();
        clockTime = new AtomicLong(500_000L);

        FileTransferService service = new FileTransferService(
                registry, storage, workerPool, outboundSink, selectorTasks, sessionRegistry, authService, clockTime::get
        );
        coordinator = new FileTransferCoordinator(service);

        aliceCtx = TransferTestFixtures.TestConnectionContext.create("alice");
        bobCtx = TransferTestFixtures.TestConnectionContext.create("bob");
        charlieCtx = TransferTestFixtures.TestConnectionContext.create("charlie");
        daveCtx = TransferTestFixtures.TestConnectionContext.create("dave");

        alice = aliceCtx.session().orElseThrow();
        bob = bobCtx.session().orElseThrow();
        charlie = charlieCtx.session().orElseThrow();
        dave = daveCtx.session().orElseThrow();

        sessionRegistry.register(alice);
        sessionRegistry.register(bob);
        sessionRegistry.register(charlie);
        sessionRegistry.register(dave);

        chatHandler = new TransferTestFixtures.TestChatHandler(outboundSink, sessionRegistry);
    }

    @AfterEach
    void tearDown() {
        workerPool.close();
    }

    @Test
    void twoConcurrentTransfersProgressWhileChatMessagesAreProcessed() throws Exception {
        int chunkSize = ProtocolLimits.MAX_CHUNK_BYTES;
        int t1Size = chunkSize + 2048; // 2 chunks
        int t2Size = chunkSize + 4096; // 2 chunks

        byte[] t1Chunk0 = new byte[chunkSize];
        byte[] t1Chunk1 = new byte[2048];
        for (int i = 0; i < t1Chunk0.length; i++) t1Chunk0[i] = (byte) ('A' + (i % 26));
        for (int i = 0; i < t1Chunk1.length; i++) t1Chunk1[i] = (byte) ('0' + (i % 10));

        byte[] t1Combined = new byte[t1Size];
        System.arraycopy(t1Chunk0, 0, t1Combined, 0, chunkSize);
        System.arraycopy(t1Chunk1, 0, t1Combined, chunkSize, 2048);
        String t1Sha256 = TransferChecksum.computeDigestHex(t1Combined);

        byte[] t2Chunk0 = new byte[chunkSize];
        byte[] t2Chunk1 = new byte[4096];
        for (int i = 0; i < t2Chunk0.length; i++) t2Chunk0[i] = (byte) ('X' + (i % 3));
        for (int i = 0; i < t2Chunk1.length; i++) t2Chunk1[i] = (byte) ('a' + (i % 26));

        byte[] t2Combined = new byte[t2Size];
        System.arraycopy(t2Chunk0, 0, t2Combined, 0, chunkSize);
        System.arraycopy(t2Chunk1, 0, t2Combined, chunkSize, 4096);
        String t2Sha256 = TransferChecksum.computeDigestHex(t2Combined);

        // Step 1: Start both offers concurrently
        UUID t1 = startTransfer(aliceCtx, bobCtx, "alice-file.bin", t1Size);
        UUID t2 = startTransfer(charlieCtx, daveCtx, "charlie-file.bin", t2Size);

        assertEquals(2, registry.activeCount());
        outboundSink.clear();

        // Step 2: Interleave chat message 1 (Alice -> Bob: "Hey Bob, sending you a file!")
        sendChat(aliceCtx, bob.clientId(), "Hey Bob, sending you a file!");
        assertNotNull(outboundSink.poll(bobCtx.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(aliceCtx.connectionId(), 5, TimeUnit.SECONDS));

        // Step 3: Stream first chunks for both transfers
        coordinator.handleChunk(aliceCtx, new FileChunk(UUID.randomUUID(), t1, 0, t1Chunk0));
        assertNotNull(outboundSink.poll(aliceCtx.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(bobCtx.connectionId(), 5, TimeUnit.SECONDS));

        coordinator.handleChunk(charlieCtx, new FileChunk(UUID.randomUUID(), t2, 0, t2Chunk0));
        assertNotNull(outboundSink.poll(charlieCtx.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(daveCtx.connectionId(), 5, TimeUnit.SECONDS));

        // Step 4: Interleave chat message 2 (Bob -> Alice: "Got the offer, downloading now")
        sendChat(bobCtx, alice.clientId(), "Got the offer, downloading now");
        assertNotNull(outboundSink.poll(aliceCtx.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(bobCtx.connectionId(), 5, TimeUnit.SECONDS));

        // Step 5: Stream final chunks for both transfers
        coordinator.handleChunk(aliceCtx, new FileChunk(UUID.randomUUID(), t1, 1, t1Chunk1));
        assertNotNull(outboundSink.poll(aliceCtx.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(bobCtx.connectionId(), 5, TimeUnit.SECONDS));

        coordinator.handleChunk(charlieCtx, new FileChunk(UUID.randomUUID(), t2, 1, t2Chunk1));
        assertNotNull(outboundSink.poll(charlieCtx.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(daveCtx.connectionId(), 5, TimeUnit.SECONDS));

        // Step 6: Interleave chat message 3 (Charlie -> Dave: "Hope your file finishes soon")
        sendChat(charlieCtx, dave.clientId(), "Hope your file finishes soon");
        assertNotNull(outboundSink.poll(daveCtx.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(charlieCtx.connectionId(), 5, TimeUnit.SECONDS));

        // Step 7: Complete Transfer 1
        completeTransfer(aliceCtx, bobCtx, t1, 2, t1Sha256);
        assertEquals(1, registry.activeCount());
        awaitStorageDeleted(storage, t1);

        // Step 8: Interleave chat message 4 (Dave -> Charlie: "Received all chunks!")
        sendChat(daveCtx, charlie.clientId(), "Received all chunks!");
        assertNotNull(outboundSink.poll(charlieCtx.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(daveCtx.connectionId(), 5, TimeUnit.SECONDS));

        // Step 9: Complete Transfer 2
        completeTransfer(charlieCtx, daveCtx, t2, 2, t2Sha256);
        assertEquals(0, registry.activeCount());
        awaitStorageDeleted(storage, t2);

        // Step 10: Verify all 4 chat messages were delivered without interference
        List<MessageEnvelope> deliveredChats = chatHandler.deliveredChats();
        assertEquals(4, deliveredChats.size());
        assertEquals("Hey Bob, sending you a file!", ((MessageBody.ChatDeliver) deliveredChats.get(0).body()).text());
        assertEquals("Got the offer, downloading now", ((MessageBody.ChatDeliver) deliveredChats.get(1).body()).text());
        assertEquals("Hope your file finishes soon", ((MessageBody.ChatDeliver) deliveredChats.get(2).body()).text());
        assertEquals("Received all chunks!", ((MessageBody.ChatDeliver) deliveredChats.get(3).body()).text());
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private UUID startTransfer(TransferTestFixtures.TestConnectionContext sCtx, TransferTestFixtures.TestConnectionContext rCtx, String fileName, long size) {
        SessionView sSession = sCtx.session().orElseThrow();
        SessionView rSession = rCtx.session().orElseThrow();

        UUID offerMsgId = UUID.randomUUID();
        var offerBody = new MessageBody.FileOffer(rSession.clientId(), fileName, size);
        coordinator.handleControl(sCtx, new MessageEnvelope(1, MessageType.FILE_OFFER, offerMsgId, null, offerBody));

        MessageEnvelope ack = (MessageEnvelope) outboundSink.getAllFrames(sCtx.connectionId())
                .stream().filter(f -> f instanceof MessageEnvelope e && e.type() == MessageType.ACK).reduce((a, b) -> b).orElseThrow();
        UUID transferId = ((MessageBody.Ack) ack.body()).transferId();

        var acceptBody = new MessageBody.TransferReference(transferId);
        coordinator.handleControl(rCtx, new MessageEnvelope(1, MessageType.FILE_ACCEPT, UUID.randomUUID(), null, acceptBody));
        return transferId;
    }

    private void completeTransfer(
            TransferTestFixtures.TestConnectionContext sCtx,
            TransferTestFixtures.TestConnectionContext rCtx,
            UUID transferId,
            int chunkCount,
            String sha256
    ) throws Exception {
        // Sender FILE_COMPLETE
        coordinator.handleControl(sCtx, new MessageEnvelope(
                1, MessageType.FILE_COMPLETE, UUID.randomUUID(), null,
                new MessageBody.FileComplete(transferId, chunkCount, sha256)
        ));

        // Wait for FILE_COMPLETE completion: ACK to sender, complete event to recipient
        assertNotNull(outboundSink.poll(sCtx.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(rCtx.connectionId(), 5, TimeUnit.SECONDS));

        // Recipient FILE_RECEIVED
        coordinator.handleControl(rCtx, new MessageEnvelope(
                1, MessageType.FILE_RECEIVED, UUID.randomUUID(), null,
                new MessageBody.FileReceived(transferId, sha256)
        ));

        // Wait for final ACK to recipient and FILE_STATUS(COMPLETED) to sender
        assertNotNull(outboundSink.poll(rCtx.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(sCtx.connectionId(), 5, TimeUnit.SECONDS));
    }

    private void sendChat(TransferTestFixtures.TestConnectionContext fromCtx, UUID recipientClientId, String text) {
        MessageEnvelope chatMsg = new MessageEnvelope(
                1, MessageType.CHAT_SEND, UUID.randomUUID(), null,
                new MessageBody.ChatSend(recipientClientId, text)
        );
        chatHandler.handle(fromCtx, chatMsg);
    }
}
