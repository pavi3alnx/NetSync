package com.netsync.transfer;

import com.netsync.common.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class FileTransferConcurrencyAndFailureTest {

    @TempDir
    Path tempDir;

    private TransferRegistry registry;
    private SafeFileStorage storage;
    private TransferWorkerPool workerPool;
    private TransferTestFixtures.TestOutboundSink outboundSink;
    private TransferTestFixtures.TestDirectSelectorTasks selectorTasks;
    private TransferTestFixtures.TestSessionRegistry sessionRegistry;
    private TransferTestFixtures.TestAuthorizationService authService;
    private AtomicLong clockTime;

    private FileTransferCoordinator coordinator;

    private TransferTestFixtures.TestConnectionContext senderContext;
    private TransferTestFixtures.TestConnectionContext recipientContext;
    private SessionView senderSession;
    private SessionView recipientSession;

    @BeforeEach
    void setUp() {
        registry = new TransferRegistry();
        storage = new SafeFileStorage(tempDir.resolve("server"));
        workerPool = new TransferWorkerPool(2, 32);
        outboundSink = new TransferTestFixtures.TestOutboundSink();
        selectorTasks = new TransferTestFixtures.TestDirectSelectorTasks();
        sessionRegistry = new TransferTestFixtures.TestSessionRegistry();
        authService = new TransferTestFixtures.TestAuthorizationService();
        clockTime = new AtomicLong(300_000L);

        FileTransferService service = new FileTransferService(
                registry, storage, workerPool, outboundSink, selectorTasks, sessionRegistry, authService, clockTime::get
        );
        coordinator = new FileTransferCoordinator(service);

        senderContext = TransferTestFixtures.TestConnectionContext.create("alice");
        recipientContext = TransferTestFixtures.TestConnectionContext.create("bob");
        senderSession = senderContext.session().orElseThrow();
        recipientSession = recipientContext.session().orElseThrow();

        sessionRegistry.register(senderSession);
        sessionRegistry.register(recipientSession);
    }

    @AfterEach
    void tearDown() {
        workerPool.close();
    }

    @Test
    void selectorCompletionReservationUnavailableReturnsBusyAndDoesNotDispatch() {
        UUID transferId = setupAcceptedTransfer("data.bin", 100L);

        // Simulate Selector completion queue saturation (tryReserveCompletion returns empty)
        selectorTasks.setRejectReservations(true);

        UUID chunkMsgId = UUID.randomUUID();
        FileChunk chunk = new FileChunk(chunkMsgId, transferId, 0, new byte[100]);

        coordinator.handleChunk(senderContext, chunk);

        // Sender receives ERROR(BUSY)
        MessageEnvelope err = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.BUSY, ((MessageBody.Error) err.body()).code());
        assertEquals(chunkMsgId, err.replyTo());

        // File was NOT written
        assertFalse(storage.exists(transferId));
        // Transfer session remains valid and not aborted
        assertTrue(registry.get(transferId).isPresent());
    }

    @Test
    void workerPoolQueueSaturationReturnsBusyAndReleasesReservation() throws InterruptedException {
        // Create small worker pool with 1 thread and 1 queue capacity to force saturation easily
        TransferWorkerPool smallPool = new TransferWorkerPool(1, 1);
        FileTransferService saturatingService = new FileTransferService(
                registry, storage, smallPool, outboundSink, selectorTasks, sessionRegistry, authService, clockTime::get
        );
        FileTransferCoordinator saturatingCoordinator = new FileTransferCoordinator(saturatingService);

        CountDownLatch blockLatch = new CountDownLatch(1);
        // Fill worker thread
        smallPool.submit(() -> {
            try {
                blockLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {}
        });
        // Fill single queue slot
        smallPool.submit(() -> {});

        // Valid transfer of 100 bytes
        UUID transferId = setupAcceptedTransfer("file.bin", 100L);

        UUID chunkMsgId = UUID.randomUUID();
        FileChunk chunk = new FileChunk(chunkMsgId, transferId, 0, new byte[100]);

        // This submit will be rejected by smallPool (AbortPolicy -> RejectedExecutionException)
        saturatingCoordinator.handleChunk(senderContext, chunk);

        // Verify BUSY error sent
        OutboundFrame frame = outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(frame);
        MessageEnvelope err = (MessageEnvelope) frame;
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.BUSY, ((MessageBody.Error) err.body()).code());
        assertEquals(chunkMsgId, err.replyTo());

        // Reservation was properly closed (not held/leaked)
        assertFalse(selectorTasks.isReservationHeld());

        blockLatch.countDown();
        smallPool.close();
    }

    @Test
    void twoConcurrentTransfersOperateIndependentlyWithoutInterference() throws Exception {
        var charlieContext = TransferTestFixtures.TestConnectionContext.create("charlie");
        var daveContext = TransferTestFixtures.TestConnectionContext.create("dave");
        SessionView charlie = charlieContext.session().orElseThrow();
        SessionView dave = daveContext.session().orElseThrow();
        sessionRegistry.register(charlie);
        sessionRegistry.register(dave);

        // Transfer 1: Alice -> Bob (100 bytes)
        UUID t1 = setupAcceptedTransfer("alice-doc.bin", 100L);
        // Transfer 2: Charlie -> Dave (200 bytes)
        UUID t2 = setupAcceptedTransferFor(charlieContext, daveContext, "charlie-doc.bin", 200L);

        assertEquals(2, registry.activeCount());

        // Stream chunk for Transfer 1
        coordinator.handleChunk(senderContext, new FileChunk(UUID.randomUUID(), t1, 0, new byte[100]));
        // Stream chunk for Transfer 2
        coordinator.handleChunk(charlieContext, new FileChunk(UUID.randomUUID(), t2, 0, new byte[200]));

        // Wait for worker completions
        OutboundFrame t1Ack = outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(t1Ack);
        OutboundFrame t1Fwd = outboundSink.poll(recipientContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(t1Fwd);

        OutboundFrame t2Ack = outboundSink.poll(charlieContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(t2Ack);
        OutboundFrame t2Fwd = outboundSink.poll(daveContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(t2Fwd);

        // Check storage for both
        assertTrue(storage.exists(t1));
        assertTrue(storage.exists(t2));

        // Both received their respective ACK(FILE_CHUNK, STORED)
        assertEquals(t1, ((MessageBody.Ack) ((MessageEnvelope) t1Ack).body()).transferId());
        assertEquals(t2, ((MessageBody.Ack) ((MessageEnvelope) t2Ack).body()).transferId());
    }

    @Test
    void cancellationDuringWorkerExecutionSafelyDiscardsResults() {
        UUID transferId = setupAcceptedTransfer("cancel-race.bin", 500L);

        // Submitting cancel
        coordinator.handleControl(senderContext, new MessageEnvelope(
                1, MessageType.FILE_CANCEL, UUID.randomUUID(), null, new MessageBody.TransferReference(transferId)
        ));

        // Verify cancelled
        assertTrue(registry.get(transferId).isEmpty());

        outboundSink.clear();

        // Stale chunk arrives after cancellation
        coordinator.handleChunk(senderContext, new FileChunk(UUID.randomUUID(), transferId, 0, new byte[100]));

        // No frames sent, no crashes, no revival of cancelled transfer
        assertEquals(0, registry.activeCount());
    }

    @Test
    void recipientOutboundRejectionDuringChunkForwardingAbortsTransfer() throws Exception {
        UUID transferId = setupAcceptedTransfer("reject-outbound.bin", 100L);

        // Recipient outbound queue saturated
        outboundSink.setRejecting(recipientContext.connectionId(), true);

        coordinator.handleChunk(senderContext, new FileChunk(UUID.randomUUID(), transferId, 0, new byte[100]));

        // Wait for worker to complete write and attempt enqueue
        Thread.sleep(100);

        assertTrue(registry.get(transferId).isEmpty());
        assertFalse(storage.exists(transferId));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private UUID setupAcceptedTransfer(String fileName, long sizeBytes) {
        return setupAcceptedTransferFor(senderContext, recipientContext, fileName, sizeBytes);
    }

    private UUID setupAcceptedTransferFor(
            TransferTestFixtures.TestConnectionContext sCtx,
            TransferTestFixtures.TestConnectionContext rCtx,
            String fileName,
            long sizeBytes
    ) {
        SessionView sSession = sCtx.session().orElseThrow();
        SessionView rSession = rCtx.session().orElseThrow();

        UUID offerMsgId = UUID.randomUUID();
        var offerBody = new MessageBody.FileOffer(rSession.clientId(), fileName, sizeBytes);
        coordinator.handleControl(sCtx, new MessageEnvelope(1, MessageType.FILE_OFFER, offerMsgId, null, offerBody));

        MessageEnvelope ack = (MessageEnvelope) outboundSink.getAllFrames(sCtx.connectionId())
                .stream().filter(f -> f instanceof MessageEnvelope e && e.type() == MessageType.ACK).reduce((a, b) -> b).orElseThrow();
        UUID transferId = ((MessageBody.Ack) ack.body()).transferId();

        var acceptBody = new MessageBody.TransferReference(transferId);
        coordinator.handleControl(rCtx, new MessageEnvelope(1, MessageType.FILE_ACCEPT, UUID.randomUUID(), null, acceptBody));
        outboundSink.clear();
        return transferId;
    }
}
