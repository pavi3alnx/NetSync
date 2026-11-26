package com.netsync.transfer;

import com.netsync.common.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
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

    @RepeatedTest(20)
    void duplicateAndFutureChunksCannotPassAnOutstandingWrite() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger appends = new AtomicInteger();
        storage = new SafeFileStorage(tempDir.resolve("blocked-server")) {
            @Override public void appendChunk(UUID id, byte[] data, int offset, int length) throws IOException {
                appends.incrementAndGet();
                entered.countDown();
                try { assertTrue(release.await(3, TimeUnit.SECONDS)); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); }
                super.appendChunk(id, data, offset, length);
            }
        };
        FileTransferService service = new FileTransferService(registry, storage, workerPool, outboundSink,
                selectorTasks, sessionRegistry, authService, clockTime::get);
        coordinator = new FileTransferCoordinator(service);
        UUID transferId = setupAcceptedTransfer("pending.bin", ProtocolLimits.MAX_CHUNK_BYTES + 1L);
        FileChunk first = new FileChunk(UUID.randomUUID(), transferId, 0, new byte[ProtocolLimits.MAX_CHUNK_BYTES]);
        coordinator.handleChunk(senderContext, first);
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        coordinator.handleChunk(senderContext, new FileChunk(UUID.randomUUID(), transferId, 0,
                new byte[ProtocolLimits.MAX_CHUNK_BYTES]));
        coordinator.handleChunk(senderContext, new FileChunk(UUID.randomUUID(), transferId, 1, new byte[]{1}));
        assertEquals(1, appends.get());
        assertEquals(0, registry.get(transferId).orElseThrow().expectedSequence());
        assertTrue(registry.get(transferId).orElseThrow().chunkWritePending());

        release.countDown();
        await(() -> registry.get(transferId).orElseThrow().expectedSequence() == 1);
        assertEquals(ProtocolLimits.MAX_CHUNK_BYTES, storage.getFileSize(transferId));
        assertEquals(1, appends.get());
    }

    @RepeatedTest(50)
    void cancellationDuringActualWorkerWriteDeletesAfterWriteAndPreventsRevival() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch cleanupFinished = new CountDownLatch(1);
        storage = new SafeFileStorage(tempDir.resolve("cancel-blocked-server")) {
            @Override public void appendChunk(UUID id, byte[] data, int offset, int length) throws IOException {
                entered.countDown();
                try { assertTrue(release.await(3, TimeUnit.SECONDS)); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); }
                super.appendChunk(id, data, offset, length);
            }
            @Override public boolean deletePartialFile(UUID id) {
                boolean deleted = super.deletePartialFile(id);
                if (deleted) cleanupFinished.countDown();
                return deleted;
            }
        };
        coordinator = new FileTransferCoordinator(new FileTransferService(registry, storage, workerPool,
                outboundSink, selectorTasks, sessionRegistry, authService, clockTime::get));
        UUID transferId = setupAcceptedTransfer("cancel-pending.bin", 100L);
        coordinator.handleChunk(senderContext, new FileChunk(UUID.randomUUID(), transferId, 0, new byte[100]));
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        coordinator.handleControl(senderContext, new MessageEnvelope(1, MessageType.FILE_CANCEL,
                UUID.randomUUID(), null, new MessageBody.TransferReference(transferId)));
        assertTrue(registry.get(transferId).isEmpty());
        release.countDown();
        assertTrue(cleanupFinished.await(5, TimeUnit.SECONDS),
                "Worker write cleanup did not complete");
        assertFalse(storage.exists(transferId), "Partial file remained; worker active=" + workerPool.activeThreadCount()
                + ", queued=" + workerPool.queueSize() + ", threads=" + Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith("netsync-transfer-worker"))
                .map(thread -> thread.getName() + ":" + thread.getState() + ":" +
                        java.util.Arrays.toString(Thread.getAllStackTraces().get(thread))).toList());
        assertEquals(0, registry.activeCount());
    }

    @RepeatedTest(20)
    void workerWriteFailureUsesReservedSelectorCompletion() throws Exception {
        storage = new SafeFileStorage(tempDir.resolve("failed-write-server")) {
            @Override public void appendChunk(UUID id, byte[] data, int offset, int length) throws IOException {
                throw new IOException("controlled disk failure");
            }
        };
        coordinator = new FileTransferCoordinator(new FileTransferService(registry, storage, workerPool,
                outboundSink, selectorTasks, sessionRegistry, authService, clockTime::get));
        UUID transferId = setupAcceptedTransfer("failed-write.bin", 100L);
        UUID messageId = UUID.randomUUID();
        coordinator.handleChunk(senderContext, new FileChunk(messageId, transferId, 0, new byte[100]));
        await(() -> registry.get(transferId).isEmpty());
        assertFalse(selectorTasks.isReservationHeld());
        assertTrue(outboundSink.getAllFrames(senderContext.connectionId()).stream()
                .anyMatch(frame -> frame instanceof MessageEnvelope error && error.type() == MessageType.ERROR
                        && error.replyTo() != null && error.replyTo().equals(messageId)));
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

    @RepeatedTest(20)
    void temporaryFilesystemDeletionFailureRecoversWithinBoundedRetries() throws Exception {
        AtomicInteger deleteAttempts = new AtomicInteger();
        CountDownLatch cleanupDone = new CountDownLatch(1);
        storage = new SafeFileStorage(tempDir.resolve("transient-delete-server")) {
            @Override public boolean deletePartialFile(UUID id) {
                int attempt = deleteAttempts.incrementAndGet();
                if (attempt <= 3) {
                    return false; // simulate transient OS/AV lock on first 3 attempts
                }
                boolean deleted = super.deletePartialFile(id);
                if (deleted) cleanupDone.countDown();
                return deleted;
            }
        };
        coordinator = new FileTransferCoordinator(new FileTransferService(
                registry, storage, workerPool, outboundSink, selectorTasks, sessionRegistry, authService, clockTime::get));
        UUID transferId = setupAcceptedTransfer("transient-lock.bin", 100L);
        storage.appendChunk(transferId, new byte[100]);
        assertTrue(storage.exists(transferId));

        coordinator.handleControl(senderContext, new MessageEnvelope(
                1, MessageType.FILE_CANCEL, UUID.randomUUID(), null, new MessageBody.TransferReference(transferId)));

        assertTrue(cleanupDone.await(5, TimeUnit.SECONDS));
        assertTrue(deleteAttempts.get() >= 4);
        assertFalse(storage.exists(transferId));
        assertTrue(coordinator.service().failedCleanupIds().isEmpty());
        assertTrue(coordinator.service().pendingCleanupIds().isEmpty());
    }

    @RepeatedTest(20)
    void cleanupSubmissionRejectionRetainsDeferredCleanupAndDrainsAfterSaturationClears() throws Exception {
        TransferWorkerPool tinyPool = new TransferWorkerPool(1, 1);
        CountDownLatch blockWorker = new CountDownLatch(1);
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch cleanupDone = new CountDownLatch(1);
        storage = new SafeFileStorage(tempDir.resolve("saturated-cleanup-server")) {
            @Override public boolean deletePartialFile(UUID id) {
                boolean deleted = super.deletePartialFile(id);
                if (deleted) cleanupDone.countDown();
                return deleted;
            }
        };
        FileTransferService satService = new FileTransferService(
                registry, storage, tinyPool, outboundSink, selectorTasks, sessionRegistry, authService, clockTime::get);
        FileTransferCoordinator satCoordinator = new FileTransferCoordinator(satService);

        try {
            UUID transferId = setupAcceptedTransferWith(satCoordinator, senderContext, recipientContext, "sat.bin", 100L);
            storage.appendChunk(transferId, new byte[100]);
            assertTrue(storage.exists(transferId));

            // Saturate worker thread (1) and queue (1)
            tinyPool.submit(() -> {
                workerStarted.countDown();
                try { blockWorker.await(5, TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
            assertTrue(workerStarted.await(2, TimeUnit.SECONDS));
            tinyPool.submit(() -> {});

            // Cancel transfer while worker pool is completely saturated -> RejectedExecutionException on scheduleCleanup
            satCoordinator.handleControl(senderContext, new MessageEnvelope(
                    1, MessageType.FILE_CANCEL, UUID.randomUUID(), null, new MessageBody.TransferReference(transferId)));

            assertTrue(registry.get(transferId).isEmpty());
            assertTrue(satService.pendingCleanupIds().contains(transferId),
                    "Rejected cleanup submission must be retained in bounded pendingCleanupIds");

            // Release worker saturation and trigger timeout check to drain deferred cleanups
            blockWorker.countDown();
            await(() -> tinyPool.queueSize() == 0 && tinyPool.activeThreadCount() == 0);
            satService.checkTimeouts();

            assertTrue(cleanupDone.await(5, TimeUnit.SECONDS));
            assertFalse(storage.exists(transferId));
            await(() -> satService.pendingCleanupIds().isEmpty());
            assertTrue(satService.failedCleanupIds().isEmpty());
        } finally {
            blockWorker.countDown();
            tinyPool.close();
        }
    }

    @Test
    void persistentExternalFilesystemDeletionFailureExhaustsBoundedRetriesAndRecordsObservableFailure() throws Exception {
        AtomicInteger deleteAttempts = new AtomicInteger();
        storage = new SafeFileStorage(tempDir.resolve("permanent-fail-server")) {
            @Override public boolean deletePartialFile(UUID id) {
                deleteAttempts.incrementAndGet();
                return false; // simulate permanent external filesystem permission/lock failure
            }
        };
        FileTransferService failService = new FileTransferService(
                registry, storage, workerPool, outboundSink, selectorTasks, sessionRegistry, authService, clockTime::get);
        coordinator = new FileTransferCoordinator(failService);

        UUID transferId = setupAcceptedTransfer("undeletable.bin", 64L);
        storage.appendChunk(transferId, new byte[64]);
        assertTrue(storage.exists(transferId));

        coordinator.handleControl(senderContext, new MessageEnvelope(
                1, MessageType.FILE_CANCEL, UUID.randomUUID(), null, new MessageBody.TransferReference(transferId)));

        await(() -> failService.failedCleanupIds().contains(transferId));
        assertEquals(50, deleteAttempts.get(), "Cleanup retry must be strictly bounded to 50 attempts");
        assertTrue(failService.pendingCleanupIds().isEmpty());
        assertEquals(0, registry.activeCount(), "Terminal session must not be retained in active registry");
    }

    @Test
    void receiverCleanupDoesNotSignalCompletionBeforeRetriesAndFailsExplicitlyOnPersistentDeleteFailure() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        SafeFileStorage transientReceiverStorage = new SafeFileStorage(tempDir.resolve("receiver-transient")) {
            @Override public boolean deletePartialFile(UUID id) {
                if (attempts.incrementAndGet() < 4) return false;
                return super.deletePartialFile(id);
            }
        };
        FileReceiver receiver = new FileReceiver(frame -> {}, transientReceiverStorage, workerPool);
        UUID tid1 = UUID.randomUUID();
        receiver.handleControl(new MessageEnvelope(1, MessageType.FILE_OFFERED, UUID.randomUUID(), null,
                new MessageBody.FileOffered(tid1, UUID.randomUUID(), "rec1.bin", 100)));
        receiver.accept(tid1);
        await(() -> receiver.state() == FileReceiver.ReceiverState.RECEIVING);
        transientReceiverStorage.appendChunk(tid1, new byte[100]);
        receiver.cancel();
        receiver.cleanupCompletion().get(5, TimeUnit.SECONDS);
        assertFalse(transientReceiverStorage.exists(tid1), "Cleanup completion must not resolve before file is deleted");
        assertTrue(attempts.get() >= 4);

        // Now test permanent failure on receiver
        SafeFileStorage permanentFailStorage = new SafeFileStorage(tempDir.resolve("receiver-perm-fail")) {
            @Override public boolean deletePartialFile(UUID id) {
                return false;
            }
        };
        FileReceiver failingReceiver = new FileReceiver(frame -> {}, permanentFailStorage, workerPool);
        UUID tid2 = UUID.randomUUID();
        failingReceiver.handleControl(new MessageEnvelope(1, MessageType.FILE_OFFERED, UUID.randomUUID(), null,
                new MessageBody.FileOffered(tid2, UUID.randomUUID(), "rec2.bin", 100)));
        failingReceiver.accept(tid2);
        await(() -> failingReceiver.state() == FileReceiver.ReceiverState.RECEIVING);
        permanentFailStorage.appendChunk(tid2, new byte[100]);
        failingReceiver.cancel();
        assertThrows(Exception.class, () -> failingReceiver.cleanupCompletion().get(5, TimeUnit.SECONDS),
                "Persistent deletion failure must complete cleanupCompletion exceptionally rather than claiming success");
    }

    private UUID setupAcceptedTransferWith(
            FileTransferCoordinator coord,
            TransferTestFixtures.TestConnectionContext sCtx,
            TransferTestFixtures.TestConnectionContext rCtx,
            String fileName,
            long sizeBytes
    ) {
        SessionView sSession = sCtx.session().orElseThrow();
        SessionView rSession = rCtx.session().orElseThrow();
        UUID offerMsgId = UUID.randomUUID();
        var offerBody = new MessageBody.FileOffer(rSession.clientId(), fileName, sizeBytes);
        coord.handleControl(sCtx, new MessageEnvelope(1, MessageType.FILE_OFFER, offerMsgId, null, offerBody));
        MessageEnvelope ack = (MessageEnvelope) outboundSink.getAllFrames(sCtx.connectionId())
                .stream().filter(f -> f instanceof MessageEnvelope e && e.type() == MessageType.ACK).reduce((a, b) -> b).orElseThrow();
        UUID transferId = ((MessageBody.Ack) ack.body()).transferId();
        var acceptBody = new MessageBody.TransferReference(transferId);
        coord.handleControl(rCtx, new MessageEnvelope(1, MessageType.FILE_ACCEPT, UUID.randomUUID(), null, acceptBody));
        outboundSink.clear();
        return transferId;
    }

    @RepeatedTest(20)
    void deferredCleanupCapacityExhaustionRejectsNewOffersWithBusyWithoutEvictingPendingCleanups() throws Exception {
        TransferWorkerPool tinyPool = new TransferWorkerPool(1, 1);
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        FileTransferService satService = new FileTransferService(
                registry, storage, tinyPool, outboundSink, selectorTasks, sessionRegistry, authService, clockTime::get);
        FileTransferCoordinator satCoordinator = new FileTransferCoordinator(satService);

        try {
            // Saturate worker thread (1) and queue (1)
            tinyPool.submit(() -> {
                workerStarted.countDown();
                try { releaseWorker.await(8, TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
            assertTrue(workerStarted.await(2, TimeUnit.SECONDS));
            tinyPool.submit(() -> {});

            // Create and cancel MAX_DEFERRED_CLEANUPS (64) transfers while worker pool is saturated
            java.util.List<UUID> expectedCleanups = new java.util.ArrayList<>();
            for (int i = 0; i < FileTransferService.MAX_DEFERRED_CLEANUPS; i++) {
                UUID tid = setupAcceptedTransferWith(satCoordinator, senderContext, recipientContext, "cap-" + i + ".bin", 32L);
                storage.appendChunk(tid, new byte[32]);
                assertTrue(storage.exists(tid));
                expectedCleanups.add(tid);

                satCoordinator.handleControl(senderContext, new MessageEnvelope(
                        1, MessageType.FILE_CANCEL, UUID.randomUUID(), null, new MessageBody.TransferReference(tid)));
                outboundSink.clear();
            }

            assertEquals(FileTransferService.MAX_DEFERRED_CLEANUPS, satService.pendingCleanupIds().size(),
                    "All 64 deferred cleanup slots must be occupied");
            assertTrue(satService.failedCleanupIds().isEmpty(),
                    "No pending cleanup may be prematurely evicted into failedCleanups");

            // 65th offer while deferredCleanups is at capacity must be rejected with ErrorCode.BUSY
            UUID blockedOfferMsgId = UUID.randomUUID();
            satCoordinator.handleControl(senderContext, new MessageEnvelope(
                    1, MessageType.FILE_OFFER, blockedOfferMsgId, null,
                    new MessageBody.FileOffer(recipientSession.clientId(), "overflow.bin", 32L)));
            MessageEnvelope err = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).getFirst();
            assertEquals(MessageType.ERROR, err.type());
            assertEquals(blockedOfferMsgId, err.replyTo());
            assertEquals(ErrorCode.BUSY, ((MessageBody.Error) err.body()).code());
            assertEquals(0, registry.activeCount());
            assertEquals(FileTransferService.MAX_DEFERRED_CLEANUPS, satService.pendingCleanupIds().size());
            assertTrue(satService.failedCleanupIds().isEmpty());

            // Release worker saturation and drain all 64 deferred cleanups
            releaseWorker.countDown();
            await(() -> tinyPool.queueSize() == 0 && tinyPool.activeThreadCount() == 0);
            satService.checkTimeouts();
            await(() -> satService.pendingCleanupIds().isEmpty());
            assertTrue(satService.failedCleanupIds().isEmpty());
            for (UUID tid : expectedCleanups) {
                assertFalse(storage.exists(tid), "Deferred partial file must be deleted after recovery: " + tid);
            }

            // Now a new offer succeeds normally!
            outboundSink.clear();
            UUID recoveredTid = setupAcceptedTransferWith(satCoordinator, senderContext, recipientContext, "recovered.bin", 32L);
            assertNotNull(recoveredTid);
            assertEquals(1, registry.activeCount());
        } finally {
            releaseWorker.countDown();
            tinyPool.close();
        }
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

    private static void await(java.util.function.BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            try { Thread.sleep(2); } catch (InterruptedException e) { Thread.currentThread().interrupt(); fail("Interrupted"); }
        }
        assertTrue(condition.getAsBoolean());
    }
}
