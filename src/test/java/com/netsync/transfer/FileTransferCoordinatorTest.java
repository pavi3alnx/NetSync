package com.netsync.transfer;

import com.netsync.common.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class FileTransferCoordinatorTest {

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

    private void awaitStorageDeleted(UUID transferId) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (storage.exists(transferId) && System.nanoTime() < deadline) {
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        assertFalse(storage.exists(transferId));
    }

    @BeforeEach
    void setUp() {
        registry = new TransferRegistry();
        storage = new SafeFileStorage(tempDir.resolve("server"));
        workerPool = new TransferWorkerPool(2, 32);
        outboundSink = new TransferTestFixtures.TestOutboundSink();
        selectorTasks = new TransferTestFixtures.TestDirectSelectorTasks();
        sessionRegistry = new TransferTestFixtures.TestSessionRegistry();
        authService = new TransferTestFixtures.TestAuthorizationService();
        clockTime = new AtomicLong(100_000L);

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

    // =========================================================================
    // FILE_OFFER Tests
    // =========================================================================

    @Test
    void validFileOfferCreatesPendingTransferAndNotifiesBothParties() {
        UUID offerMsgId = UUID.randomUUID();
        var offerBody = new MessageBody.FileOffer(recipientSession.clientId(), "report.pdf", 1024L);
        var envelope = new MessageEnvelope(1, MessageType.FILE_OFFER, offerMsgId, null, offerBody);

        coordinator.handleControl(senderContext, envelope);

        // Sender receives ACK(ACCEPTED)
        List<OutboundFrame> senderFrames = outboundSink.getAllFrames(senderContext.connectionId());
        assertEquals(1, senderFrames.size());
        assertTrue(senderFrames.get(0) instanceof MessageEnvelope);
        MessageEnvelope senderAck = (MessageEnvelope) senderFrames.get(0);
        assertEquals(MessageType.ACK, senderAck.type());
        assertEquals(offerMsgId, senderAck.replyTo());
        assertTrue(senderAck.body() instanceof MessageBody.Ack);
        MessageBody.Ack ackBody = (MessageBody.Ack) senderAck.body();
        assertEquals(MessageType.FILE_OFFER, ackBody.operation());
        assertEquals(AckStatus.ACCEPTED, ackBody.status());
        assertNotNull(ackBody.transferId());

        UUID transferId = ackBody.transferId();

        // Recipient receives FILE_OFFERED
        List<OutboundFrame> recipientFrames = outboundSink.getAllFrames(recipientContext.connectionId());
        assertEquals(1, recipientFrames.size());
        MessageEnvelope recipientOffer = (MessageEnvelope) recipientFrames.get(0);
        assertEquals(MessageType.FILE_OFFERED, recipientOffer.type());
        assertNull(recipientOffer.replyTo());
        assertTrue(recipientOffer.body() instanceof MessageBody.FileOffered);
        MessageBody.FileOffered offeredBody = (MessageBody.FileOffered) recipientOffer.body();
        assertEquals(transferId, offeredBody.transferId());
        assertEquals(senderSession.clientId(), offeredBody.senderClientId());
        assertEquals("report.pdf", offeredBody.fileName());
        assertEquals(1024L, offeredBody.sizeBytes());

        // Registry holds transfer in OFFERED state
        Optional<TransferSession> sessionOpt = registry.get(transferId);
        assertTrue(sessionOpt.isPresent());
        assertEquals(TransferState.OFFERED, sessionOpt.get().state());
    }

    @Test
    void fileOfferFromUnauthenticatedConnectionReturnsError() {
        var unauthContext = TransferTestFixtures.TestConnectionContext.unauthenticated();
        var envelope = new MessageEnvelope(1, MessageType.FILE_OFFER, UUID.randomUUID(), null,
                new MessageBody.FileOffer(recipientSession.clientId(), "doc.txt", 100L));

        coordinator.handleControl(unauthContext, envelope);

        List<OutboundFrame> frames = outboundSink.getAllFrames(unauthContext.connectionId());
        assertEquals(1, frames.size());
        MessageEnvelope err = (MessageEnvelope) frames.get(0);
        assertEquals(MessageType.ERROR, err.type());
        MessageBody.Error errBody = (MessageBody.Error) err.body();
        assertEquals(ErrorCode.UNAUTHENTICATED, errBody.code());
    }

    @Test
    void fileOfferToSelfIsRejected() {
        var envelope = new MessageEnvelope(1, MessageType.FILE_OFFER, UUID.randomUUID(), null,
                new MessageBody.FileOffer(senderSession.clientId(), "doc.txt", 100L));

        coordinator.handleControl(senderContext, envelope);

        MessageEnvelope err = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.ERROR, err.type());
        MessageBody.Error errBody = (MessageBody.Error) err.body();
        assertEquals(ErrorCode.INVALID_REQUEST, errBody.code());
    }

    @Test
    void fileOfferWithTraversalNameIsRejected() {
        var envelope = new MessageEnvelope(1, MessageType.FILE_OFFER, UUID.randomUUID(), null,
                new MessageBody.FileOffer(recipientSession.clientId(), "../evil.sh", 100L));

        coordinator.handleControl(senderContext, envelope);

        MessageEnvelope err = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.INVALID_REQUEST, ((MessageBody.Error) err.body()).code());
    }

    @Test
    void fileOfferExceedingMaxFileBytesReturnsSizeLimitError() {
        var envelope = new MessageEnvelope(1, MessageType.FILE_OFFER, UUID.randomUUID(), null,
                new MessageBody.FileOffer(recipientSession.clientId(), "huge.iso", ProtocolLimits.MAX_FILE_BYTES + 1L));

        coordinator.handleControl(senderContext, envelope);

        MessageEnvelope err = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.SIZE_LIMIT, ((MessageBody.Error) err.body()).code());
    }

    @Test
    void fileOfferToNonExistentRecipientReturnsRecipientUnavailable() {
        UUID unknownRecipientId = UUID.randomUUID();
        var envelope = new MessageEnvelope(1, MessageType.FILE_OFFER, UUID.randomUUID(), null,
                new MessageBody.FileOffer(unknownRecipientId, "test.txt", 50L));

        coordinator.handleControl(senderContext, envelope);

        MessageEnvelope err = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.RECIPIENT_UNAVAILABLE, ((MessageBody.Error) err.body()).code());
    }

    @Test
    void fileOfferWhenClientAlreadyActiveReturnsBusy() {
        // Register an active transfer for sender
        UUID activeTransferId = UUID.randomUUID();
        TransferSession existing = new TransferSession(
                activeTransferId, senderSession.clientId(), recipientSession.clientId(),
                "file1.txt", 100L, senderContext.connectionId(), clockTime.get()
        );
        registry.registerOffer(existing);

        // Attempt second offer
        var envelope = new MessageEnvelope(1, MessageType.FILE_OFFER, UUID.randomUUID(), null,
                new MessageBody.FileOffer(recipientSession.clientId(), "file2.txt", 200L));

        coordinator.handleControl(senderContext, envelope);

        MessageEnvelope err = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.BUSY, ((MessageBody.Error) err.body()).code());
    }

    @Test
    void fileOfferWhenRecipientOutboundRejectsRemovesPendingTransfer() {
        outboundSink.setRejecting(recipientContext.connectionId(), true);

        var envelope = new MessageEnvelope(1, MessageType.FILE_OFFER, UUID.randomUUID(), null,
                new MessageBody.FileOffer(recipientSession.clientId(), "doc.txt", 100L));

        coordinator.handleControl(senderContext, envelope);

        MessageEnvelope err = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.RECIPIENT_UNAVAILABLE, ((MessageBody.Error) err.body()).code());
        assertEquals(0, registry.activeCount());
    }

    // =========================================================================
    // FILE_ACCEPT & FILE_REJECT Tests
    // =========================================================================

    @Test
    void validFileAcceptTransitionsStateAndNotifiesSender() {
        UUID transferId = createOffer("data.bin", 5000L);
        outboundSink.clear();

        UUID acceptMsgId = UUID.randomUUID();
        var envelope = new MessageEnvelope(1, MessageType.FILE_ACCEPT, acceptMsgId, null,
                new MessageBody.TransferReference(transferId));

        coordinator.handleControl(recipientContext, envelope);

        // Recipient receives ACK(FILE_ACCEPT, ACCEPTED)
        MessageEnvelope recipientAck = (MessageEnvelope) outboundSink.getAllFrames(recipientContext.connectionId()).get(0);
        assertEquals(MessageType.ACK, recipientAck.type());
        assertEquals(acceptMsgId, recipientAck.replyTo());
        assertEquals(MessageType.FILE_ACCEPT, ((MessageBody.Ack) recipientAck.body()).operation());
        assertEquals(AckStatus.ACCEPTED, ((MessageBody.Ack) recipientAck.body()).status());

        // Sender receives FILE_STATUS(ACCEPTED)
        MessageEnvelope senderStatus = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.FILE_STATUS, senderStatus.type());
        MessageBody.FileStatus statusBody = (MessageBody.FileStatus) senderStatus.body();
        assertEquals(transferId, statusBody.transferId());
        assertEquals(FileStatusState.ACCEPTED, statusBody.state());

        // Session state is now ACCEPTED
        assertEquals(TransferState.ACCEPTED, registry.get(transferId).orElseThrow().state());
    }

    @Test
    void fileAcceptByUnauthorizedClientIsForbidden() {
        UUID transferId = createOffer("data.bin", 5000L);
        outboundSink.clear();

        // Third-party caller "charlie"
        var charlieContext = TransferTestFixtures.TestConnectionContext.create("charlie");
        sessionRegistry.register(charlieContext.session().orElseThrow());

        var envelope = new MessageEnvelope(1, MessageType.FILE_ACCEPT, UUID.randomUUID(), null,
                new MessageBody.TransferReference(transferId));

        coordinator.handleControl(charlieContext, envelope);

        MessageEnvelope err = (MessageEnvelope) outboundSink.getAllFrames(charlieContext.connectionId()).get(0);
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.FORBIDDEN, ((MessageBody.Error) err.body()).code());
    }

    @Test
    void fileAcceptOnNonExistentTransferReturnsInvalidRequest() {
        UUID randomTransferId = UUID.randomUUID();
        var envelope = new MessageEnvelope(1, MessageType.FILE_ACCEPT, UUID.randomUUID(), null,
                new MessageBody.TransferReference(randomTransferId));

        coordinator.handleControl(recipientContext, envelope);

        MessageEnvelope err = (MessageEnvelope) outboundSink.getAllFrames(recipientContext.connectionId()).get(0);
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.INVALID_REQUEST, ((MessageBody.Error) err.body()).code());
    }

    @Test
    void fileAcceptWhenAlreadyAcceptedReturnsInvalidState() {
        UUID transferId = createOffer("data.bin", 5000L);
        registry.get(transferId).orElseThrow().transitionTo(TransferState.ACCEPTED, clockTime.get());
        outboundSink.clear();

        var envelope = new MessageEnvelope(1, MessageType.FILE_ACCEPT, UUID.randomUUID(), null,
                new MessageBody.TransferReference(transferId));

        coordinator.handleControl(recipientContext, envelope);

        MessageEnvelope err = (MessageEnvelope) outboundSink.getAllFrames(recipientContext.connectionId()).get(0);
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.INVALID_STATE, ((MessageBody.Error) err.body()).code());
    }

    @Test
    void validFileRejectTransitionsStateCleansStorageAndNotifiesSender() throws IOException {
        UUID transferId = createOffer("data.bin", 5000L);
        storage.appendChunk(transferId, new byte[]{1, 2, 3});
        assertTrue(storage.exists(transferId));
        outboundSink.clear();

        UUID rejectMsgId = UUID.randomUUID();
        var envelope = new MessageEnvelope(1, MessageType.FILE_REJECT, rejectMsgId, null,
                new MessageBody.TransferReference(transferId));

        coordinator.handleControl(recipientContext, envelope);

        // Recipient receives ACK(FILE_REJECT, ACCEPTED)
        MessageEnvelope recipientAck = (MessageEnvelope) outboundSink.getAllFrames(recipientContext.connectionId()).get(0);
        assertEquals(MessageType.ACK, recipientAck.type());
        assertEquals(rejectMsgId, recipientAck.replyTo());

        // Sender receives FILE_STATUS(REJECTED)
        MessageEnvelope senderStatus = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.FILE_STATUS, senderStatus.type());
        assertEquals(FileStatusState.REJECTED, ((MessageBody.FileStatus) senderStatus.body()).state());

        // Registry entry removed and partial file deleted
        assertTrue(registry.get(transferId).isEmpty());
        awaitStorageDeleted(transferId);
    }

    // =========================================================================
    // FILE_CANCEL Tests
    // =========================================================================

    @Test
    void senderCanCancelActiveTransfer() throws IOException {
        UUID transferId = createOffer("archive.tar", 2000L);
        storage.appendChunk(transferId, new byte[]{10, 20});
        outboundSink.clear();

        UUID cancelMsgId = UUID.randomUUID();
        var envelope = new MessageEnvelope(1, MessageType.FILE_CANCEL, cancelMsgId, null,
                new MessageBody.TransferReference(transferId));

        coordinator.handleControl(senderContext, envelope);

        // Sender receives ACK(FILE_CANCEL, ACCEPTED)
        MessageEnvelope senderAck = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.ACK, senderAck.type());
        assertEquals(cancelMsgId, senderAck.replyTo());
        assertEquals(MessageType.FILE_CANCEL, ((MessageBody.Ack) senderAck.body()).operation());

        // Recipient receives FILE_STATUS(CANCELLED)
        MessageEnvelope recipientStatus = (MessageEnvelope) outboundSink.getAllFrames(recipientContext.connectionId()).get(0);
        assertEquals(MessageType.FILE_STATUS, recipientStatus.type());
        assertEquals(FileStatusState.CANCELLED, ((MessageBody.FileStatus) recipientStatus.body()).state());

        // Storage and registry cleaned
        awaitStorageDeleted(transferId);
        assertTrue(registry.get(transferId).isEmpty());
    }

    @Test
    void recipientCanCancelActiveTransfer() {
        UUID transferId = createOffer("archive.tar", 2000L);
        outboundSink.clear();

        var envelope = new MessageEnvelope(1, MessageType.FILE_CANCEL, UUID.randomUUID(), null,
                new MessageBody.TransferReference(transferId));

        coordinator.handleControl(recipientContext, envelope);

        MessageEnvelope recipientAck = (MessageEnvelope) outboundSink.getAllFrames(recipientContext.connectionId()).get(0);
        assertEquals(MessageType.ACK, recipientAck.type());

        MessageEnvelope senderStatus = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.FILE_STATUS, senderStatus.type());
        assertEquals(FileStatusState.CANCELLED, ((MessageBody.FileStatus) senderStatus.body()).state());
    }

    @Test
    void unauthorizedPartyCannotCancelTransfer() {
        UUID transferId = createOffer("archive.tar", 2000L);
        outboundSink.clear();

        var charlieContext = TransferTestFixtures.TestConnectionContext.create("charlie");
        sessionRegistry.register(charlieContext.session().orElseThrow());

        var envelope = new MessageEnvelope(1, MessageType.FILE_CANCEL, UUID.randomUUID(), null,
                new MessageBody.TransferReference(transferId));

        coordinator.handleControl(charlieContext, envelope);

        MessageEnvelope err = (MessageEnvelope) outboundSink.getAllFrames(charlieContext.connectionId()).get(0);
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.FORBIDDEN, ((MessageBody.Error) err.body()).code());
    }

    // =========================================================================
    // FILE_STATUS Query Tests
    // =========================================================================

    @Test
    void participantCanQueryTransferStatus() {
        UUID transferId = createOffer("data.csv", 500L);
        outboundSink.clear();

        UUID queryMsgId = UUID.randomUUID();
        var envelope = new MessageEnvelope(1, MessageType.FILE_STATUS, queryMsgId, null,
                new MessageBody.FileStatus(transferId, FileStatusState.ACCEPTED, null));

        coordinator.handleControl(senderContext, envelope);

        MessageEnvelope reply = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.FILE_STATUS, reply.type());
        assertEquals(queryMsgId, reply.replyTo());
        assertEquals(transferId, ((MessageBody.FileStatus) reply.body()).transferId());
    }

    // =========================================================================
    // Disconnection & Timeout Tests
    // =========================================================================

    @Test
    void senderDisconnectionCleansTransferAndNotifiesRecipient() throws IOException {
        UUID transferId = createOffer("log.txt", 100L);
        storage.appendChunk(transferId, new byte[]{1, 2});
        assertTrue(storage.exists(transferId));
        outboundSink.clear();

        coordinator.onDisconnected(senderContext.connectionId());

        awaitStorageDeleted(transferId);
        assertTrue(registry.get(transferId).isEmpty());

        // Recipient received FILE_STATUS(FAILED)
        MessageEnvelope status = (MessageEnvelope) outboundSink.getAllFrames(recipientContext.connectionId()).get(0);
        assertEquals(MessageType.FILE_STATUS, status.type());
        assertEquals(FileStatusState.FAILED, ((MessageBody.FileStatus) status.body()).state());
    }

    @Test
    void recipientDisconnectionCleansTransferAndNotifiesSender() throws IOException {
        UUID transferId = createOffer("log.txt", 100L);
        storage.appendChunk(transferId, new byte[]{1, 2});
        outboundSink.clear();

        coordinator.onDisconnected(recipientContext.connectionId());

        awaitStorageDeleted(transferId);
        assertTrue(registry.get(transferId).isEmpty());

        // Sender received FILE_STATUS(FAILED)
        MessageEnvelope status = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(MessageType.FILE_STATUS, status.type());
        assertEquals(FileStatusState.FAILED, ((MessageBody.FileStatus) status.body()).state());
    }

    @Test
    void timeoutCheckCleansExpiredTransfersAndNotifiesBothParties() throws IOException {
        UUID transferId = createOffer("timeout.txt", 100L);
        storage.appendChunk(transferId, new byte[]{1, 2});
        outboundSink.clear();

        // Advance clock past 30 seconds
        clockTime.addAndGet(31_000L);

        int expiredCount = coordinator.service().checkTimeouts();
        assertEquals(1, expiredCount);

        awaitStorageDeleted(transferId);
        assertTrue(registry.get(transferId).isEmpty());

        // Both parties notified with FILE_STATUS(FAILED)
        MessageEnvelope senderStatus = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        assertEquals(FileStatusState.FAILED, ((MessageBody.FileStatus) senderStatus.body()).state());

        MessageEnvelope recipientStatus = (MessageEnvelope) outboundSink.getAllFrames(recipientContext.connectionId()).get(0);
        assertEquals(FileStatusState.FAILED, ((MessageBody.FileStatus) recipientStatus.body()).state());
    }

    // =========================================================================
    // Helper
    // =========================================================================

    private UUID createOffer(String fileName, long sizeBytes) {
        UUID offerMsgId = UUID.randomUUID();
        var offerBody = new MessageBody.FileOffer(recipientSession.clientId(), fileName, sizeBytes);
        var envelope = new MessageEnvelope(1, MessageType.FILE_OFFER, offerMsgId, null, offerBody);
        coordinator.handleControl(senderContext, envelope);
        MessageEnvelope senderAck = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        return ((MessageBody.Ack) senderAck.body()).transferId();
    }
}
