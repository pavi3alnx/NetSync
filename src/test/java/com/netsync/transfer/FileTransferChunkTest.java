package com.netsync.transfer;

import com.netsync.common.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class FileTransferChunkTest {

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

    private void awaitStorageDeleted(UUID transferId) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (storage.exists(transferId) && System.nanoTime() < deadline) {
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        assertFalse(storage.exists(transferId));
    }

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
        clockTime = new AtomicLong(200_000L);

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
    void validChunkIsStoredForwardedAndAcknowledged() throws Exception {
        int fileSize = 200;
        UUID transferId = setupAcceptedTransfer("binary.dat", fileSize);

        byte[] data = new byte[fileSize];
        for (int i = 0; i < data.length; i++) data[i] = (byte) (i + 1);

        UUID chunkMsgId = UUID.randomUUID();
        FileChunk chunk = new FileChunk(chunkMsgId, transferId, 0, data);

        coordinator.handleChunk(senderContext, chunk);

        // Sender receives ACK(FILE_CHUNK, STORED) after worker completes
        OutboundFrame senderFrame = outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(senderFrame);
        assertTrue(senderFrame instanceof MessageEnvelope);
        MessageEnvelope ack = (MessageEnvelope) senderFrame;
        assertEquals(MessageType.ACK, ack.type());
        assertEquals(chunkMsgId, ack.replyTo());
        MessageBody.Ack ackBody = (MessageBody.Ack) ack.body();
        assertEquals(MessageType.FILE_CHUNK, ackBody.operation());
        assertEquals(AckStatus.STORED, ackBody.status());
        assertEquals(transferId, ackBody.transferId());

        // Verify chunk stored on disk
        assertTrue(storage.exists(transferId));
        assertEquals(fileSize, storage.getFileSize(transferId));

        // Recipient receives forwarded FileChunk
        OutboundFrame recipientFrame = outboundSink.poll(recipientContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(recipientFrame);
        assertTrue(recipientFrame instanceof FileChunk);
        FileChunk forwarded = (FileChunk) recipientFrame;
        assertEquals(transferId, forwarded.transferId());
        assertEquals(0, forwarded.sequence());
        assertArrayEquals(data, forwarded.data());
        assertNotEquals(chunkMsgId, forwarded.messageId(), "Forwarded chunk must receive a fresh message UUID");
    }

    @Test
    void outOfOrderChunkAbortsTransferAndSendsInvalidSequenceError() throws Exception {
        UUID transferId = setupAcceptedTransfer("test.bin", 500L);

        // Sequence 1 when sequence 0 is expected
        byte[] data = new byte[250];
        UUID chunkMsgId = UUID.randomUUID();
        FileChunk chunk = new FileChunk(chunkMsgId, transferId, 1, data);

        coordinator.handleChunk(senderContext, chunk);

        // Sender receives ERROR(INVALID_SEQUENCE)
        OutboundFrame frame = outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(frame);
        MessageEnvelope err = (MessageEnvelope) frame;
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.INVALID_SEQUENCE, ((MessageBody.Error) err.body()).code());
        assertEquals(chunkMsgId, err.replyTo());

        // Transfer aborted and storage cleaned
        assertTrue(registry.get(transferId).isEmpty());
        awaitStorageDeleted(transferId);
    }

    @Test
    void duplicateChunkSequenceAbortsTransfer() throws Exception {
        int chunkSize = ProtocolLimits.MAX_CHUNK_BYTES;
        UUID transferId = setupAcceptedTransfer("large.bin", chunkSize * 2L);
        byte[] data = new byte[chunkSize];

        // Send chunk 0 successfully
        coordinator.handleChunk(senderContext, new FileChunk(UUID.randomUUID(), transferId, 0, data));
        assertNotNull(outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(recipientContext.connectionId(), 5, TimeUnit.SECONDS));

        // Send duplicate chunk 0
        UUID dupMsgId = UUID.randomUUID();
        coordinator.handleChunk(senderContext, new FileChunk(dupMsgId, transferId, 0, data));

        OutboundFrame frame = outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(frame);
        MessageEnvelope err = (MessageEnvelope) frame;
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.INVALID_SEQUENCE, ((MessageBody.Error) err.body()).code());
        assertEquals(dupMsgId, err.replyTo());

        assertTrue(registry.get(transferId).isEmpty());
        awaitStorageDeleted(transferId);
    }

    @Test
    void nonFinalChunkNotFullSizeIsRejected() throws Exception {
        long declaredSize = ProtocolLimits.MAX_CHUNK_BYTES * 2L;
        UUID transferId = setupAcceptedTransfer("padded.bin", declaredSize);

        // Send short chunk (e.g. 100 bytes) when file size is 32 KiB
        byte[] shortData = new byte[100];
        coordinator.handleChunk(senderContext, new FileChunk(UUID.randomUUID(), transferId, 0, shortData));

        OutboundFrame frame = outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(frame);
        MessageEnvelope err = (MessageEnvelope) frame;
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.INVALID_REQUEST, ((MessageBody.Error) err.body()).code());

        assertTrue(registry.get(transferId).isEmpty());
    }

    @Test
    void chunkExceedingDeclaredFileSizeIsRejected() throws Exception {
        UUID transferId = setupAcceptedTransfer("small.bin", 50L);

        byte[] oversizedData = new byte[60];
        coordinator.handleChunk(senderContext, new FileChunk(UUID.randomUUID(), transferId, 0, oversizedData));

        OutboundFrame frame = outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(frame);
        MessageEnvelope err = (MessageEnvelope) frame;
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.SIZE_LIMIT, ((MessageBody.Error) err.body()).code());

        assertTrue(registry.get(transferId).isEmpty());
    }

    @Test
    void unauthorizedCallerCannotUploadChunks() throws Exception {
        UUID transferId = setupAcceptedTransfer("data.bin", 100L);

        // Recipient tries to upload chunk instead of sender
        byte[] data = new byte[100];
        coordinator.handleChunk(recipientContext, new FileChunk(UUID.randomUUID(), transferId, 0, data));

        OutboundFrame frame = outboundSink.poll(recipientContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(frame);
        MessageEnvelope err = (MessageEnvelope) frame;
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.FORBIDDEN, ((MessageBody.Error) err.body()).code());

        // Transfer still intact for sender
        assertTrue(registry.get(transferId).isPresent());
    }

    @Test
    void completeLifecycleMultiChunkAndChecksumVerification() throws Exception {
        int chunkSize = ProtocolLimits.MAX_CHUNK_BYTES;
        int lastChunkSize = 1000;
        int totalSize = chunkSize + lastChunkSize;

        UUID transferId = setupAcceptedTransfer("bigfile.bin", totalSize);

        byte[] chunk0 = new byte[chunkSize];
        byte[] chunk1 = new byte[lastChunkSize];
        for (int i = 0; i < chunkSize; i++) chunk0[i] = (byte) (i & 0xFF);
        for (int i = 0; i < lastChunkSize; i++) chunk1[i] = (byte) ((i + 7) & 0xFF);

        // Upload chunk 0
        coordinator.handleChunk(senderContext, new FileChunk(UUID.randomUUID(), transferId, 0, chunk0));
        assertNotNull(outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(recipientContext.connectionId(), 5, TimeUnit.SECONDS));

        // Upload chunk 1 (final)
        coordinator.handleChunk(senderContext, new FileChunk(UUID.randomUUID(), transferId, 1, chunk1));
        assertNotNull(outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(recipientContext.connectionId(), 5, TimeUnit.SECONDS));

        // Calculate expected SHA-256 from combined bytes
        byte[] allBytes = new byte[totalSize];
        System.arraycopy(chunk0, 0, allBytes, 0, chunkSize);
        System.arraycopy(chunk1, 0, allBytes, chunkSize, lastChunkSize);
        String expectedSha256 = TransferChecksum.computeDigestHex(allBytes);

        // Sender sends FILE_COMPLETE
        UUID completeMsgId = UUID.randomUUID();
        var completeBody = new MessageBody.FileComplete(transferId, 2, expectedSha256);
        coordinator.handleControl(senderContext, new MessageEnvelope(1, MessageType.FILE_COMPLETE, completeMsgId, null, completeBody));

        // Sender receives ACK(FILE_COMPLETE, ACCEPTED)
        OutboundFrame sFrame = outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(sFrame);
        MessageEnvelope senderAck = (MessageEnvelope) sFrame;
        assertEquals(MessageType.ACK, senderAck.type());
        assertEquals(completeMsgId, senderAck.replyTo());
        assertEquals(MessageType.FILE_COMPLETE, ((MessageBody.Ack) senderAck.body()).operation());

        // Recipient receives forwarded FILE_COMPLETE
        OutboundFrame rFrame = outboundSink.poll(recipientContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(rFrame);
        MessageEnvelope recipientComplete = (MessageEnvelope) rFrame;
        assertEquals(MessageType.FILE_COMPLETE, recipientComplete.type());
        assertNull(recipientComplete.replyTo());
        assertEquals(expectedSha256, ((MessageBody.FileComplete) recipientComplete.body()).sha256());

        // Recipient sends FILE_RECEIVED with matching hash
        UUID receivedMsgId = UUID.randomUUID();
        var receivedBody = new MessageBody.FileReceived(transferId, expectedSha256);
        coordinator.handleControl(recipientContext, new MessageEnvelope(1, MessageType.FILE_RECEIVED, receivedMsgId, null, receivedBody));

        // Recipient receives ACK(FILE_RECEIVED, ACCEPTED)
        OutboundFrame rAckFrame = outboundSink.poll(recipientContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(rAckFrame);
        MessageEnvelope recipientAck = (MessageEnvelope) rAckFrame;
        assertEquals(MessageType.ACK, recipientAck.type());
        assertEquals(receivedMsgId, recipientAck.replyTo());

        // Sender receives FILE_STATUS(COMPLETED)
        OutboundFrame sStatusFrame = outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(sStatusFrame);
        MessageEnvelope senderStatus = (MessageEnvelope) sStatusFrame;
        assertEquals(MessageType.FILE_STATUS, senderStatus.type());
        assertEquals(FileStatusState.COMPLETED, ((MessageBody.FileStatus) senderStatus.body()).state());

        // Transfer slots freed and server partial file removed
        assertTrue(registry.get(transferId).isEmpty());
        awaitStorageDeleted(transferId);
    }

    @Test
    void checksumMismatchOnFileCompleteAbortsTransfer() throws Exception {
        int size = 100;
        UUID transferId = setupAcceptedTransfer("data.bin", size);
        byte[] data = new byte[size];
        coordinator.handleChunk(senderContext, new FileChunk(UUID.randomUUID(), transferId, 0, data));
        assertNotNull(outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS));
        assertNotNull(outboundSink.poll(recipientContext.connectionId(), 5, TimeUnit.SECONDS));

        // Send incorrect SHA-256
        String wrongSha256 = "0".repeat(64);
        UUID completeMsgId = UUID.randomUUID();
        var completeBody = new MessageBody.FileComplete(transferId, 1, wrongSha256);
        coordinator.handleControl(senderContext, new MessageEnvelope(1, MessageType.FILE_COMPLETE, completeMsgId, null, completeBody));

        // Sender receives ERROR(TRANSFER_FAILED)
        OutboundFrame frame = outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(frame);
        MessageEnvelope err = (MessageEnvelope) frame;
        assertEquals(MessageType.ERROR, err.type());
        assertEquals(ErrorCode.TRANSFER_FAILED, ((MessageBody.Error) err.body()).code());
        assertEquals(completeMsgId, err.replyTo());

        assertTrue(registry.get(transferId).isEmpty());
        awaitStorageDeleted(transferId);
    }

    @Test
    void emptyFileTransferCompletesWithZeroChunks() throws Exception {
        UUID transferId = setupAcceptedTransfer("empty.txt", 0L);

        // Sender immediately sends FILE_COMPLETE with 0 chunks and EMPTY_SHA256
        UUID completeMsgId = UUID.randomUUID();
        var completeBody = new MessageBody.FileComplete(transferId, 0, TransferChecksum.EMPTY_SHA256);
        coordinator.handleControl(senderContext, new MessageEnvelope(1, MessageType.FILE_COMPLETE, completeMsgId, null, completeBody));

        // Sender receives ACK
        OutboundFrame sFrame = outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(sFrame);
        MessageEnvelope senderAck = (MessageEnvelope) sFrame;
        assertEquals(MessageType.ACK, senderAck.type());

        // Recipient receives FILE_COMPLETE
        OutboundFrame rFrame = outboundSink.poll(recipientContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(rFrame);
        MessageEnvelope recipientComplete = (MessageEnvelope) rFrame;
        assertEquals(MessageType.FILE_COMPLETE, recipientComplete.type());
        assertEquals(TransferChecksum.EMPTY_SHA256, ((MessageBody.FileComplete) recipientComplete.body()).sha256());

        // Recipient confirms with FILE_RECEIVED
        UUID receivedMsgId = UUID.randomUUID();
        var receivedBody = new MessageBody.FileReceived(transferId, TransferChecksum.EMPTY_SHA256);
        coordinator.handleControl(recipientContext, new MessageEnvelope(1, MessageType.FILE_RECEIVED, receivedMsgId, null, receivedBody));

        // Final completion verified
        OutboundFrame rAck = outboundSink.poll(recipientContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(rAck);
        OutboundFrame sStatus = outboundSink.poll(senderContext.connectionId(), 5, TimeUnit.SECONDS);
        assertNotNull(sStatus);
        MessageEnvelope senderStatus = (MessageEnvelope) sStatus;
        assertEquals(FileStatusState.COMPLETED, ((MessageBody.FileStatus) senderStatus.body()).state());

        assertTrue(registry.get(transferId).isEmpty());
    }

    // =========================================================================
    // Helper
    // =========================================================================

    private UUID setupAcceptedTransfer(String fileName, long sizeBytes) {
        UUID offerMsgId = UUID.randomUUID();
        var offerBody = new MessageBody.FileOffer(recipientSession.clientId(), fileName, sizeBytes);
        coordinator.handleControl(senderContext, new MessageEnvelope(1, MessageType.FILE_OFFER, offerMsgId, null, offerBody));
        MessageEnvelope ack = (MessageEnvelope) outboundSink.getAllFrames(senderContext.connectionId()).get(0);
        UUID transferId = ((MessageBody.Ack) ack.body()).transferId();

        var acceptBody = new MessageBody.TransferReference(transferId);
        coordinator.handleControl(recipientContext, new MessageEnvelope(1, MessageType.FILE_ACCEPT, UUID.randomUUID(), null, acceptBody));
        outboundSink.clear();
        return transferId;
    }
}
