package com.netsync.transfer;

import com.netsync.common.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit and integration tests for {@link FileSender} and {@link FileReceiver}.
 * <p>
 * Tests all 12 required scenarios covering small, multi-chunk, empty, exact 16 KiB,
 * oversized, sequence violations, duplicate chunks, checksum corruption, interruption,
 * rejection, cancellation, and concurrent transfers.
 */
@Timeout(15)
class ClientFileTransferTest {
    private void awaitStorageDeleted(SafeFileStorage storage, UUID transferId) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (storage.exists(transferId) && System.nanoTime() < deadline) {
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        assertFalse(storage.exists(transferId));
    }

    @TempDir
    Path tempDir;

    private Path senderDir;
    private Path receiverDir;
    private SafeFileStorage receiverStorage;
    private TransferWorkerPool workerPool;

    @BeforeEach
    void setUp() throws IOException {
        senderDir = tempDir.resolve("sender");
        receiverDir = tempDir.resolve("receiver");
        Files.createDirectories(senderDir);
        Files.createDirectories(receiverDir);
        receiverStorage = new SafeFileStorage(receiverDir);
        workerPool = new TransferWorkerPool(4, 64);
    }

    @AfterEach
    void tearDown() {
        if (workerPool != null) {
            workerPool.close();
        }
    }

    /**
     * Test harness that simulates the server relay between sender and receiver.
     */
    private static final class SimulatedServerRelay {
        private final UUID senderClientId = UUID.randomUUID();
        private final UUID recipientClientId = UUID.randomUUID();
        private FileSender sender;
        private FileReceiver receiver;
        private volatile UUID currentTransferId;
        private volatile boolean autoAccept = true;
        private volatile boolean corruptChecksum = false;
        private volatile boolean duplicateFirstChunk = false;
        private volatile boolean dropChunks = false;
        private final java.util.concurrent.atomic.AtomicBoolean firstChunkSeen = new java.util.concurrent.atomic.AtomicBoolean(false);

        void setSender(FileSender sender) {
            this.sender = sender;
        }

        void setReceiver(FileReceiver receiver) {
            this.receiver = receiver;
        }

        synchronized void onSenderOutbound(OutboundFrame frame) {
            if (frame instanceof MessageEnvelope envelope) {
                switch (envelope.type()) {
                    case FILE_OFFER -> {
                        MessageBody.FileOffer offer = (MessageBody.FileOffer) envelope.body();
                        currentTransferId = UUID.randomUUID();

                        // 1. Send ACK(FILE_OFFER) to sender
                        if (sender != null) {
                            MessageEnvelope ackOffer = new MessageEnvelope(
                                    ProtocolLimits.VERSION,
                                    MessageType.ACK,
                                    UUID.randomUUID(),
                                    envelope.messageId(),
                                    new MessageBody.Ack(MessageType.FILE_OFFER, AckStatus.ACCEPTED, currentTransferId)
                            );
                            sender.handleControl(ackOffer);
                        }

                        // 2. Forward FILE_OFFERED to receiver
                        if (receiver != null) {
                            MessageEnvelope offered = new MessageEnvelope(
                                    ProtocolLimits.VERSION,
                                    MessageType.FILE_OFFERED,
                                    UUID.randomUUID(),
                                    null,
                                    new MessageBody.FileOffered(currentTransferId, senderClientId, offer.fileName(), offer.sizeBytes())
                            );
                            receiver.handleControl(offered);

                            if (autoAccept && receiver.state() == FileReceiver.ReceiverState.OFFERED) {
                                receiver.accept(currentTransferId);
                            }
                        }
                    }
                    case FILE_COMPLETE -> {
                        MessageBody.FileComplete complete = (MessageBody.FileComplete) envelope.body();

                        // 1. Forward FILE_COMPLETE to receiver (optionally with corrupted hash)
                        if (receiver != null) {
                            String hashToSend = corruptChecksum
                                    ? "0000000000000000000000000000000000000000000000000000000000000000"
                                    : complete.sha256();

                            MessageEnvelope completeForward = new MessageEnvelope(
                                    ProtocolLimits.VERSION,
                                    MessageType.FILE_COMPLETE,
                                    UUID.randomUUID(),
                                    null,
                                    new MessageBody.FileComplete(complete.transferId(), complete.chunkCount(), hashToSend)
                            );
                            receiver.handleControl(completeForward);
                        }

                        // 2. Send ACK(FILE_COMPLETE) to sender
                        if (sender != null) {
                            MessageEnvelope ackComplete = new MessageEnvelope(
                                    ProtocolLimits.VERSION,
                                    MessageType.ACK,
                                    UUID.randomUUID(),
                                    envelope.messageId(),
                                    new MessageBody.Ack(MessageType.FILE_COMPLETE, AckStatus.ACCEPTED, complete.transferId())
                            );
                            sender.handleControl(ackComplete);
                        }
                    }
                    case FILE_CANCEL -> {
                        MessageBody.TransferReference ref = (MessageBody.TransferReference) envelope.body();
                        if (receiver != null) {
                            MessageEnvelope status = new MessageEnvelope(
                                    ProtocolLimits.VERSION,
                                    MessageType.FILE_STATUS,
                                    UUID.randomUUID(),
                                    null,
                                    new MessageBody.FileStatus(ref.transferId(), FileStatusState.CANCELLED, "Sender cancelled")
                            );
                            receiver.handleControl(status);
                        }
                    }
                    default -> {}
                }
            } else if (frame instanceof FileChunk chunk) {
                if (dropChunks) {
                    return; // simulate network loss
                }

                // 1. Forward chunk to receiver with fresh UUID first
                if (receiver != null) {
                    FileChunk forwardedChunk = new FileChunk(UUID.randomUUID(), chunk.transferId(), chunk.sequence(), chunk.data());
                    receiver.handleChunk(forwardedChunk);

                    if (duplicateFirstChunk && chunk.sequence() == 0 && firstChunkSeen.compareAndSet(false, true)) {
                        // Inject duplicate chunk
                        receiver.handleChunk(forwardedChunk);
                    }
                }

                // 2. Send ACK(FILE_CHUNK, STORED) to sender
                if (sender != null) {
                    MessageEnvelope ackChunk = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.ACK,
                            UUID.randomUUID(),
                            chunk.messageId(),
                            new MessageBody.Ack(MessageType.FILE_CHUNK, AckStatus.STORED, chunk.transferId())
                    );
                    sender.handleControl(ackChunk);
                }
            }
        }

        synchronized void onReceiverOutbound(OutboundFrame frame) {
            if (frame instanceof MessageEnvelope envelope) {
                switch (envelope.type()) {
                    case FILE_ACCEPT -> {
                        MessageBody.TransferReference ref = (MessageBody.TransferReference) envelope.body();
                        if (sender != null) {
                            MessageEnvelope status = new MessageEnvelope(
                                    ProtocolLimits.VERSION,
                                    MessageType.FILE_STATUS,
                                    UUID.randomUUID(),
                                    null,
                                    new MessageBody.FileStatus(ref.transferId(), FileStatusState.ACCEPTED, null)
                            );
                            sender.handleControl(status);
                        }
                    }
                    case FILE_REJECT -> {
                        MessageBody.TransferReference ref = (MessageBody.TransferReference) envelope.body();
                        if (sender != null) {
                            MessageEnvelope status = new MessageEnvelope(
                                    ProtocolLimits.VERSION,
                                    MessageType.FILE_STATUS,
                                    UUID.randomUUID(),
                                    null,
                                    new MessageBody.FileStatus(ref.transferId(), FileStatusState.REJECTED, "Receiver rejected")
                            );
                            sender.handleControl(status);
                        }
                    }
                    case FILE_RECEIVED -> {
                        MessageBody.FileReceived received = (MessageBody.FileReceived) envelope.body();
                        if (receiver != null) {
                            MessageEnvelope ackReceived = new MessageEnvelope(
                                    ProtocolLimits.VERSION,
                                    MessageType.ACK,
                                    UUID.randomUUID(),
                                    envelope.messageId(),
                                    new MessageBody.Ack(MessageType.FILE_RECEIVED, AckStatus.ACCEPTED, received.transferId())
                            );
                            receiver.handleControl(ackReceived);
                        }

                        if (sender != null) {
                            MessageEnvelope status = new MessageEnvelope(
                                    ProtocolLimits.VERSION,
                                    MessageType.FILE_STATUS,
                                    UUID.randomUUID(),
                                    null,
                                    new MessageBody.FileStatus(received.transferId(), FileStatusState.COMPLETED, null)
                            );
                            sender.handleControl(status);
                        }
                    }
                    case FILE_CANCEL -> {
                        MessageBody.TransferReference ref = (MessageBody.TransferReference) envelope.body();
                        if (sender != null) {
                            MessageEnvelope status = new MessageEnvelope(
                                    ProtocolLimits.VERSION,
                                    MessageType.FILE_STATUS,
                                    UUID.randomUUID(),
                                    null,
                                    new MessageBody.FileStatus(ref.transferId(), FileStatusState.CANCELLED, "Receiver cancelled")
                            );
                            sender.handleControl(status);
                        }
                    }
                    default -> {}
                }
            }
        }
    }

    private Path createTestFile(Path dir, String name, int sizeBytes) throws IOException {
        Path file = dir.resolve(name);
        byte[] content = new byte[sizeBytes];
        for (int i = 0; i < sizeBytes; i++) {
            content[i] = (byte) ((i % 251) + 1);
        }
        Files.write(file, content);
        return file;
    }

    @Test
    void test1_successfulSmallFileTransfer() throws Exception {
        SimulatedServerRelay relay = new SimulatedServerRelay();
        FileSender sender = new FileSender(relay::onSenderOutbound, workerPool);
        FileReceiver receiver = new FileReceiver(relay::onReceiverOutbound, receiverStorage, workerPool);
        relay.setSender(sender);
        relay.setReceiver(receiver);

        Path source = createTestFile(senderDir, "small.txt", 512);
        CompletableFuture<FileTransferResult> senderFuture = sender.startTransfer(relay.recipientClientId, source);

        FileTransferResult senderResult = senderFuture.get(5, TimeUnit.SECONDS);
        assertTrue(senderResult.success());
        assertEquals(FileSender.SenderState.COMPLETED, sender.state());
        assertEquals(FileReceiver.ReceiverState.COMPLETED, receiver.state());

        assertNotNull(receiver.finalFile());
        assertTrue(Files.exists(receiver.finalFile()));
        assertEquals(512, Files.size(receiver.finalFile()));
        assertArrayEquals(Files.readAllBytes(source), Files.readAllBytes(receiver.finalFile()));
    }

    @Test
    void test2_successfulMultiChunkTransfer() throws Exception {
        SimulatedServerRelay relay = new SimulatedServerRelay();
        FileSender sender = new FileSender(relay::onSenderOutbound, workerPool);
        FileReceiver receiver = new FileReceiver(relay::onReceiverOutbound, receiverStorage, workerPool);
        relay.setSender(sender);
        relay.setReceiver(receiver);

        // 50 KiB file = 3 full chunks (16 KiB) + 1 final partial chunk (2 KiB)
        int size = (3 * ProtocolLimits.MAX_CHUNK_BYTES) + 2048;
        Path source = createTestFile(senderDir, "multi.dat", size);

        CompletableFuture<FileTransferResult> senderFuture = sender.startTransfer(relay.recipientClientId, source);
        FileTransferResult senderResult = senderFuture.get(5, TimeUnit.SECONDS);

        assertTrue(senderResult.success());
        assertEquals(FileSender.SenderState.COMPLETED, sender.state());
        assertEquals(FileReceiver.ReceiverState.COMPLETED, receiver.state());

        assertNotNull(receiver.finalFile());
        assertEquals(size, Files.size(receiver.finalFile()));
        assertArrayEquals(Files.readAllBytes(source), Files.readAllBytes(receiver.finalFile()));
        assertEquals(TransferChecksum.computeFileDigestHex(source), TransferChecksum.computeFileDigestHex(receiver.finalFile()));
    }

    @Test
    void test3_emptyFileTransfer() throws Exception {
        SimulatedServerRelay relay = new SimulatedServerRelay();
        FileSender sender = new FileSender(relay::onSenderOutbound, workerPool);
        FileReceiver receiver = new FileReceiver(relay::onReceiverOutbound, receiverStorage, workerPool);
        relay.setSender(sender);
        relay.setReceiver(receiver);

        Path source = createTestFile(senderDir, "empty.txt", 0);
        CompletableFuture<FileTransferResult> senderFuture = sender.startTransfer(relay.recipientClientId, source);
        FileTransferResult senderResult = senderFuture.get(5, TimeUnit.SECONDS);

        assertTrue(senderResult.success());
        assertEquals(FileSender.SenderState.COMPLETED, sender.state());
        assertEquals(FileReceiver.ReceiverState.COMPLETED, receiver.state());

        assertNotNull(receiver.finalFile());
        assertEquals(0, Files.size(receiver.finalFile()));
        assertEquals(TransferChecksum.EMPTY_SHA256, senderResult.sha256());
    }

    @Test
    void test4_exactly16KbFileTransfer() throws Exception {
        SimulatedServerRelay relay = new SimulatedServerRelay();
        FileSender sender = new FileSender(relay::onSenderOutbound, workerPool);
        FileReceiver receiver = new FileReceiver(relay::onReceiverOutbound, receiverStorage, workerPool);
        relay.setSender(sender);
        relay.setReceiver(receiver);

        int size = ProtocolLimits.MAX_CHUNK_BYTES; // exactly 16,384 bytes
        Path source = createTestFile(senderDir, "exact16k.dat", size);

        CompletableFuture<FileTransferResult> senderFuture = sender.startTransfer(relay.recipientClientId, source);
        FileTransferResult senderResult = senderFuture.get(5, TimeUnit.SECONDS);

        assertTrue(senderResult.success());
        assertEquals(FileSender.SenderState.COMPLETED, sender.state());
        assertEquals(FileReceiver.ReceiverState.COMPLETED, receiver.state());
        assertEquals(size, Files.size(receiver.finalFile()));
    }

    @Test
    void test5_fileLargerThanOneChunk() throws Exception {
        SimulatedServerRelay relay = new SimulatedServerRelay();
        FileSender sender = new FileSender(relay::onSenderOutbound, workerPool);
        FileReceiver receiver = new FileReceiver(relay::onReceiverOutbound, receiverStorage, workerPool);
        relay.setSender(sender);
        relay.setReceiver(receiver);

        // 16 KiB + 1 byte
        int size = ProtocolLimits.MAX_CHUNK_BYTES + 1;
        Path source = createTestFile(senderDir, "oneChunkPlusOne.bin", size);

        CompletableFuture<FileTransferResult> senderFuture = sender.startTransfer(relay.recipientClientId, source);
        FileTransferResult senderResult = senderFuture.get(5, TimeUnit.SECONDS);

        assertTrue(senderResult.success());
        assertEquals(size, Files.size(receiver.finalFile()));
        assertArrayEquals(Files.readAllBytes(source), Files.readAllBytes(receiver.finalFile()));
    }

    @Test
    void test6_invalidChunkSequence() throws Exception {
        SimulatedServerRelay relay = new SimulatedServerRelay();
        FileReceiver receiver = new FileReceiver(relay::onReceiverOutbound, receiverStorage, workerPool);
        relay.setReceiver(receiver);

        UUID transferId = UUID.randomUUID();
        // Offer a 32 KiB file
        MessageEnvelope offered = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(transferId, UUID.randomUUID(), "test.dat", 32768)
        );
        receiver.handleControl(offered);
        CompletableFuture<FileTransferResult> receiverFuture = receiver.accept(transferId);

        // Send sequence 1 instead of expected 0 (out of order!)
        byte[] chunkData = new byte[ProtocolLimits.MAX_CHUNK_BYTES];
        FileChunk invalidSequenceChunk = new FileChunk(UUID.randomUUID(), transferId, 1, chunkData);
        receiver.handleChunk(invalidSequenceChunk);

        FileTransferResult result = receiverFuture.get(3, TimeUnit.SECONDS);
        assertFalse(result.success());
        assertEquals(FileReceiver.ReceiverState.FAILED, receiver.state());
        awaitStorageDeleted(receiverStorage, transferId); // cleaned up
    }

    @Test
    void test7_duplicateChunkIsSilentlyIgnoredWithoutCorruptingTransfer() throws Exception {
        SimulatedServerRelay relay = new SimulatedServerRelay();
        relay.duplicateFirstChunk = true; // injects duplicate chunk 0

        FileSender sender = new FileSender(relay::onSenderOutbound, workerPool);
        FileReceiver receiver = new FileReceiver(relay::onReceiverOutbound, receiverStorage, workerPool);
        relay.setSender(sender);
        relay.setReceiver(receiver);

        int size = ProtocolLimits.MAX_CHUNK_BYTES + 4096;
        Path source = createTestFile(senderDir, "duplicate_test.dat", size);

        CompletableFuture<FileTransferResult> senderFuture = sender.startTransfer(relay.recipientClientId, source);
        FileTransferResult senderResult = senderFuture.get(5, TimeUnit.SECONDS);

        assertTrue(senderResult.success());
        assertEquals(FileReceiver.ReceiverState.COMPLETED, receiver.state());
        assertEquals(size, Files.size(receiver.finalFile()));
        assertArrayEquals(Files.readAllBytes(source), Files.readAllBytes(receiver.finalFile()));
    }

    @Test
    void test8_checksumMismatchAbortsAndDeletesPartialFile() throws Exception {
        SimulatedServerRelay relay = new SimulatedServerRelay();
        relay.corruptChecksum = true; // injects bad SHA-256 in FILE_COMPLETE

        FileSender sender = new FileSender(relay::onSenderOutbound, workerPool);
        FileReceiver receiver = new FileReceiver(relay::onReceiverOutbound, receiverStorage, workerPool);
        relay.setSender(sender);
        relay.setReceiver(receiver);

        Path source = createTestFile(senderDir, "corrupt.dat", 4096);
        CompletableFuture<FileTransferResult> senderFuture = sender.startTransfer(relay.recipientClientId, source);

        FileTransferResult senderResult = senderFuture.get(5, TimeUnit.SECONDS);
        assertFalse(senderResult.success());
        assertEquals(FileReceiver.ReceiverState.FAILED, receiver.state());
        assertEquals(FileSender.SenderState.CANCELLED, sender.state());

        // Partial file must be deleted on checksum failure
        awaitStorageDeleted(receiverStorage, sender.transferId());
    }

    @Test
    void test9_interruptedTransferCleansUpPartialFiles() throws Exception {
        SimulatedServerRelay relay = new SimulatedServerRelay();
        FileSender sender = new FileSender(relay::onSenderOutbound, workerPool);
        FileReceiver receiver = new FileReceiver(relay::onReceiverOutbound, receiverStorage, workerPool);
        relay.setSender(sender);
        relay.setReceiver(receiver);

        Path source = createTestFile(senderDir, "interrupt.dat", 32768);
        CompletableFuture<FileTransferResult> senderFuture = sender.startTransfer(relay.recipientClientId, source);

        // Cancel receiver directly while transfer is in progress
        receiver.cancel();

        FileTransferResult senderResult = senderFuture.get(3, TimeUnit.SECONDS);
        assertFalse(senderResult.success());
        assertEquals(FileReceiver.ReceiverState.CANCELLED, receiver.state());

        // Partial file must be deleted
        awaitStorageDeleted(receiverStorage, sender.transferId());
    }

    @Test
    void test10_fileExceeding10MbLimitIsRejected() throws Exception {
        SimulatedServerRelay relay = new SimulatedServerRelay();
        FileSender sender = new FileSender(relay::onSenderOutbound, workerPool);
        relay.setSender(sender);

        // 10 MiB + 1 byte
        long oversized = ProtocolLimits.MAX_FILE_BYTES + 1;
        Path dummyOversized = senderDir.resolve("oversized.dat");
        try (var channel = java.nio.channels.FileChannel.open(
                dummyOversized,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.WRITE)) {
            channel.position(oversized - 1);
            channel.write(java.nio.ByteBuffer.wrap(new byte[]{1}));
        }

        assertThrows(IllegalArgumentException.class, () ->
                sender.startTransfer(relay.recipientClientId, dummyOversized));
    }

    @Test
    void test11_rejectedAndCancelledTransfers() throws Exception {
        SimulatedServerRelay relay = new SimulatedServerRelay();
        relay.autoAccept = false; // do not accept

        FileSender sender = new FileSender(relay::onSenderOutbound, workerPool);
        FileReceiver receiver = new FileReceiver(relay::onReceiverOutbound, receiverStorage, workerPool);
        relay.setSender(sender);
        relay.setReceiver(receiver);

        Path source = createTestFile(senderDir, "reject_me.txt", 1024);
        CompletableFuture<FileTransferResult> senderFuture = sender.startTransfer(relay.recipientClientId, source);

        // Wait until receiver receives offer
        for (int i = 0; i < 20 && receiver.state() != FileReceiver.ReceiverState.OFFERED; i++) {
            Thread.sleep(20);
        }
        assertEquals(FileReceiver.ReceiverState.OFFERED, receiver.state());

        // Explicitly reject
        receiver.reject(receiver.transferId());

        FileTransferResult result = senderFuture.get(3, TimeUnit.SECONDS);
        assertFalse(result.success());
        assertEquals(FileSender.SenderState.REJECTED, sender.state());
        assertEquals(FileReceiver.ReceiverState.REJECTED, receiver.state());
    }

    @Test
    void test12_concurrentTransfersWithinConfiguredLimits() throws Exception {
        // Two independent sender-receiver pairs
        SimulatedServerRelay relay1 = new SimulatedServerRelay();
        SimulatedServerRelay relay2 = new SimulatedServerRelay();

        Path receiver1Dir = tempDir.resolve("receiver1");
        Path receiver2Dir = tempDir.resolve("receiver2");
        Files.createDirectories(receiver1Dir);
        Files.createDirectories(receiver2Dir);

        FileSender sender1 = new FileSender(relay1::onSenderOutbound, workerPool);
        FileReceiver receiver1 = new FileReceiver(relay1::onReceiverOutbound, new SafeFileStorage(receiver1Dir), workerPool);
        relay1.setSender(sender1);
        relay1.setReceiver(receiver1);

        FileSender sender2 = new FileSender(relay2::onSenderOutbound, workerPool);
        FileReceiver receiver2 = new FileReceiver(relay2::onReceiverOutbound, new SafeFileStorage(receiver2Dir), workerPool);
        relay2.setSender(sender2);
        relay2.setReceiver(receiver2);

        Path source1 = createTestFile(senderDir, "concurrent1.dat", 40000);
        Path source2 = createTestFile(senderDir, "concurrent2.dat", 60000);

        CompletableFuture<FileTransferResult> future1 = sender1.startTransfer(relay1.recipientClientId, source1);
        CompletableFuture<FileTransferResult> future2 = sender2.startTransfer(relay2.recipientClientId, source2);

        CompletableFuture.allOf(future1, future2).get(10, TimeUnit.SECONDS);

        assertTrue(future1.get().success());
        assertTrue(future2.get().success());

        assertEquals(40000, Files.size(receiver1.finalFile()));
        assertEquals(60000, Files.size(receiver2.finalFile()));
        assertArrayEquals(Files.readAllBytes(source1), Files.readAllBytes(receiver1.finalFile()));
        assertArrayEquals(Files.readAllBytes(source2), Files.readAllBytes(receiver2.finalFile()));
    }
}
