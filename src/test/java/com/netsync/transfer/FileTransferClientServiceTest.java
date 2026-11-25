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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit and integration tests for {@link FileTransferClientService}.
 */
@Timeout(15)
class FileTransferClientServiceTest {

    @TempDir
    Path tempDir;

    private Path client1Downloads;
    private Path client2Downloads;
    private Path senderSourceDir;
    private TransferWorkerPool sharedWorkerPool;

    @BeforeEach
    void setUp() throws IOException {
        client1Downloads = tempDir.resolve("downloads_client1");
        client2Downloads = tempDir.resolve("downloads_client2");
        senderSourceDir = tempDir.resolve("sender_source");

        Files.createDirectories(client1Downloads);
        Files.createDirectories(client2Downloads);
        Files.createDirectories(senderSourceDir);

        sharedWorkerPool = new TransferWorkerPool(4, 64);
    }

    @AfterEach
    void tearDown() {
        if (sharedWorkerPool != null && !sharedWorkerPool.isShutdown()) {
            sharedWorkerPool.close();
        }
    }

    private Path createTestFile(String name, int sizeBytes) throws IOException {
        Path file = senderSourceDir.resolve(name);
        byte[] content = new byte[sizeBytes];
        for (int i = 0; i < sizeBytes; i++) {
            content[i] = (byte) ((i % 251) + 1);
        }
        Files.write(file, content);
        return file;
    }

    /**
     * Capturing sink that accumulates outgoing frames and allows thread-safe inspection.
     */
    private static final class CapturingSink implements java.util.function.Consumer<OutboundFrame> {
        private final BlockingQueue<OutboundFrame> frames = new LinkedBlockingQueue<>();

        @Override
        public void accept(OutboundFrame frame) {
            frames.add(frame);
        }

        OutboundFrame takeFrame(long timeoutSeconds) throws InterruptedException {
            OutboundFrame frame = frames.poll(timeoutSeconds, TimeUnit.SECONDS);
            assertNotNull(frame, "Expected outbound frame was not emitted within timeout");
            return frame;
        }

        List<OutboundFrame> drainFrames() {
            List<OutboundFrame> drained = new ArrayList<>();
            frames.drainTo(drained);
            return drained;
        }
    }

    /**
     * Test listener recording all callback invocations.
     */
    private static final class TestEventListener implements FileTransferClientService.FileTransferEventListener {
        final List<UUID> offersReceived = new CopyOnWriteArrayList<>();
        final List<FileTransferProgress> progressUpdates = new CopyOnWriteArrayList<>();
        final List<FileTransferResult> completions = new CopyOnWriteArrayList<>();
        final List<String> failures = new CopyOnWriteArrayList<>();
        final List<String> cancellations = new CopyOnWriteArrayList<>();
        final List<String> rejections = new CopyOnWriteArrayList<>();

        @Override
        public void onOfferReceived(UUID transferId, UUID senderClientId, String fileName, long sizeBytes) {
            offersReceived.add(transferId);
        }

        @Override
        public void onProgress(FileTransferProgress progress) {
            progressUpdates.add(progress);
        }

        @Override
        public void onCompleted(FileTransferResult result) {
            completions.add(result);
        }

        @Override
        public void onFailed(UUID transferId, String reason) {
            failures.add(reason);
        }

        @Override
        public void onCancelled(UUID transferId, String reason) {
            cancellations.add(reason);
        }

        @Override
        public void onRejected(UUID transferId, String reason) {
            rejections.add(reason);
        }
    }

    @Test
    void test1_successfulOutgoingTransfer() throws Exception {
        CapturingSink sink = new CapturingSink();
        TestEventListener listener = new TestEventListener();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);
        service.setEventListener(listener);

        UUID recipientId = UUID.randomUUID();
        Path source = createTestFile("outbound.txt", 1024);

        CompletableFuture<FileTransferResult> future = service.sendFile(recipientId, source);
        assertTrue(service.isTransferActive());
        assertEquals(Optional.of(FileSender.SenderState.OFFERING), service.senderState());

        // 1. Service emits FILE_OFFER
        OutboundFrame frame1 = sink.takeFrame(3);
        assertInstanceOf(MessageEnvelope.class, frame1);
        MessageEnvelope offerEnv = (MessageEnvelope) frame1;
        assertEquals(MessageType.FILE_OFFER, offerEnv.type());

        UUID transferId = UUID.randomUUID();

        // 2. Server replies ACK(FILE_OFFER)
        MessageEnvelope ackOffer = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ACK,
                UUID.randomUUID(),
                offerEnv.messageId(),
                new MessageBody.Ack(MessageType.FILE_OFFER, AckStatus.ACCEPTED, transferId)
        );
        service.handleControl(ackOffer);
        assertEquals(Optional.of(FileSender.SenderState.AWAITING_ACCEPTANCE), service.senderState());

        // 3. Server sends FILE_STATUS(ACCEPTED)
        MessageEnvelope statusAccepted = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_STATUS,
                UUID.randomUUID(),
                null,
                new MessageBody.FileStatus(transferId, FileStatusState.ACCEPTED, null)
        );
        service.handleControl(statusAccepted);

        // 4. Sender reads chunk 0 and emits FileChunk
        OutboundFrame frame2 = sink.takeFrame(3);
        assertInstanceOf(FileChunk.class, frame2);
        FileChunk chunk = (FileChunk) frame2;
        assertEquals(0, chunk.sequence());
        assertEquals(1024, chunk.data().length);

        // 5. Server sends ACK(FILE_CHUNK, STORED)
        MessageEnvelope ackChunk = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ACK,
                UUID.randomUUID(),
                chunk.messageId(),
                new MessageBody.Ack(MessageType.FILE_CHUNK, AckStatus.STORED, transferId)
        );
        service.handleControl(ackChunk);

        // 6. Sender finishes and emits FILE_COMPLETE
        OutboundFrame frame3 = sink.takeFrame(3);
        assertInstanceOf(MessageEnvelope.class, frame3);
        MessageEnvelope completeEnv = (MessageEnvelope) frame3;
        assertEquals(MessageType.FILE_COMPLETE, completeEnv.type());

        // 7. Server sends ACK(FILE_COMPLETE)
        MessageEnvelope ackComplete = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ACK,
                UUID.randomUUID(),
                completeEnv.messageId(),
                new MessageBody.Ack(MessageType.FILE_COMPLETE, AckStatus.ACCEPTED, transferId)
        );
        service.handleControl(ackComplete);

        // 8. Server sends FILE_STATUS(COMPLETED)
        MessageEnvelope statusComplete = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_STATUS,
                UUID.randomUUID(),
                null,
                new MessageBody.FileStatus(transferId, FileStatusState.COMPLETED, null)
        );
        service.handleControl(statusComplete);

        FileTransferResult result = future.get(5, TimeUnit.SECONDS);
        assertTrue(result.success());
        assertEquals(1024, result.bytesTransferred());
        assertFalse(service.isTransferActive());
        assertEquals(1, listener.completions.size());
    }

    @Test
    void test2_concurrentTransferRequestRejection() throws Exception {
        CapturingSink sink = new CapturingSink();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);

        UUID recipientId = UUID.randomUUID();
        Path source = createTestFile("file1.dat", 512);

        service.sendFile(recipientId, source);
        assertTrue(service.isTransferActive());

        // Second upload attempt must fail immediately
        Path source2 = createTestFile("file2.dat", 512);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                service.sendFile(recipientId, source2));
        assertTrue(ex.getMessage().contains("already active"));

        // Attempting to accept a fake offer while active must fail
        assertThrows(IllegalStateException.class, () ->
                service.acceptOffer(UUID.randomUUID()));
    }

    @Test
    void test3_incomingOfferNotification() throws Exception {
        CapturingSink sink = new CapturingSink();
        TestEventListener listener = new TestEventListener();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);
        service.setEventListener(listener);

        UUID transferId = UUID.randomUUID();
        UUID senderClientId = UUID.randomUUID();
        MessageEnvelope offerEnv = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(transferId, senderClientId, "photo.png", 2048)
        );

        service.handleControl(offerEnv);

        assertTrue(service.isTransferActive());
        assertEquals(Optional.of(transferId), service.activeTransferId());
        assertEquals(Optional.of(FileReceiver.ReceiverState.OFFERED), service.receiverState());
        assertEquals(1, listener.offersReceived.size());
        assertEquals(transferId, listener.offersReceived.getFirst());
    }

    @Test
    void test4_incomingOfferRejectionWhenBusy() throws Exception {
        CapturingSink sink = new CapturingSink();
        TestEventListener listener = new TestEventListener();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);
        service.setEventListener(listener);

        // Put service into active upload state
        Path source = createTestFile("busy.dat", 1024);
        service.sendFile(UUID.randomUUID(), source);
        assertTrue(service.isTransferActive());
        sink.drainFrames(); // clear outgoing offer

        // Now an incoming offer arrives
        UUID newTransferId = UUID.randomUUID();
        MessageEnvelope offerEnv = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(newTransferId, UUID.randomUUID(), "incoming.dat", 4096)
        );

        service.handleControl(offerEnv);

        // Service must automatically emit FILE_REJECT for the new offer
        OutboundFrame frame = sink.takeFrame(3);
        assertInstanceOf(MessageEnvelope.class, frame);
        MessageEnvelope rejectEnv = (MessageEnvelope) frame;
        assertEquals(MessageType.FILE_REJECT, rejectEnv.type());
        MessageBody.TransferReference ref = (MessageBody.TransferReference) rejectEnv.body();
        assertEquals(newTransferId, ref.transferId());

        // Rejection must be reported to listener
        assertEquals(1, listener.rejections.size());
        // Original sender transfer remains active
        assertTrue(service.isTransferActive());
    }

    @Test
    void test5_acceptIncomingOfferAndComplete() throws Exception {
        CapturingSink sink = new CapturingSink();
        TestEventListener listener = new TestEventListener();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);
        service.setEventListener(listener);

        UUID transferId = UUID.randomUUID();
        int size = 512;
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) data[i] = (byte) (i + 1);
        String expectedHash = TransferChecksum.computeDigestHex(data);

        // 1. Server sends FILE_OFFERED
        MessageEnvelope offerEnv = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(transferId, UUID.randomUUID(), "received.bin", size)
        );
        service.handleControl(offerEnv);

        // 2. User accepts offer
        CompletableFuture<FileTransferResult> future = service.acceptOffer(transferId);
        assertEquals(Optional.of(FileReceiver.ReceiverState.RECEIVING), service.receiverState());

        // 3. Outbound FILE_ACCEPT emitted
        OutboundFrame acceptFrame = sink.takeFrame(3);
        assertInstanceOf(MessageEnvelope.class, acceptFrame);
        assertEquals(MessageType.FILE_ACCEPT, ((MessageEnvelope) acceptFrame).type());

        // 4. Server forwards binary FileChunk
        FileChunk chunk = new FileChunk(UUID.randomUUID(), transferId, 0, data);
        service.handleChunk(chunk);

        // 5. Server forwards FILE_COMPLETE
        MessageEnvelope completeEnv = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_COMPLETE,
                UUID.randomUUID(),
                null,
                new MessageBody.FileComplete(transferId, 1, expectedHash)
        );
        service.handleControl(completeEnv);

        // 6. Receiver verifies and emits FILE_RECEIVED
        OutboundFrame receivedFrame = sink.takeFrame(3);
        assertInstanceOf(MessageEnvelope.class, receivedFrame);
        MessageEnvelope receivedEnv = (MessageEnvelope) receivedFrame;
        assertEquals(MessageType.FILE_RECEIVED, receivedEnv.type());

        // 7. Server acknowledges FILE_RECEIVED
        MessageEnvelope ackReceived = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ACK,
                UUID.randomUUID(),
                receivedEnv.messageId(),
                new MessageBody.Ack(MessageType.FILE_RECEIVED, AckStatus.ACCEPTED, transferId)
        );
        service.handleControl(ackReceived);

        FileTransferResult result = future.get(5, TimeUnit.SECONDS);
        assertTrue(result.success());
        assertEquals(size, result.bytesTransferred());
        assertNotNull(result.localFile());
        assertTrue(Files.exists(result.localFile()));
        assertArrayEquals(data, Files.readAllBytes(result.localFile()));
        assertFalse(service.isTransferActive());
        assertEquals(1, listener.completions.size());
    }

    @Test
    void test6_explicitOfferRejection() throws Exception {
        CapturingSink sink = new CapturingSink();
        TestEventListener listener = new TestEventListener();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);
        service.setEventListener(listener);

        UUID transferId = UUID.randomUUID();
        MessageEnvelope offerEnv = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(transferId, UUID.randomUUID(), "unwanted.exe", 1024)
        );
        service.handleControl(offerEnv);
        assertEquals(Optional.of(FileReceiver.ReceiverState.OFFERED), service.receiverState());

        // Reject offer
        service.rejectOffer(transferId);

        OutboundFrame frame = sink.takeFrame(3);
        assertInstanceOf(MessageEnvelope.class, frame);
        assertEquals(MessageType.FILE_REJECT, ((MessageEnvelope) frame).type());

        assertFalse(service.isTransferActive());
        assertEquals(Optional.empty(), service.activeTransferId());
        assertEquals(1, listener.rejections.size());
    }

    @Test
    void test7_binaryChunkRouting() throws Exception {
        CapturingSink sink = new CapturingSink();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);

        UUID transferId = UUID.randomUUID();
        MessageEnvelope offerEnv = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(transferId, UUID.randomUUID(), "chunk_test.dat", 2048)
        );
        service.handleControl(offerEnv);
        service.acceptOffer(transferId);
        OutboundFrame accepted = sink.takeFrame(3);
        assertEquals(MessageType.FILE_ACCEPT, ((MessageEnvelope) accepted).type());
        sink.drainFrames();

        // Send a chunk for a completely different transfer ID (should be dropped without corrupting state)
        UUID unrelatedId = UUID.randomUUID();
        FileChunk unrelatedChunk = new FileChunk(UUID.randomUUID(), unrelatedId, 0, new byte[512]);
        assertDoesNotThrow(() -> service.handleChunk(unrelatedChunk));

        // State remains RECEIVING for our transfer
        assertEquals(Optional.of(FileReceiver.ReceiverState.RECEIVING), service.receiverState());

        // Send valid chunk for our transfer
        FileChunk validChunk = new FileChunk(UUID.randomUUID(), transferId, 0, new byte[1024]);
        assertDoesNotThrow(() -> service.handleChunk(validChunk));
    }

    @Test
    void test8_cancellationAndPartialFileCleanup() throws Exception {
        CapturingSink sink = new CapturingSink();
        TestEventListener listener = new TestEventListener();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);
        service.setEventListener(listener);

        UUID transferId = UUID.randomUUID();
        int totalSize = ProtocolLimits.MAX_CHUNK_BYTES + 4096;
        MessageEnvelope offerEnv = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(transferId, UUID.randomUUID(), "cancel_me.dat", totalSize)
        );
        service.handleControl(offerEnv);
        service.acceptOffer(transferId);
        assertEquals(MessageType.FILE_ACCEPT, ((MessageEnvelope) sink.takeFrame(3)).type());

        // Write first non-final chunk (must be exactly MAX_CHUNK_BYTES per protocol)
        FileChunk chunk = new FileChunk(UUID.randomUUID(), transferId, 0, new byte[ProtocolLimits.MAX_CHUNK_BYTES]);
        service.handleChunk(chunk);

        Path partialFile = client1Downloads.resolve(transferId + ".part");
        await(() -> Files.exists(partialFile));

        // User cancels active transfer
        service.cancelActiveTransfer();

        OutboundFrame cancelFrame = sink.takeFrame(3);
        assertInstanceOf(MessageEnvelope.class, cancelFrame);
        assertEquals(MessageType.FILE_CANCEL, ((MessageEnvelope) cancelFrame).type());

        // Partial file must be deleted upon cancellation
        await(() -> !Files.exists(partialFile));
        assertFalse(service.isTransferActive());
    }

    @Test
    void test9_disconnectCleanup() throws Exception {
        CapturingSink sink = new CapturingSink();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);

        UUID transferId = UUID.randomUUID();
        int totalSize = ProtocolLimits.MAX_CHUNK_BYTES + 4096;
        MessageEnvelope offerEnv = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(transferId, UUID.randomUUID(), "disconnect.dat", totalSize)
        );
        service.handleControl(offerEnv);
        service.acceptOffer(transferId);

        Path partialFile = client1Downloads.resolve(transferId + ".part");
        FileChunk chunk = new FileChunk(UUID.randomUUID(), transferId, 0, new byte[ProtocolLimits.MAX_CHUNK_BYTES]);
        service.handleChunk(chunk);
        await(() -> Files.exists(partialFile));

        // TCP Disconnection triggers onDisconnect
        service.onDisconnect();

        assertFalse(service.isTransferActive());
        await(() -> !Files.exists(partialFile));
    }

    @Test
    void test10_workerPoolLifecycleAndIdempotentClose() {
        CapturingSink sink = new CapturingSink();

        // 1. External worker pool should NOT be closed when service is closed
        TransferWorkerPool externalPool = new TransferWorkerPool(2, 16);
        FileTransferClientService service1 = new FileTransferClientService(sink, () -> client1Downloads, externalPool);
        service1.close();
        assertTrue(service1.isClosed());
        assertFalse(externalPool.isShutdown(), "External worker pool must not be closed by service");
        externalPool.close();

        // 2. Internal worker pool should be closed when service is closed
        FileTransferClientService service2 = new FileTransferClientService(sink, () -> client1Downloads);
        service2.close();
        assertTrue(service2.isClosed());

        // Idempotent close
        assertDoesNotThrow(service2::close);

        // Operations after close must fail
        assertThrows(IllegalStateException.class, () ->
                service2.sendFile(UUID.randomUUID(), tempDir.resolve("nonexistent.txt")));
    }

    @Test
    void test11_endToEndTransferBetweenTwoServices() throws Exception {
        // Relay simulation between Client 1 (Sender) and Client 2 (Receiver)
        AtomicReference<FileTransferClientService> service1Ref = new AtomicReference<>();
        AtomicReference<FileTransferClientService> service2Ref = new AtomicReference<>();

        Consumer<OutboundFrame> sink1 = frame -> {
            FileTransferClientService s2 = service2Ref.get();
            FileTransferClientService s1 = service1Ref.get();
            if (frame instanceof MessageEnvelope env) {
                if (env.type() == MessageType.FILE_OFFER) {
                    MessageBody.FileOffer offer = (MessageBody.FileOffer) env.body();
                    UUID transferId = UUID.randomUUID();

                    // Send ACK(FILE_OFFER) to sender
                    MessageEnvelope ackOffer = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.ACK,
                            UUID.randomUUID(),
                            env.messageId(),
                            new MessageBody.Ack(MessageType.FILE_OFFER, AckStatus.ACCEPTED, transferId)
                    );
                    s1.handleControl(ackOffer);

                    // Forward FILE_OFFERED to receiver
                    MessageEnvelope offered = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_OFFERED,
                            UUID.randomUUID(),
                            null,
                            new MessageBody.FileOffered(transferId, UUID.randomUUID(), offer.fileName(), offer.sizeBytes())
                    );
                    s2.handleControl(offered);
                    // Automatically accept on receiver
                    s2.acceptOffer(transferId);
                } else if (env.type() == MessageType.FILE_COMPLETE) {
                    MessageBody.FileComplete complete = (MessageBody.FileComplete) env.body();
                    // Forward FILE_COMPLETE to receiver first
                    s2.handleControl(env);
                    // ACK FILE_COMPLETE to sender
                    MessageEnvelope ackComplete = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.ACK,
                            UUID.randomUUID(),
                            env.messageId(),
                            new MessageBody.Ack(MessageType.FILE_COMPLETE, AckStatus.ACCEPTED, complete.transferId())
                    );
                    s1.handleControl(ackComplete);
                }
            } else if (frame instanceof FileChunk chunk) {
                // Forward chunk to receiver
                s2.handleChunk(chunk);
                // Send ACK to sender
                MessageEnvelope ackChunk = new MessageEnvelope(
                        ProtocolLimits.VERSION,
                        MessageType.ACK,
                        UUID.randomUUID(),
                        chunk.messageId(),
                        new MessageBody.Ack(MessageType.FILE_CHUNK, AckStatus.STORED, chunk.transferId())
                );
                s1.handleControl(ackChunk);
            }
        };

        Consumer<OutboundFrame> sink2 = frame -> {
            FileTransferClientService s1 = service1Ref.get();
            FileTransferClientService s2 = service2Ref.get();
            if (frame instanceof MessageEnvelope env) {
                if (env.type() == MessageType.FILE_ACCEPT) {
                    MessageBody.TransferReference ref = (MessageBody.TransferReference) env.body();
                    MessageEnvelope statusAccepted = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_STATUS,
                            UUID.randomUUID(),
                            null,
                            new MessageBody.FileStatus(ref.transferId(), FileStatusState.ACCEPTED, null)
                    );
                    s1.handleControl(statusAccepted);
                } else if (env.type() == MessageType.FILE_RECEIVED) {
                    MessageBody.FileReceived received = (MessageBody.FileReceived) env.body();
                    // ACK to receiver
                    MessageEnvelope ackReceived = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.ACK,
                            UUID.randomUUID(),
                            env.messageId(),
                            new MessageBody.Ack(MessageType.FILE_RECEIVED, AckStatus.ACCEPTED, received.transferId())
                    );
                    s2.handleControl(ackReceived);
                    // Notify sender of completion
                    MessageEnvelope statusComplete = new MessageEnvelope(
                            ProtocolLimits.VERSION,
                            MessageType.FILE_STATUS,
                            UUID.randomUUID(),
                            null,
                            new MessageBody.FileStatus(received.transferId(), FileStatusState.COMPLETED, null)
                    );
                    s1.handleControl(statusComplete);
                }
            }
        };

        FileTransferClientService service1 = new FileTransferClientService(sink1, () -> client1Downloads, sharedWorkerPool);
        FileTransferClientService service2 = new FileTransferClientService(sink2, () -> client2Downloads, sharedWorkerPool);
        service1Ref.set(service1);
        service2Ref.set(service2);

        int fileSize = (2 * ProtocolLimits.MAX_CHUNK_BYTES) + 1024;
        Path source = createTestFile("end_to_end.dat", fileSize);

        CompletableFuture<FileTransferResult> sendFuture = service1.sendFile(UUID.randomUUID(), source);
        FileTransferResult sendResult = sendFuture.get(5, TimeUnit.SECONDS);

        assertTrue(sendResult.success());
        assertEquals(fileSize, sendResult.bytesTransferred());
        assertFalse(service1.isTransferActive());
        assertFalse(service2.isTransferActive());

        // Verify receiver downloaded file
        Path downloadedFile = client2Downloads.resolve(sendResult.transferId() + ".bin");
        assertTrue(Files.exists(downloadedFile));
        assertEquals(fileSize, Files.size(downloadedFile));
        assertArrayEquals(Files.readAllBytes(source), Files.readAllBytes(downloadedFile));
        assertEquals(TransferChecksum.computeFileDigestHex(source), TransferChecksum.computeFileDigestHex(downloadedFile));
    }

    @Test
    void test12_raceConditionBetweenSendAndIncomingOffer() throws Exception {
        CapturingSink sink = new CapturingSink();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);

        Path source = createTestFile("race.dat", 1024);
        UUID incomingTransferId = UUID.randomUUID();
        MessageEnvelope incomingOffer = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(incomingTransferId, UUID.randomUUID(), "incoming.dat", 2048)
        );

        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<Exception> sendException = new AtomicReference<>();
        AtomicBoolean sendStarted = new AtomicBoolean(false);

        Thread t1 = new Thread(() -> {
            try {
                barrier.await();
                service.sendFile(UUID.randomUUID(), source);
                sendStarted.set(true);
            } catch (Exception e) {
                sendException.set(e);
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                barrier.await();
                service.handleControl(incomingOffer);
            } catch (Exception ignored) {
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        // Invariant: Exactly one must be active
        assertTrue(service.isTransferActive(), "Exactly one transfer should have become active");
        if (sendStarted.get()) {
            assertEquals(Optional.of(FileSender.SenderState.OFFERING), service.senderState());
            assertNull(sendException.get());
        } else {
            assertNotNull(sendException.get());
            assertInstanceOf(IllegalStateException.class, sendException.get());
            assertEquals(Optional.of(FileReceiver.ReceiverState.OFFERED), service.receiverState());
            assertEquals(Optional.of(incomingTransferId), service.activeTransferId());
        }
    }

    @Test
    void test13_unrelatedAndUnknownTransferIdMessagesIgnored() throws Exception {
        CapturingSink sink = new CapturingSink();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);

        Path source = createTestFile("unrelated_test.dat", 512);
        service.sendFile(UUID.randomUUID(), source);

        UUID activeOfferMsgId = ((MessageEnvelope) sink.takeFrame(3)).messageId();
        UUID actualTransferId = UUID.randomUUID();

        // Transition sender to AWAITING_ACCEPTANCE
        service.handleControl(new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ACK,
                UUID.randomUUID(),
                activeOfferMsgId,
                new MessageBody.Ack(MessageType.FILE_OFFER, AckStatus.ACCEPTED, actualTransferId)
        ));
        assertEquals(Optional.of(FileSender.SenderState.AWAITING_ACCEPTANCE), service.senderState());

        // Inject messages with random unrelated transfer IDs
        UUID bogusId = UUID.randomUUID();
        service.handleControl(new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_STATUS,
                UUID.randomUUID(),
                null,
                new MessageBody.FileStatus(bogusId, FileStatusState.CANCELLED, "Bogus cancel")
        ));
        service.handleControl(new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_STATUS,
                UUID.randomUUID(),
                null,
                new MessageBody.FileStatus(bogusId, FileStatusState.COMPLETED, null)
        ));
        service.handleControl(new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_CANCEL,
                UUID.randomUUID(),
                null,
                new MessageBody.TransferReference(bogusId)
        ));
        service.handleControl(new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ERROR,
                UUID.randomUUID(),
                UUID.randomUUID(),
                new MessageBody.Error(ErrorCode.TRANSFER_FAILED, "Bogus error", bogusId)
        ));
        service.handleControl(new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ACK,
                UUID.randomUUID(),
                UUID.randomUUID(),
                new MessageBody.Ack(MessageType.FILE_CHUNK, AckStatus.STORED, bogusId)
        ));

        // Active transfer must remain completely unaffected
        assertEquals(Optional.of(FileSender.SenderState.AWAITING_ACCEPTANCE), service.senderState());
        assertTrue(service.isTransferActive());
        assertEquals(Optional.of(actualTransferId), service.activeTransferId());
    }

    @Test
    void test14_corruptedOrMalformedEnvelopesHandledGracefully() {
        CapturingSink sink = new CapturingSink();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);

        // Envelope with mismatched body type for FILE_OFFERED
        MessageEnvelope bogusOffer = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.TransferReference(UUID.randomUUID())
        );
        assertDoesNotThrow(() -> service.handleControl(bogusOffer));
        assertFalse(service.isTransferActive());

        // Envelope with non-transfer message type (e.g. CHAT_SEND)
        MessageEnvelope chatEnv = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.CHAT_SEND,
                UUID.randomUUID(),
                null,
                new MessageBody.ChatSend(UUID.randomUUID(), "Hello world")
        );
        assertDoesNotThrow(() -> service.handleControl(chatEnv));
        assertFalse(service.isTransferActive());
    }

    @Test
    void test15_frameSinkFailureDuringSendCleansUpState() throws Exception {
        Consumer<OutboundFrame> failingSink = frame -> {
            throw new RuntimeException("Simulated network sink failure");
        };
        FileTransferClientService service = new FileTransferClientService(failingSink, () -> client1Downloads, sharedWorkerPool);

        Path source = createTestFile("sink_fail.dat", 1024);
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                service.sendFile(UUID.randomUUID(), source));
        assertTrue(ex.getMessage().contains("Simulated network sink failure"));

        // Service must NOT have a stuck active transfer
        assertFalse(service.isTransferActive());
        assertEquals(Optional.empty(), service.activeTransferId());
        assertEquals(Optional.empty(), service.senderState());
    }

    @Test
    void test16_frameSinkFailureDuringAcceptOfferCleansUpState() {
        AtomicBoolean failSink = new AtomicBoolean(false);
        Consumer<OutboundFrame> conditionallyFailingSink = frame -> {
            if (failSink.get()) {
                throw new RuntimeException("Network pipe broken on accept");
            }
        };

        FileTransferClientService service = new FileTransferClientService(conditionallyFailingSink, () -> client1Downloads, sharedWorkerPool);
        UUID transferId = UUID.randomUUID();

        // Deliver incoming offer
        service.handleControl(new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(transferId, UUID.randomUUID(), "test.dat", 1024)
        ));
        assertTrue(service.isTransferActive());

        // Break sink, then accept
        failSink.set(true);
        assertDoesNotThrow(() -> service.acceptOffer(transferId));
        await(() -> !service.isTransferActive());

        // Service state must be cleaned up
        assertFalse(service.isTransferActive());
        assertEquals(Optional.empty(), service.receiverState());
    }

    @Test
    void test17_offerCancelledBySenderBeforeAcceptance() {
        CapturingSink sink = new CapturingSink();
        TestEventListener listener = new TestEventListener();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);
        service.setEventListener(listener);

        UUID transferId = UUID.randomUUID();
        MessageEnvelope offerEnv = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(transferId, UUID.randomUUID(), "cancelled_offer.dat", 2048)
        );
        service.handleControl(offerEnv);
        assertEquals(Optional.of(FileReceiver.ReceiverState.OFFERED), service.receiverState());

        // Server notifies FILE_STATUS(CANCELLED) before user accepts
        MessageEnvelope cancelStatus = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_STATUS,
                UUID.randomUUID(),
                null,
                new MessageBody.FileStatus(transferId, FileStatusState.CANCELLED, "Sender retracted offer")
        );
        service.handleControl(cancelStatus);

        // Service must clean up activeReceiver and notify listener
        assertFalse(service.isTransferActive());
        assertEquals(Optional.empty(), service.activeTransferId());
        assertEquals(1, listener.cancellations.size());
    }

    @Test
    void test18_cancelSpecificTransferIdMatchesAndMismatches() throws Exception {
        CapturingSink sink = new CapturingSink();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);

        Path source = createTestFile("cancel_specific.dat", 1024);
        CompletableFuture<FileTransferResult> future = service.sendFile(UUID.randomUUID(), source);
        assertTrue(service.isTransferActive());

        OutboundFrame offerFrame = sink.takeFrame(3);
        UUID actualTransferId = UUID.randomUUID();
        service.handleControl(new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ACK,
                UUID.randomUUID(),
                ((MessageEnvelope) offerFrame).messageId(),
                new MessageBody.Ack(MessageType.FILE_OFFER, AckStatus.ACCEPTED, actualTransferId)
        ));

        // Cancel with non-matching UUID has no effect
        UUID wrongId = UUID.randomUUID();
        service.cancelTransfer(wrongId);
        assertTrue(service.isTransferActive());

        // Cancel with matching UUID cancels the transfer
        service.cancelTransfer(actualTransferId);
        assertFalse(service.isTransferActive());
        FileTransferResult result = future.get(3, TimeUnit.SECONDS);
        assertFalse(result.success());
        assertTrue(result.message().toLowerCase().contains("cancel"));
    }

    @Test
    void test19_offerRejectedWhenDownloadDirSupplierReturnsNull() throws Exception {
        CapturingSink sink = new CapturingSink();
        TestEventListener listener = new TestEventListener();
        FileTransferClientService service = new FileTransferClientService(sink, () -> null, sharedWorkerPool);
        service.setEventListener(listener);

        UUID transferId = UUID.randomUUID();
        MessageEnvelope offerEnv = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.FILE_OFFERED,
                UUID.randomUUID(),
                null,
                new MessageBody.FileOffered(transferId, UUID.randomUUID(), "no_dir.dat", 1024)
        );
        service.handleControl(offerEnv);

        OutboundFrame rejectFrame = sink.takeFrame(3);
        assertInstanceOf(MessageEnvelope.class, rejectFrame);
        assertEquals(MessageType.FILE_REJECT, ((MessageEnvelope) rejectFrame).type());
        assertFalse(service.isTransferActive());
        assertEquals(1, listener.rejections.size());
    }

    @Test
    void existingFinalDestinationIsPreservedWhenTransferFinalizationCollides() throws Exception {
        CapturingSink sink = new CapturingSink();
        FileTransferClientService service = new FileTransferClientService(sink, () -> client1Downloads, sharedWorkerPool);
        UUID transferId = UUID.randomUUID();
        byte[] payload = "replacement must not happen".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Path destination = client1Downloads.resolve(transferId + ".bin");
        byte[] sentinel = new byte[]{91, 92, 93, 94};
        Files.write(destination, sentinel);
        service.handleControl(new MessageEnvelope(1, MessageType.FILE_OFFERED, UUID.randomUUID(), null,
                new MessageBody.FileOffered(transferId, UUID.randomUUID(), "collision.bin", payload.length)));
        service.acceptOffer(transferId);
        assertEquals(MessageType.FILE_ACCEPT, ((MessageEnvelope) sink.takeFrame(3)).type());
        service.handleChunk(new FileChunk(UUID.randomUUID(), transferId, 0, payload));
        service.handleControl(new MessageEnvelope(1, MessageType.FILE_COMPLETE, UUID.randomUUID(), null,
                new MessageBody.FileComplete(transferId, 1, TransferChecksum.computeDigestHex(payload))));

        await(() -> service.receiverState().isEmpty());
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
        await(() -> !Files.exists(client1Downloads.resolve(transferId + ".part")));
    }

    @Test
    void blockedReceiverDiskWriteDoesNotBlockSerialControlCallbacks() throws Exception {
        CountDownLatch writeEntered = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        SafeFileStorage blockingStorage = new SafeFileStorage(client1Downloads) {
            @Override public void appendChunk(UUID id, byte[] data, int offset, int length) throws IOException {
                writeEntered.countDown();
                try {
                    if (!releaseWrite.await(5, TimeUnit.SECONDS)) throw new IOException("controlled write timed out");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt(); throw new IOException(interrupted);
                }
                super.appendChunk(id, data, offset, length);
            }
        };
        CapturingSink sink = new CapturingSink();
        FileReceiver receiver = new FileReceiver(sink, blockingStorage, sharedWorkerPool);
        UUID transferId = UUID.randomUUID();
        receiver.handleControl(new MessageEnvelope(1, MessageType.FILE_OFFERED, UUID.randomUUID(), null,
                new MessageBody.FileOffered(transferId, UUID.randomUUID(), "slow.bin", 100)));
        receiver.accept(transferId);
        assertEquals(MessageType.FILE_ACCEPT, ((MessageEnvelope) sink.takeFrame(3)).type());
        receiver.handleChunk(new FileChunk(UUID.randomUUID(), transferId, 0, new byte[100]));
        assertTrue(writeEntered.await(2, TimeUnit.SECONDS));

        CompletableFuture<Void> nextControl = CompletableFuture.runAsync(() -> receiver.handleControl(
                new MessageEnvelope(1, MessageType.ERROR, UUID.randomUUID(), null,
                        new MessageBody.Error(ErrorCode.INVALID_REQUEST, "unrelated", null))));
        assertDoesNotThrow(() -> nextControl.get(500, TimeUnit.MILLISECONDS),
                "A blocked file write must not hold the serial control callback");
        receiver.cancel();
        releaseWrite.countDown();
        receiver.cleanupCompletion().get(3, TimeUnit.SECONDS);
        await(() -> !Files.exists(blockingStorage.resolvePartFile(transferId)));
        receiver.close();
    }

    @Test
    void clientWorkerSubmissionRejectionAndReceiverChunkQueueLimitEnforceBoundedBackpressure() throws Exception {
        // 1. Client worker pool saturation during receiver accept / chunk write
        TransferWorkerPool tinyPool = new TransferWorkerPool(1, 1);
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            tinyPool.submit(() -> {
                occupied.countDown();
                try { release.await(5, TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
            assertTrue(occupied.await(2, TimeUnit.SECONDS));

            CapturingSink sink = new CapturingSink();
            FileReceiver receiver = new FileReceiver(sink, new SafeFileStorage(client1Downloads), tinyPool);
            UUID tid = UUID.randomUUID();
            receiver.handleControl(new MessageEnvelope(1, MessageType.FILE_OFFERED, UUID.randomUUID(), null,
                    new MessageBody.FileOffered(tid, UUID.randomUUID(), "bounded.bin", ProtocolLimits.MAX_CHUNK_BYTES * 40L)));

            // Accept uses the 1 remaining queue slot in tinyPool
            CompletableFuture<FileTransferResult> future = receiver.accept(tid);

            // Queue 32 chunks while writePending=false? Wait: first chunk sets writePending=true and submits to tinyPool, which rejects!
            receiver.handleChunk(new FileChunk(UUID.randomUUID(), tid, 0, new byte[ProtocolLimits.MAX_CHUNK_BYTES]));
            FileTransferResult res = future.get(3, TimeUnit.SECONDS);
            assertFalse(res.success());
            assertTrue(res.message().contains("worker capacity is unavailable"));
            assertEquals(FileReceiver.ReceiverState.FAILED, receiver.state());
        } finally {
            release.countDown();
            tinyPool.close();
        }

        // 2. Receiver pending chunk queue limit (MAX_QUEUED_CHUNKS = 32)
        CountDownLatch writeEntered = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        SafeFileStorage holdingStorage = new SafeFileStorage(client2Downloads) {
            @Override public void appendChunk(UUID id, byte[] data, int offset, int length) throws IOException {
                writeEntered.countDown();
                try { releaseWrite.await(5, TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                super.appendChunk(id, data, offset, length);
            }
        };
        try {
            CapturingSink sink2 = new CapturingSink();
            FileReceiver receiver2 = new FileReceiver(sink2, holdingStorage, sharedWorkerPool);
            UUID tid2 = UUID.randomUUID();
            long totalSize = ProtocolLimits.MAX_CHUNK_BYTES * 40L;
            receiver2.handleControl(new MessageEnvelope(1, MessageType.FILE_OFFERED, UUID.randomUUID(), null,
                    new MessageBody.FileOffered(tid2, UUID.randomUUID(), "queue_bound.bin", totalSize)));
            CompletableFuture<FileTransferResult> future2 = receiver2.accept(tid2);
            assertEquals(MessageType.FILE_ACCEPT, ((MessageEnvelope) sink2.takeFrame(3)).type());

            // Chunk 0 enters worker and blocks in appendChunk (writePending = true)
            receiver2.handleChunk(new FileChunk(UUID.randomUUID(), tid2, 0, new byte[ProtocolLimits.MAX_CHUNK_BYTES]));
            assertTrue(writeEntered.await(2, TimeUnit.SECONDS));

            // Chunks 1..32 fill the 32-element receiver queue
            for (int seq = 1; seq <= 32; seq++) {
                receiver2.handleChunk(new FileChunk(UUID.randomUUID(), tid2, seq, new byte[ProtocolLimits.MAX_CHUNK_BYTES]));
            }
            assertEquals(FileReceiver.ReceiverState.RECEIVING, receiver2.state());

            // Chunk 33 exceeds MAX_QUEUED_CHUNKS (32) and deterministically fails the transfer
            receiver2.handleChunk(new FileChunk(UUID.randomUUID(), tid2, 33, new byte[ProtocolLimits.MAX_CHUNK_BYTES]));
            FileTransferResult res2 = future2.get(3, TimeUnit.SECONDS);
            assertFalse(res2.success());
            assertTrue(res2.message().contains("pending chunk limit reached"));
            assertEquals(FileReceiver.ReceiverState.FAILED, receiver2.state());
        } finally {
            releaseWrite.countDown();
        }
    }

    private static void await(java.util.function.BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            try { Thread.sleep(2); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); fail("Interrupted"); }
        }
        assertTrue(condition.getAsBoolean(), "Expected asynchronous file operation did not finish");
    }
}
