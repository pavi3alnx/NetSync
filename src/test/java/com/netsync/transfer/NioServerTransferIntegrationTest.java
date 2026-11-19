package com.netsync.transfer;

import com.netsync.common.*;
import com.netsync.server.MessageDispatcher;
import com.netsync.server.NioServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Component integration test verifying the wire integration between
 * {@link NioServer}, {@link MessageDispatcher}, and {@link FileTransferCoordinator}.
 * <p>
 * Validates real TCP socket communication, frame decoding/encoding, control dispatch,
 * binary chunk dispatch, unauthenticated boundary enforcement, and server lifecycle.
 */
@Timeout(15)
class NioServerTransferIntegrationTest {

    @TempDir
    Path tempDir;

    private final FrameCodec codec = new FrameCodec(new MessageCodec());
    private NioServer server;
    private TransferWorkerPool workerPool;
    private FileTransferCoordinator coordinator;

    @BeforeEach
    void setUp() throws IOException {
        TransferRegistry registry = new TransferRegistry();
        SafeFileStorage storage = new SafeFileStorage(tempDir.resolve("server"));
        workerPool = new TransferWorkerPool(2, 32);

        TransferTestFixtures.TestSessionRegistry sessionRegistry = new TransferTestFixtures.TestSessionRegistry();
        TransferTestFixtures.TestAuthorizationService authService = new TransferTestFixtures.TestAuthorizationService();
        TransferTestFixtures.TestDirectSelectorTasks selectorTasks = new TransferTestFixtures.TestDirectSelectorTasks();

        AtomicReference<OutboundSink> serverSinkRef = new AtomicReference<>();
        OutboundSink delegatingSink = (connectionId, frame) -> {
            OutboundSink s = serverSinkRef.get();
            return s != null ? s.send(connectionId, frame) : EnqueueResult.REJECTED;
        };

        FileTransferService service = new FileTransferService(
                registry, storage, workerPool, delegatingSink, selectorTasks, sessionRegistry, authService
        );
        coordinator = new FileTransferCoordinator(service);

        // Wire MessageDispatcher with FileTransferCoordinator for all file controls
        MessageDispatcher.Builder builder = MessageDispatcher.builder();
        builder.register(MessageType.FILE_OFFER, coordinator::handleControl);
        builder.register(MessageType.FILE_ACCEPT, coordinator::handleControl);
        builder.register(MessageType.FILE_REJECT, coordinator::handleControl);
        builder.register(MessageType.FILE_COMPLETE, coordinator::handleControl);
        builder.register(MessageType.FILE_RECEIVED, coordinator::handleControl);
        builder.register(MessageType.FILE_CANCEL, coordinator::handleControl);

        // Create NioServer on an ephemeral port with the builder and binary chunk callback
        server = new NioServer(0, builder, coordinator::handleChunk);
        serverSinkRef.set(server);
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
        if (workerPool != null) {
            workerPool.close();
        }
    }

    private Socket connect() throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 3000);
        socket.setSoTimeout(5000);
        socket.setTcpNoDelay(true);
        return socket;
    }

    private byte[] encode(OutboundFrame frame) throws ProtocolException {
        ByteBuffer buffer = codec.encode(frame);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private OutboundFrame readFrame(Socket socket) throws Exception {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int length = input.readInt();
        byte[] payload = input.readNBytes(length);
        assertEquals(length, payload.length);
        return codec.decodePayload(ByteBuffer.wrap(payload));
    }

    @Test
    void fileOfferDispatchesThroughNioServerAndReturnsUnauthenticatedError() throws Exception {
        try (Socket client = connect()) {
            UUID messageId = UUID.randomUUID();
            UUID recipientId = UUID.randomUUID();
            MessageEnvelope offer = new MessageEnvelope(
                    ProtocolLimits.VERSION,
                    MessageType.FILE_OFFER,
                    messageId,
                    null,
                    new MessageBody.FileOffer(recipientId, "document.pdf", 1024)
            );

            client.getOutputStream().write(encode(offer));
            client.getOutputStream().flush();

            OutboundFrame responseFrame = readFrame(client);
            assertInstanceOf(MessageEnvelope.class, responseFrame);
            MessageEnvelope response = (MessageEnvelope) responseFrame;

            assertEquals(MessageType.ERROR, response.type());
            assertEquals(messageId, response.replyTo());
            assertInstanceOf(MessageBody.Error.class, response.body());

            MessageBody.Error error = (MessageBody.Error) response.body();
            assertEquals(ErrorCode.UNAUTHENTICATED, error.code());
            assertEquals("Client is not authenticated", error.message());
        }
    }

    @Test
    void binaryChunkDispatchesThroughNioServerAndReturnsUnauthenticatedError() throws Exception {
        try (Socket client = connect()) {
            UUID messageId = UUID.randomUUID();
            UUID transferId = UUID.randomUUID();
            byte[] fileBytes = "NetSync binary chunk integration test payload".getBytes();
            FileChunk chunk = new FileChunk(messageId, transferId, 0, fileBytes);

            client.getOutputStream().write(encode(chunk));
            client.getOutputStream().flush();

            OutboundFrame responseFrame = readFrame(client);
            assertInstanceOf(MessageEnvelope.class, responseFrame);
            MessageEnvelope response = (MessageEnvelope) responseFrame;

            assertEquals(MessageType.ERROR, response.type());
            assertEquals(messageId, response.replyTo());
            assertInstanceOf(MessageBody.Error.class, response.body());

            MessageBody.Error error = (MessageBody.Error) response.body();
            assertEquals(ErrorCode.UNAUTHENTICATED, error.code());
            assertEquals(transferId, error.transferId());
        }
    }

    @Test
    void unsupportedMessageTypeReturnsInvalidRequestFromDispatcher() throws Exception {
        try (Socket client = connect()) {
            UUID messageId = UUID.randomUUID();
            // CHAT_SEND is not registered in this builder
            MessageEnvelope chat = new MessageEnvelope(
                    ProtocolLimits.VERSION,
                    MessageType.CHAT_SEND,
                    messageId,
                    null,
                    new MessageBody.ChatSend(UUID.randomUUID(), "Hello")
            );

            client.getOutputStream().write(encode(chat));
            client.getOutputStream().flush();

            OutboundFrame responseFrame = readFrame(client);
            assertInstanceOf(MessageEnvelope.class, responseFrame);
            MessageEnvelope response = (MessageEnvelope) responseFrame;

            assertEquals(MessageType.ERROR, response.type());
            assertEquals(messageId, response.replyTo());
            assertInstanceOf(MessageBody.Error.class, response.body());

            MessageBody.Error error = (MessageBody.Error) response.body();
            assertEquals(ErrorCode.INVALID_REQUEST, error.code());
        }
    }
}
