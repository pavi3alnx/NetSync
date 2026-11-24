package com.netsync.chat;

import com.netsync.client.ClientDispatcher;
import com.netsync.client.ClientOutbound;
import com.netsync.client.CommandRegistry;
import com.netsync.common.AckStatus;
import com.netsync.common.EnqueueResult;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundFrame;
import com.netsync.common.ProtocolLimits;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChatClientAdapterTest {

    private static final UUID BOB_CLIENT_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID ALICE_CLIENT_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    private FakeClientOutbound outbound;
    private ClientDispatcher dispatcher;
    private CommandRegistry commands;
    private List<String> terminalOutput;
    private ChatClientAdapter adapter;

    @BeforeEach
    void setUp() {
        outbound = new FakeClientOutbound();
        dispatcher = new ClientDispatcher();
        commands = new CommandRegistry();
        terminalOutput = new ArrayList<>();
        adapter = new ChatClientAdapter(outbound, dispatcher, commands, terminalOutput::add);
    }

    @AfterEach
    void tearDown() {
        adapter.close();
        dispatcher.close();
    }

    @Test
    @DisplayName("/chat command executes, sends CHAT_SEND and notifies user")
    void executeChatCommandSendsEnvelope() throws Exception {
        commands.execute("/chat " + BOB_CLIENT_ID + " Hello Bob!");

        assertEquals(1, outbound.sentFrames.size());
        MessageEnvelope sent = assertInstanceOf(MessageEnvelope.class, outbound.sentFrames.getFirst());
        assertEquals(MessageType.CHAT_SEND, sent.type());

        MessageBody.ChatSend body = assertInstanceOf(MessageBody.ChatSend.class, sent.body());
        assertEquals(BOB_CLIENT_ID, body.recipientClientId());
        assertEquals("Hello Bob!", body.text());

        assertTrue(terminalOutput.stream().anyMatch(line -> line.contains("Me -> " + BOB_CLIENT_ID) && line.contains("Hello Bob!")));
    }

    @Test
    @DisplayName("Incoming CHAT_DELIVER prints to terminal with sender ID and text")
    void incomingChatDeliverPrintsMessage() {
        UUID sourceMsgId = UUID.randomUUID();
        MessageEnvelope deliver = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.CHAT_DELIVER,
                UUID.randomUUID(),
                null,
                new MessageBody.ChatDeliver(ALICE_CLIENT_ID, "Good morning!", sourceMsgId)
        );

        adapter.onChatDeliver(deliver);

        assertTrue(terminalOutput.stream().anyMatch(line -> line.contains("From " + ALICE_CLIENT_ID) && line.contains("Good morning!")));
    }

    @Test
    @DisplayName("Correlated ACK reports queue acceptance without claiming delivery")
    void correlatedAckPrintsDeliveryConfirmation() throws Exception {
        commands.execute("/chat " + BOB_CLIENT_ID + " Ping");
        MessageEnvelope sent = (MessageEnvelope) outbound.sentFrames.getFirst();

        MessageEnvelope ack = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ACK,
                UUID.randomUUID(),
                sent.messageId(),
                new MessageBody.Ack(MessageType.CHAT_SEND, AckStatus.QUEUED, null)
        );

        adapter.onAck(ack);

        assertTrue(terminalOutput.stream().anyMatch(line -> line.contains("Queued for recipient " + BOB_CLIENT_ID)
                && line.contains("does not confirm that it was read")));
    }

    @Test
    @DisplayName("Correlated ERROR prints failure reason")
    void correlatedErrorPrintsFailureReason() throws Exception {
        commands.execute("/chat " + BOB_CLIENT_ID + " Ping");
        MessageEnvelope sent = (MessageEnvelope) outbound.sentFrames.getFirst();

        MessageEnvelope error = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ERROR,
                UUID.randomUUID(),
                sent.messageId(),
                new MessageBody.Error(ErrorCode.RECIPIENT_UNAVAILABLE, "Recipient is not connected", null)
        );

        adapter.onError(error);

        assertTrue(terminalOutput.stream().anyMatch(line -> line.contains("Delivery failed for recipient " + BOB_CLIENT_ID)
                && line.contains("RECIPIENT_UNAVAILABLE")));
    }

    @Test
    @DisplayName("Invalid /chat inputs throw InputException")
    void invalidInputThrows() {
        assertThrows(CommandRegistry.InputException.class, () -> commands.execute("/chat"));
        assertThrows(CommandRegistry.InputException.class, () -> commands.execute("/chat invalid-uuid text"));
        assertThrows(CommandRegistry.InputException.class, () -> commands.execute("/chat " + BOB_CLIENT_ID + "   "));
    }

    @Test
    @DisplayName("Unrelated, duplicate, and unknown responses do not consume pending chat requests")
    void responseCorrelationIsStrictAndTerminalOnlyOnce() throws Exception {
        commands.execute("/chat " + BOB_CLIENT_ID + " Ping");
        MessageEnvelope sent = (MessageEnvelope) outbound.sentFrames.getFirst();
        adapter.onAck(new MessageEnvelope(1, MessageType.ACK, UUID.randomUUID(), sent.messageId(),
                new MessageBody.Ack(MessageType.PRESENCE_QUERY, AckStatus.ACCEPTED, null)));
        adapter.onError(new MessageEnvelope(1, MessageType.ERROR, UUID.randomUUID(), UUID.randomUUID(),
                new MessageBody.Error(ErrorCode.INVALID_REQUEST, "safe", null)));
        assertFalse(adapter.ownsResponse(new MessageEnvelope(1, MessageType.ERROR, UUID.randomUUID(), UUID.randomUUID(),
                new MessageBody.Error(ErrorCode.INVALID_REQUEST, "unrelated", null))));
        assertEquals(1, adapter.pendingRequestCountForTesting());

        MessageEnvelope ack = new MessageEnvelope(1, MessageType.ACK, UUID.randomUUID(), sent.messageId(),
                new MessageBody.Ack(MessageType.CHAT_SEND, AckStatus.QUEUED, null));
        adapter.onAck(ack);
        int outputCount = terminalOutput.size();
        adapter.onAck(ack);
        MessageEnvelope duplicateError = new MessageEnvelope(1, MessageType.ERROR, UUID.randomUUID(), sent.messageId(),
                new MessageBody.Error(ErrorCode.RECIPIENT_UNAVAILABLE, "safe", null));
        assertTrue(adapter.ownsResponse(duplicateError));
        adapter.onError(duplicateError);
        assertEquals(0, adapter.pendingRequestCountForTesting());
        assertEquals(outputCount, terminalOutput.size());
    }

    @Test
    @DisplayName("Rejected enqueue removes pending state and bounded timeout reports unknown outcome")
    void rejectedEnqueueAndTimeoutAreBounded() throws Exception {
        adapter.close();
        AtomicLong clock = new AtomicLong();
        outbound.result = EnqueueResult.REJECTED;
        adapter = new ChatClientAdapter(outbound, dispatcher, commands, terminalOutput::add,
                clock::get, Duration.ofSeconds(5), 1);
        assertThrows(CommandRegistry.InputException.class, () -> commands.execute("/chat " + BOB_CLIENT_ID + " rejected"));
        assertEquals(0, adapter.pendingRequestCountForTesting());

        outbound.result = EnqueueResult.ACCEPTED;
        commands.execute("/chat " + BOB_CLIENT_ID + " pending");
        MessageEnvelope request = (MessageEnvelope) outbound.sentFrames.getFirst();
        assertEquals(1, adapter.pendingRequestCountForTesting());
        assertThrows(CommandRegistry.InputException.class, () -> commands.execute("/chat " + BOB_CLIENT_ID + " at-capacity"));
        clock.set(TimeUnit.SECONDS.toNanos(5));
        adapter.expirePendingForTesting();
        assertEquals(0, adapter.pendingRequestCountForTesting());
        assertTrue(terminalOutput.stream().anyMatch(line -> line.contains("outcome is unknown") && line.contains("No retry")));
        MessageEnvelope lateError = new MessageEnvelope(1, MessageType.ERROR, UUID.randomUUID(), request.messageId(),
                new MessageBody.Error(ErrorCode.RECIPIENT_UNAVAILABLE, "safe", null));
        assertTrue(adapter.ownsResponse(lateError));
        adapter.onError(lateError);
        MessageEnvelope lateAck = new MessageEnvelope(1, MessageType.ACK, UUID.randomUUID(), request.messageId(),
                new MessageBody.Ack(MessageType.CHAT_SEND, AckStatus.QUEUED, null));
        adapter.onAck(lateAck);
        assertEquals(0, adapter.pendingRequestCountForTesting());
    }

    @Test
    @DisplayName("Concurrent request and response transitions keep pending storage consistent")
    void concurrentPendingTransitionsRemainConsistent() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            var sends = new CopyOnWriteArrayList<MessageEnvelope>();
            for (int i = 0; i < 40; i++) {
                int index = i;
                pool.submit(() -> {
                    try { commands.execute("/chat " + BOB_CLIENT_ID + " message " + index); }
                    catch (Exception failure) { throw new CompletionException(failure); }
                });
            }
            pool.shutdown();
            assertTrue(pool.awaitTermination(3, TimeUnit.SECONDS));
            outbound.sentFrames.stream().filter(MessageEnvelope.class::isInstance)
                    .map(MessageEnvelope.class::cast).forEach(sends::add);
            assertEquals(40, sends.size());
            ExecutorService responses = Executors.newFixedThreadPool(8);
            try {
                for (MessageEnvelope request : sends) responses.submit(() -> adapter.onAck(new MessageEnvelope(
                        1, MessageType.ACK, UUID.randomUUID(), request.messageId(),
                        new MessageBody.Ack(MessageType.CHAT_SEND, AckStatus.QUEUED, null))));
                responses.shutdown();
                assertTrue(responses.awaitTermination(3, TimeUnit.SECONDS));
                assertEquals(0, adapter.pendingRequestCountForTesting());
            } finally { responses.shutdownNow(); }
        } finally { pool.shutdownNow(); }
    }

    @Test
    @DisplayName("Terminal response tombstones stay bounded and do not claim unrelated module errors")
    void terminalTombstonesRemainBounded() throws Exception {
        for (int i = 0; i < ChatClientAdapter.MAX_PENDING_REQUESTS + 12; i++) {
            commands.execute("/chat " + BOB_CLIENT_ID + " tombstone " + i);
            MessageEnvelope request = (MessageEnvelope) outbound.sentFrames.getLast();
            adapter.onAck(new MessageEnvelope(1, MessageType.ACK, UUID.randomUUID(), request.messageId(),
                    new MessageBody.Ack(MessageType.CHAT_SEND, AckStatus.QUEUED, null)));
        }
        assertEquals(0, adapter.pendingRequestCountForTesting());
        assertEquals(ChatClientAdapter.MAX_PENDING_REQUESTS, adapter.terminalTombstoneCountForTesting());
        MessageEnvelope unrelated = new MessageEnvelope(1, MessageType.ERROR, UUID.randomUUID(), UUID.randomUUID(),
                new MessageBody.Error(ErrorCode.BUSY, "safe", null));
        assertFalse(adapter.ownsResponse(unrelated));
    }

    static final class FakeClientOutbound implements ClientOutbound {
        final List<OutboundFrame> sentFrames = new ArrayList<>();
        boolean connected = true;
        EnqueueResult result = EnqueueResult.ACCEPTED;

        @Override
        public EnqueueResult send(OutboundFrame frame) {
            if (result == EnqueueResult.ACCEPTED) sentFrames.add(frame);
            return result;
        }

        @Override
        public boolean isConnected() {
            return connected;
        }
    }
}
