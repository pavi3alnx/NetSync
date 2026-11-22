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

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    @DisplayName("Correlated ACK prints delivery confirmation")
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

        assertTrue(terminalOutput.stream().anyMatch(line -> line.contains("Delivered to server: " + BOB_CLIENT_ID)));
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

        assertTrue(terminalOutput.stream().anyMatch(line -> line.contains("Delivery failed for message to " + BOB_CLIENT_ID)
                && line.contains("RECIPIENT_UNAVAILABLE")));
    }

    @Test
    @DisplayName("Invalid /chat inputs throw InputException")
    void invalidInputThrows() {
        assertThrows(CommandRegistry.InputException.class, () -> commands.execute("/chat"));
        assertThrows(CommandRegistry.InputException.class, () -> commands.execute("/chat invalid-uuid text"));
        assertThrows(CommandRegistry.InputException.class, () -> commands.execute("/chat " + BOB_CLIENT_ID + "   "));
    }

    static final class FakeClientOutbound implements ClientOutbound {
        final List<OutboundFrame> sentFrames = new ArrayList<>();
        boolean connected = true;

        @Override
        public EnqueueResult send(OutboundFrame frame) {
            sentFrames.add(frame);
            return EnqueueResult.ACCEPTED;
        }

        @Override
        public boolean isConnected() {
            return connected;
        }
    }
}
