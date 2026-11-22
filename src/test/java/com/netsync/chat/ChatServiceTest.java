package com.netsync.chat;

import com.netsync.common.AckStatus;
import com.netsync.common.EnqueueResult;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundFrame;
import com.netsync.common.OutboundSink;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChatServiceTest {

    private static final UUID SENDER_CLIENT = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID SENDER_CONN = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID RECIPIENT_CLIENT = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID REQ_ID = UUID.fromString("33333333-3333-3333-3333-333333333333");

    private MessageRouterTest.FakeSessionRegistry registry;
    private MessageRouterTest.RecordingSink sink;
    private MessageRouter router;
    private ChatService service;
    private SessionView senderSession;

    @BeforeEach
    void setUp() {
        registry = new MessageRouterTest.FakeSessionRegistry();
        sink = new MessageRouterTest.RecordingSink();
        router = new MessageRouter(registry, sink);
        service = new ChatService(router, sink);
        senderSession = new SessionView(SENDER_CLIENT, "alice", SENDER_CONN);
    }

    @Test
    @DisplayName("Successful chat processing sends ACK(QUEUED) to sender")
    void processChatSendSuccess() {
        UUID recipientConn = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        registry.register(new SessionView(RECIPIENT_CLIENT, "bob", recipientConn));

        MessageBody.ChatSend chatSend = new MessageBody.ChatSend(RECIPIENT_CLIENT, "Hey Bob");
        service.processChatSend(senderSession, REQ_ID, chatSend);

        // 2 frames: 1 to recipient (CHAT_DELIVER), 1 to sender (ACK)
        assertEquals(2, sink.records.size());

        // First frame: CHAT_DELIVER to recipient
        MessageRouterTest.RecordingSink.Record deliverRecord = sink.records.get(0);
        assertEquals(recipientConn, deliverRecord.connectionId());
        assertEquals(MessageType.CHAT_DELIVER, ((MessageEnvelope) deliverRecord.frame()).type());

        // Second frame: ACK to sender
        MessageRouterTest.RecordingSink.Record ackRecord = sink.records.get(1);
        assertEquals(SENDER_CONN, ackRecord.connectionId());
        MessageEnvelope ackEnvelope = (MessageEnvelope) ackRecord.frame();
        assertEquals(MessageType.ACK, ackEnvelope.type());
        assertEquals(REQ_ID, ackEnvelope.replyTo());

        MessageBody.Ack ack = assertInstanceOf(MessageBody.Ack.class, ackEnvelope.body());
        assertEquals(MessageType.CHAT_SEND, ack.operation());
        assertEquals(AckStatus.QUEUED, ack.status());
        assertNull(ack.transferId());
    }

    @Test
    @DisplayName("Empty text returns INVALID_REQUEST error to sender")
    void processChatSendEmptyText() {
        MessageBody.ChatSend chatSend = new MessageBody.ChatSend(RECIPIENT_CLIENT, "");
        service.processChatSend(senderSession, REQ_ID, chatSend);

        assertEquals(1, sink.records.size());
        MessageRouterTest.RecordingSink.Record record = sink.records.getFirst();
        assertEquals(SENDER_CONN, record.connectionId());

        MessageEnvelope envelope = (MessageEnvelope) record.frame();
        assertEquals(MessageType.ERROR, envelope.type());
        assertEquals(REQ_ID, envelope.replyTo());

        MessageBody.Error error = assertInstanceOf(MessageBody.Error.class, envelope.body());
        assertEquals(ErrorCode.INVALID_REQUEST, error.code());
    }

    @Test
    @DisplayName("Null text returns INVALID_REQUEST error to sender")
    void processChatSendNullText() {
        MessageBody.ChatSend chatSend = new MessageBody.ChatSend(RECIPIENT_CLIENT, null);
        service.processChatSend(senderSession, REQ_ID, chatSend);

        assertEquals(1, sink.records.size());
        MessageEnvelope envelope = (MessageEnvelope) sink.records.getFirst().frame();
        assertEquals(MessageType.ERROR, envelope.type());
        assertEquals(ErrorCode.INVALID_REQUEST, ((MessageBody.Error) envelope.body()).code());
    }

    @Test
    @DisplayName("Null recipient client ID returns INVALID_REQUEST error to sender")
    void processChatSendNullRecipient() {
        MessageBody.ChatSend chatSend = new MessageBody.ChatSend(null, "Hello");
        service.processChatSend(senderSession, REQ_ID, chatSend);

        assertEquals(1, sink.records.size());
        MessageEnvelope envelope = (MessageEnvelope) sink.records.getFirst().frame();
        assertEquals(MessageType.ERROR, envelope.type());
        assertEquals(ErrorCode.INVALID_REQUEST, ((MessageBody.Error) envelope.body()).code());
    }

    @Test
    @DisplayName("Oversized text (>4096 bytes) returns SIZE_LIMIT error to sender")
    void processChatSendOversizedText() {
        String longText = "A".repeat(ProtocolLimits.MAX_CHAT_BYTES + 1);
        MessageBody.ChatSend chatSend = new MessageBody.ChatSend(RECIPIENT_CLIENT, longText);
        service.processChatSend(senderSession, REQ_ID, chatSend);

        assertEquals(1, sink.records.size());
        MessageEnvelope envelope = (MessageEnvelope) sink.records.getFirst().frame();
        assertEquals(MessageType.ERROR, envelope.type());
        assertEquals(ErrorCode.SIZE_LIMIT, ((MessageBody.Error) envelope.body()).code());
    }

    @Test
    @DisplayName("Unavailable recipient returns RECIPIENT_UNAVAILABLE error to sender")
    void processChatSendRecipientUnavailable() {
        MessageBody.ChatSend chatSend = new MessageBody.ChatSend(RECIPIENT_CLIENT, "Hello?");
        service.processChatSend(senderSession, REQ_ID, chatSend);

        assertEquals(1, sink.records.size());
        MessageEnvelope envelope = (MessageEnvelope) sink.records.getFirst().frame();
        assertEquals(MessageType.ERROR, envelope.type());
        assertEquals(REQ_ID, envelope.replyTo());

        MessageBody.Error error = assertInstanceOf(MessageBody.Error.class, envelope.body());
        assertEquals(ErrorCode.RECIPIENT_UNAVAILABLE, error.code());
    }

    @Test
    @DisplayName("Queue full returns BUSY error to sender")
    void processChatSendQueueFull() {
        UUID recipientConn = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        registry.register(new SessionView(RECIPIENT_CLIENT, "bob", recipientConn));
        sink.nextResult = EnqueueResult.REJECTED;

        MessageBody.ChatSend chatSend = new MessageBody.ChatSend(RECIPIENT_CLIENT, "Hey Bob");
        service.processChatSend(senderSession, REQ_ID, chatSend);

        // 2 records: 1 failed delivery attempt to recipient, 1 BUSY error to sender
        assertEquals(2, sink.records.size());
        MessageEnvelope errorEnvelope = (MessageEnvelope) sink.records.get(1).frame();
        assertEquals(MessageType.ERROR, errorEnvelope.type());
        assertEquals(ErrorCode.BUSY, ((MessageBody.Error) errorEnvelope.body()).code());
    }

    @Test
    @DisplayName("Constructor null checks")
    void constructorNullChecks() {
        assertThrows(NullPointerException.class, () -> new ChatService(null, sink));
        assertThrows(NullPointerException.class, () -> new ChatService(router, null));
    }
}
