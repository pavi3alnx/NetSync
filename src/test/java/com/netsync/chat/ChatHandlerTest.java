package com.netsync.chat;

import com.netsync.common.AckStatus;
import com.netsync.common.AuthorizationService;
import com.netsync.common.ConnectionContext;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChatHandlerTest {

    private static final UUID ALICE_CLIENT = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID ALICE_CONN = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID BOB_CLIENT = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID BOB_CONN = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    private static final UUID REQ_ID = UUID.fromString("44444444-4444-4444-4444-444444444444");

    private MessageRouterTest.FakeSessionRegistry registry;
    private MessageRouterTest.RecordingSink sink;
    private FakeAuthService auth;
    private ChatHandler handler;

    @BeforeEach
    void setUp() {
        registry = new MessageRouterTest.FakeSessionRegistry();
        sink = new MessageRouterTest.RecordingSink();
        auth = new FakeAuthService();
        handler = new ChatHandler(registry, auth, sink);
    }

    @Test
    @DisplayName("End-to-end: Alice sends message to Bob, Bob receives CHAT_DELIVER, Alice receives ACK")
    void fullChatFlowBetweenTwoClients() {
        SessionView aliceSession = new SessionView(ALICE_CLIENT, "alice", ALICE_CONN);
        SessionView bobSession = new SessionView(BOB_CLIENT, "bob", BOB_CONN);

        registry.register(aliceSession);
        registry.register(bobSession);
        auth.session = aliceSession;

        ConnectionContext aliceContext = fakeContext(ALICE_CONN, aliceSession);
        MessageEnvelope sendEnvelope = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.CHAT_SEND,
                REQ_ID,
                null,
                new MessageBody.ChatSend(BOB_CLIENT, "Hello Bob!")
        );

        handler.handle(aliceContext, sendEnvelope);

        assertEquals(2, sink.records.size());

        // 1. Recipient receives CHAT_DELIVER
        MessageRouterTest.RecordingSink.Record deliverRecord = sink.records.get(0);
        assertEquals(BOB_CONN, deliverRecord.connectionId());
        MessageEnvelope deliver = (MessageEnvelope) deliverRecord.frame();
        assertEquals(MessageType.CHAT_DELIVER, deliver.type());
        assertNull(deliver.replyTo());
        MessageBody.ChatDeliver deliverBody = (MessageBody.ChatDeliver) deliver.body();
        assertEquals(ALICE_CLIENT, deliverBody.senderClientId()); // Server derived
        assertEquals("Hello Bob!", deliverBody.text());
        assertEquals(REQ_ID, deliverBody.sourceMessageId());

        // 2. Sender receives ACK
        MessageRouterTest.RecordingSink.Record ackRecord = sink.records.get(1);
        assertEquals(ALICE_CONN, ackRecord.connectionId());
        MessageEnvelope ack = (MessageEnvelope) ackRecord.frame();
        assertEquals(MessageType.ACK, ack.type());
        assertEquals(REQ_ID, ack.replyTo());
        MessageBody.Ack ackBody = (MessageBody.Ack) ack.body();
        assertEquals(MessageType.CHAT_SEND, ackBody.operation());
        assertEquals(AckStatus.QUEUED, ackBody.status());
    }

    @Test
    @DisplayName("Unauthenticated sender receives UNAUTHENTICATED error")
    void unauthenticatedSenderRejected() {
        auth.session = null;
        ConnectionContext context = fakeContext(ALICE_CONN, null);
        MessageEnvelope message = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.CHAT_SEND,
                REQ_ID,
                null,
                new MessageBody.ChatSend(BOB_CLIENT, "Hello")
        );

        handler.handle(context, message);

        assertEquals(1, sink.records.size());
        MessageEnvelope error = (MessageEnvelope) sink.records.getFirst().frame();
        assertEquals(MessageType.ERROR, error.type());
        assertEquals(REQ_ID, error.replyTo());
        assertEquals(ErrorCode.UNAUTHENTICATED, ((MessageBody.Error) error.body()).code());
    }

    @Test
    @DisplayName("Inactive sender session receives UNAUTHENTICATED error")
    void inactiveSessionRejected() {
        SessionView session = new SessionView(ALICE_CLIENT, "alice", ALICE_CONN);
        auth.session = session;
        auth.active = false;

        ConnectionContext context = fakeContext(ALICE_CONN, session);
        MessageEnvelope message = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.CHAT_SEND,
                REQ_ID,
                null,
                new MessageBody.ChatSend(BOB_CLIENT, "Hello")
        );

        handler.handle(context, message);

        assertEquals(1, sink.records.size());
        MessageEnvelope error = (MessageEnvelope) sink.records.getFirst().frame();
        assertEquals(ErrorCode.UNAUTHENTICATED, ((MessageBody.Error) error.body()).code());
    }

    @Test
    @DisplayName("Wrong message type produces INVALID_REQUEST error")
    void wrongMessageTypeRejected() {
        ConnectionContext context = fakeContext(ALICE_CONN, null);
        MessageEnvelope message = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.LOGIN_REQUEST,
                REQ_ID,
                null,
                new MessageBody.LoginRequest("alice", "pass")
        );

        handler.handle(context, message);

        assertEquals(1, sink.records.size());
        MessageEnvelope error = (MessageEnvelope) sink.records.getFirst().frame();
        assertEquals(ErrorCode.INVALID_REQUEST, ((MessageBody.Error) error.body()).code());
    }

    @Test
    @DisplayName("Wrong body shape produces INVALID_REQUEST error")
    void wrongBodyShapeRejected() {
        ConnectionContext context = fakeContext(ALICE_CONN, null);
        MessageEnvelope message = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.CHAT_SEND,
                REQ_ID,
                null,
                new MessageBody.Empty()
        );

        handler.handle(context, message);

        assertEquals(1, sink.records.size());
        MessageEnvelope error = (MessageEnvelope) sink.records.getFirst().frame();
        assertEquals(ErrorCode.INVALID_REQUEST, ((MessageBody.Error) error.body()).code());
    }

    @Test
    @DisplayName("Constructor null validation")
    void constructorNullValidation() {
        assertThrows(NullPointerException.class, () -> new ChatHandler((ChatService) null, auth, sink));
        assertThrows(NullPointerException.class, () -> new ChatHandler((SessionRegistry) null, auth, sink));
        assertThrows(NullPointerException.class, () -> new ChatHandler(registry, null, sink));
        assertThrows(NullPointerException.class, () -> new ChatHandler(registry, auth, null));
    }

    private static ConnectionContext fakeContext(UUID connectionId, SessionView session) {
        return new ConnectionContext() {
            @Override public UUID connectionId() { return connectionId; }
            @Override public Optional<SessionView> session() { return Optional.ofNullable(session); }
        };
    }

    private static final class FakeAuthService implements AuthorizationService {
        SessionView session;
        boolean active = true;

        @Override
        public Optional<SessionView> authenticatedSession(ConnectionContext context) {
            return Optional.ofNullable(session);
        }

        @Override
        public boolean isActive(SessionView s) {
            return active && s.equals(session);
        }
    }
}
