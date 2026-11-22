package com.netsync.chat;

import com.netsync.common.EnqueueResult;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundFrame;
import com.netsync.common.OutboundSink;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageRouterTest {

    private static final UUID ALICE_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID BOB_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID BOB_CONN = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    private static final UUID SOURCE_MSG_ID = UUID.fromString("99999999-9999-9999-9999-999999999999");

    private FakeSessionRegistry registry;
    private RecordingSink sink;
    private MessageRouter router;

    @BeforeEach
    void setUp() {
        registry = new FakeSessionRegistry();
        sink = new RecordingSink();
        router = new MessageRouter(registry, sink);
    }

    @Test
    @DisplayName("routeDirectMessage succeeds and enqueues CHAT_DELIVER to recipient connection")
    void routeDirectMessageSuccess() {
        registry.register(new SessionView(BOB_ID, "bob", BOB_CONN));

        MessageRouter.RoutingStatus status = router.routeDirectMessage(
                ALICE_ID, BOB_ID, "Hello Bob!", SOURCE_MSG_ID);

        assertEquals(MessageRouter.RoutingStatus.DELIVERED, status);
        assertEquals(1, sink.records.size());

        RecordingSink.Record record = sink.records.getFirst();
        assertEquals(BOB_CONN, record.connectionId());

        MessageEnvelope envelope = assertInstanceOf(MessageEnvelope.class, record.frame());
        assertEquals(ProtocolLimits.VERSION, envelope.version());
        assertEquals(MessageType.CHAT_DELIVER, envelope.type());
        assertNull(envelope.replyTo());

        MessageBody.ChatDeliver deliver = assertInstanceOf(MessageBody.ChatDeliver.class, envelope.body());
        assertEquals(ALICE_ID, deliver.senderClientId());
        assertEquals("Hello Bob!", deliver.text());
        assertEquals(SOURCE_MSG_ID, deliver.sourceMessageId());
    }

    @Test
    @DisplayName("routeDirectMessage returns RECIPIENT_UNAVAILABLE when recipient is not registered")
    void routeDirectMessageRecipientUnavailable() {
        MessageRouter.RoutingStatus status = router.routeDirectMessage(
                ALICE_ID, BOB_ID, "Hello Bob!", SOURCE_MSG_ID);

        assertEquals(MessageRouter.RoutingStatus.RECIPIENT_UNAVAILABLE, status);
        assertEquals(0, sink.records.size());
    }

    @Test
    @DisplayName("routeDirectMessage returns QUEUE_FULL when outbound sink rejects enqueue")
    void routeDirectMessageQueueFull() {
        registry.register(new SessionView(BOB_ID, "bob", BOB_CONN));
        sink.nextResult = EnqueueResult.REJECTED;

        MessageRouter.RoutingStatus status = router.routeDirectMessage(
                ALICE_ID, BOB_ID, "Hello Bob!", SOURCE_MSG_ID);

        assertEquals(MessageRouter.RoutingStatus.QUEUE_FULL, status);
        assertEquals(1, sink.records.size());
    }

    @Test
    @DisplayName("Null arguments throw NullPointerException")
    void nullArgumentsThrow() {
        assertThrows(NullPointerException.class, () -> new MessageRouter(null, sink));
        assertThrows(NullPointerException.class, () -> new MessageRouter(registry, null));

        assertThrows(NullPointerException.class,
                () -> router.routeDirectMessage(null, BOB_ID, "text", SOURCE_MSG_ID));
        assertThrows(NullPointerException.class,
                () -> router.routeDirectMessage(ALICE_ID, null, "text", SOURCE_MSG_ID));
        assertThrows(NullPointerException.class,
                () -> router.routeDirectMessage(ALICE_ID, BOB_ID, null, SOURCE_MSG_ID));
        assertThrows(NullPointerException.class,
                () -> router.routeDirectMessage(ALICE_ID, BOB_ID, "text", null));
    }

    static final class FakeSessionRegistry implements SessionRegistry {
        final Map<UUID, SessionView> byClient = new HashMap<>();
        final Map<UUID, SessionView> byConn = new HashMap<>();

        void register(SessionView view) {
            byClient.put(view.clientId(), view);
            byConn.put(view.connectionId(), view);
        }

        @Override
        public Optional<SessionView> findByClientId(UUID id) {
            return Optional.ofNullable(byClient.get(id));
        }

        @Override
        public Optional<SessionView> findByConnectionId(UUID id) {
            return Optional.ofNullable(byConn.get(id));
        }

        @Override
        public List<SessionView> activeSessions() {
            return List.copyOf(byClient.values());
        }
    }

    static final class RecordingSink implements OutboundSink {
        record Record(UUID connectionId, OutboundFrame frame) {}
        final List<Record> records = new ArrayList<>();
        EnqueueResult nextResult = EnqueueResult.ACCEPTED;

        @Override
        public EnqueueResult send(UUID connectionId, OutboundFrame frame) {
            records.add(new Record(connectionId, frame));
            return nextResult;
        }
    }
}
