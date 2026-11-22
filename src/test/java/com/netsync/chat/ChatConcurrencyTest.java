package com.netsync.chat;

import com.netsync.common.AckStatus;
import com.netsync.common.AuthorizationService;
import com.netsync.common.ConnectionContext;
import com.netsync.common.EnqueueResult;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import com.netsync.common.ProtocolLimits;
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

class ChatConcurrencyTest {

    private MessageRouterTest.FakeSessionRegistry registry;
    private MultiClientSink sink;
    private MultiClientAuth auth;
    private ChatHandler handler;

    // 5 Clients: Alice, Bob, Charlie, David, Eve
    private final List<SessionView> clients = new ArrayList<>();
    private final Map<UUID, ConnectionContext> contexts = new HashMap<>();

    @BeforeEach
    void setUp() {
        registry = new MessageRouterTest.FakeSessionRegistry();
        sink = new MultiClientSink();
        auth = new MultiClientAuth();
        handler = new ChatHandler(registry, auth, sink);

        String[] names = {"alice", "bob", "charlie", "david", "eve"};
        for (int i = 0; i < names.length; i++) {
            UUID clientId = UUID.fromString("00000000-0000-0000-0000-00000000000" + (i + 1));
            UUID connId = UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccc000" + (i + 1));
            SessionView view = new SessionView(clientId, names[i], connId);
            clients.add(view);
            registry.register(view);
            auth.register(connId, view);

            contexts.put(clientId, new ConnectionContext() {
                @Override public UUID connectionId() { return connId; }
                @Override public Optional<SessionView> session() { return Optional.of(view); }
            });
        }
    }

    @Test
    @DisplayName("5 clients concurrently exchange messages: all delivered correctly with correlated ACKs")
    void fiveClientConcurrentMessaging() {
        SessionView alice = clients.get(0);
        SessionView bob = clients.get(1);
        SessionView charlie = clients.get(2);
        SessionView david = clients.get(3);
        SessionView eve = clients.get(4);

        // Alice -> Bob
        UUID req1 = UUID.randomUUID();
        handler.handle(contexts.get(alice.clientId()), new MessageEnvelope(
                ProtocolLimits.VERSION, MessageType.CHAT_SEND, req1, null,
                new MessageBody.ChatSend(bob.clientId(), "Hello Bob from Alice")
        ));

        // Charlie -> David
        UUID req2 = UUID.randomUUID();
        handler.handle(contexts.get(charlie.clientId()), new MessageEnvelope(
                ProtocolLimits.VERSION, MessageType.CHAT_SEND, req2, null,
                new MessageBody.ChatSend(david.clientId(), "Hello David from Charlie")
        ));

        // Eve -> Alice
        UUID req3 = UUID.randomUUID();
        handler.handle(contexts.get(eve.clientId()), new MessageEnvelope(
                ProtocolLimits.VERSION, MessageType.CHAT_SEND, req3, null,
                new MessageBody.ChatSend(alice.clientId(), "Hello Alice from Eve")
        ));

        // Bob receives deliver from Alice
        List<MessageEnvelope> bobReceived = sink.getFrames(bob.connectionId());
        assertEquals(1, bobReceived.size());
        assertEquals(MessageType.CHAT_DELIVER, bobReceived.getFirst().type());
        MessageBody.ChatDeliver bDeliver = (MessageBody.ChatDeliver) bobReceived.getFirst().body();
        assertEquals(alice.clientId(), bDeliver.senderClientId());
        assertEquals("Hello Bob from Alice", bDeliver.text());

        // Alice receives ACK for req1
        List<MessageEnvelope> aliceReceived = sink.getFrames(alice.connectionId());
        // Alice also received deliver from Eve! So Alice has 2 frames: Deliver from Eve, ACK for req1
        assertEquals(2, aliceReceived.size());
        MessageEnvelope aliceDeliver = aliceReceived.stream()
                .filter(e -> e.type() == MessageType.CHAT_DELIVER).findFirst().orElseThrow();
        assertEquals(eve.clientId(), ((MessageBody.ChatDeliver) aliceDeliver.body()).senderClientId());

        MessageEnvelope aliceAck = aliceReceived.stream()
                .filter(e -> e.type() == MessageType.ACK).findFirst().orElseThrow();
        assertEquals(req1, aliceAck.replyTo());

        // David receives deliver from Charlie
        List<MessageEnvelope> davidReceived = sink.getFrames(david.connectionId());
        assertEquals(1, davidReceived.size());
        assertEquals(MessageType.CHAT_DELIVER, davidReceived.getFirst().type());

        // Charlie receives ACK for req2
        List<MessageEnvelope> charlieReceived = sink.getFrames(charlie.connectionId());
        assertEquals(1, charlieReceived.size());
        assertEquals(req2, charlieReceived.getFirst().replyTo());

        // Eve receives ACK for req3
        List<MessageEnvelope> eveReceived = sink.getFrames(eve.connectionId());
        assertEquals(1, eveReceived.size());
        assertEquals(req3, eveReceived.getFirst().replyTo());
    }

    @Test
    @DisplayName("When one client disconnects, messages to that client fail with RECIPIENT_UNAVAILABLE without affecting others")
    void disconnectedClientFailsCleanly() {
        SessionView alice = clients.get(0);
        SessionView bob = clients.get(1);
        SessionView charlie = clients.get(2);

        // Bob disconnects: removed from registry and auth
        registry.byClient.remove(bob.clientId());
        registry.byConn.remove(bob.connectionId());
        auth.remove(bob.connectionId());

        // Alice tries to send to Bob
        UUID reqAlice = UUID.randomUUID();
        handler.handle(contexts.get(alice.clientId()), new MessageEnvelope(
                ProtocolLimits.VERSION, MessageType.CHAT_SEND, reqAlice, null,
                new MessageBody.ChatSend(bob.clientId(), "Are you there Bob?")
        ));

        // Alice gets RECIPIENT_UNAVAILABLE
        List<MessageEnvelope> aliceFrames = sink.getFrames(alice.connectionId());
        assertEquals(1, aliceFrames.size());
        MessageEnvelope aliceErr = aliceFrames.getFirst();
        assertEquals(MessageType.ERROR, aliceErr.type());
        assertEquals(reqAlice, aliceErr.replyTo());
        assertEquals(ErrorCode.RECIPIENT_UNAVAILABLE, ((MessageBody.Error) aliceErr.body()).code());

        // Meanwhile, Alice sends to Charlie -> Succeeds!
        UUID reqAlice2 = UUID.randomUUID();
        handler.handle(contexts.get(alice.clientId()), new MessageEnvelope(
                ProtocolLimits.VERSION, MessageType.CHAT_SEND, reqAlice2, null,
                new MessageBody.ChatSend(charlie.clientId(), "Hey Charlie!")
        ));

        List<MessageEnvelope> charlieFrames = sink.getFrames(charlie.connectionId());
        assertEquals(1, charlieFrames.size());
        assertEquals(MessageType.CHAT_DELIVER, charlieFrames.getFirst().type());
    }

    @Test
    @DisplayName("Saturated recipient output queue returns BUSY without blocking or impacting others")
    void slowConsumerQueueFullIsolation() {
        SessionView alice = clients.get(0);
        SessionView bob = clients.get(1);
        SessionView charlie = clients.get(2);

        // Bob's queue is full
        sink.setQueueFull(bob.connectionId(), true);

        // Alice sends to Bob
        UUID req1 = UUID.randomUUID();
        handler.handle(contexts.get(alice.clientId()), new MessageEnvelope(
                ProtocolLimits.VERSION, MessageType.CHAT_SEND, req1, null,
                new MessageBody.ChatSend(bob.clientId(), "Bob you are slow")
        ));

        // Alice receives BUSY error, NOT success ACK
        List<MessageEnvelope> aliceFrames = sink.getFrames(alice.connectionId());
        assertEquals(1, aliceFrames.size());
        MessageEnvelope aliceErr = aliceFrames.getFirst();
        assertEquals(MessageType.ERROR, aliceErr.type());
        assertEquals(req1, aliceErr.replyTo());
        assertEquals(ErrorCode.BUSY, ((MessageBody.Error) aliceErr.body()).code());

        // Alice sends to Charlie (healthy queue) -> immediate success!
        UUID req2 = UUID.randomUUID();
        handler.handle(contexts.get(alice.clientId()), new MessageEnvelope(
                ProtocolLimits.VERSION, MessageType.CHAT_SEND, req2, null,
                new MessageBody.ChatSend(charlie.clientId(), "Charlie is fast")
        ));

        List<MessageEnvelope> charlieFrames = sink.getFrames(charlie.connectionId());
        assertEquals(1, charlieFrames.size());
        assertEquals(MessageType.CHAT_DELIVER, charlieFrames.getFirst().type());
    }

    private static final class MultiClientSink implements com.netsync.common.OutboundSink {
        private final Map<UUID, List<MessageEnvelope>> clientFrames = new HashMap<>();
        private final Map<UUID, Boolean> queueFull = new HashMap<>();

        void setQueueFull(UUID connId, boolean full) {
            queueFull.put(connId, full);
        }

        List<MessageEnvelope> getFrames(UUID connId) {
            return clientFrames.computeIfAbsent(connId, k -> new ArrayList<>());
        }

        @Override
        public EnqueueResult send(UUID connectionId, com.netsync.common.OutboundFrame frame) {
            if (Boolean.TRUE.equals(queueFull.get(connectionId))) {
                return EnqueueResult.REJECTED;
            }
            if (frame instanceof MessageEnvelope envelope) {
                getFrames(connectionId).add(envelope);
            }
            return EnqueueResult.ACCEPTED;
        }
    }

    private static final class MultiClientAuth implements AuthorizationService {
        private final Map<UUID, SessionView> sessions = new HashMap<>();

        void register(UUID connId, SessionView view) {
            sessions.put(connId, view);
        }

        void remove(UUID connId) {
            sessions.remove(connId);
        }

        @Override
        public Optional<SessionView> authenticatedSession(ConnectionContext context) {
            return Optional.ofNullable(sessions.get(context.connectionId()));
        }

        @Override
        public boolean isActive(SessionView session) {
            return session.equals(sessions.get(session.connectionId()));
        }
    }
}
