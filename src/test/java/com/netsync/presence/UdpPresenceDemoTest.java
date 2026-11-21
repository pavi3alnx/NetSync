package com.netsync.presence;

import com.netsync.common.AuthorizationService;
import com.netsync.common.ConnectionContext;
import com.netsync.common.EnqueueResult;
import com.netsync.common.Heartbeat;
import com.netsync.common.HeartbeatCodec;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundFrame;
import com.netsync.common.OutboundSink;
import com.netsync.common.PresenceState;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;
import org.junit.jupiter.api.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UdpPresenceDemoTest {
    private static final UUID CLIENT_ID = UUID.randomUUID();
    private static final UUID CONNECTION_ID = UUID.randomUUID();
    private static final SessionView SESSION = new SessionView(CLIENT_ID, "demo_user", CONNECTION_ID);
    private static final String TEST_TOKEN = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    @Test
    void loopbackHeartbeatShowsOnlineOfflineAndRecovery() throws Exception {
        TestSessionRegistry sessions = new TestSessionRegistry(SESSION);
        AtomicLong monotonicTime = new AtomicLong();
        List<PresenceState> transitions = new CopyOnWriteArrayList<>();
        HeartbeatTracker tracker = new HeartbeatTracker(
                monotonicTime::get,
                (clientId, state) -> {
                    transitions.add(state);
                    System.out.println("[Presence demo] transition " + SESSION.username() + "=" + state);
                },
                sessions,
                (clientId, token) -> CLIENT_ID.equals(clientId) && TEST_TOKEN.equals(token));
        CapturingSink sink = new CapturingSink();
        PresenceHandler handler = presenceHandler(sessions, tracker, sink);
        UdpPresenceService service = new UdpPresenceService(
                new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0),
                tracker);

        assertTrue(tracker.registerSession(CLIENT_ID, TEST_TOKEN));
        service.start();
        try {
            assertEquals(PresenceState.ONLINE, query(handler, sink, tracker));
            monotonicTime.addAndGet(TimeUnit.SECONDS.toNanos(ProtocolLimits.PRESENCE_TIMEOUT_SECONDS));
            assertEventually(
                    3_000,
                    () -> tracker.getPresenceState(CLIENT_ID).orElse(null) == PresenceState.OFFLINE);
            assertEquals(PresenceState.OFFLINE, query(handler, sink, tracker));

            sendHeartbeat(service.getLocalAddress(), 1);
            assertEventually(
                    3_000,
                    () -> tracker.getPresenceState(CLIENT_ID).orElse(null) == PresenceState.ONLINE);
            assertEquals(PresenceState.ONLINE, query(handler, sink, tracker));
            assertEquals(
                    List.of(PresenceState.ONLINE, PresenceState.OFFLINE, PresenceState.ONLINE),
                    transitions);
        } finally {
            service.stop();
        }
    }

    private static void sendHeartbeat(InetSocketAddress destination, long sequence) throws Exception {
        byte[] datagram = new HeartbeatCodec().encode(
                new Heartbeat(ProtocolLimits.VERSION, "HEARTBEAT", CLIENT_ID, TEST_TOKEN, sequence));
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.send(new DatagramPacket(
                    datagram,
                    datagram.length,
                    destination.getAddress(),
                    destination.getPort()));
        }
    }

    private static PresenceHandler presenceHandler(
            TestSessionRegistry sessions,
            HeartbeatTracker tracker,
            CapturingSink sink) {
        AuthorizationService authorization = new AuthorizationService() {
            @Override
            public Optional<SessionView> authenticatedSession(ConnectionContext context) {
                return context != null
                        && context.session().filter(SESSION::equals).isPresent()
                        && sessions.findByConnectionId(context.connectionId()).filter(SESSION::equals).isPresent()
                        ? Optional.of(SESSION)
                        : Optional.empty();
            }

            @Override
            public boolean isActive(SessionView session) {
                return SESSION.equals(session)
                        && sessions.findByClientId(session.clientId()).filter(SESSION::equals).isPresent();
            }
        };
        return new PresenceHandler(authorization, sessions, tracker, sink);
    }

    private static PresenceState query(
            PresenceHandler handler,
            CapturingSink sink,
            HeartbeatTracker tracker) {
        MessageEnvelope request = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.PRESENCE_QUERY,
                UUID.randomUUID(),
                null,
                new MessageBody.Empty());
        sink.frame = null;
        handler.handle(new TestConnectionContext(), request);
        MessageEnvelope response = assertInstanceOf(MessageEnvelope.class, sink.frame);
        assertEquals(MessageType.PRESENCE_SNAPSHOT, response.type());
        assertEquals(request.messageId(), response.replyTo());
        MessageBody.PresenceSnapshot snapshot =
                assertInstanceOf(MessageBody.PresenceSnapshot.class, response.body());
        MessageBody.PresenceEntry entry = snapshot.clients().stream()
                .filter(client -> client.clientId().equals(CLIENT_ID))
                .findFirst()
                .orElseThrow();
        assertEquals(SESSION.username(), entry.username());
        assertEquals(tracker.getPresenceState(CLIENT_ID).orElseThrow(), entry.state());
        System.out.println("[Presence demo] PRESENCE_SNAPSHOT " + entry.username() + "=" + entry.state());
        return entry.state();
    }

    private static void assertEventually(long timeoutMillis, java.util.function.BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), "Presence state did not change before timeout");
    }

    private static final class TestConnectionContext implements ConnectionContext {
        @Override
        public UUID connectionId() {
            return CONNECTION_ID;
        }

        @Override
        public Optional<SessionView> session() {
            return Optional.of(SESSION);
        }
    }

    private static final class TestSessionRegistry implements SessionRegistry {
        private final SessionView session;

        private TestSessionRegistry(SessionView session) {
            this.session = session;
        }

        @Override
        public Optional<SessionView> findByConnectionId(UUID id) {
            return session.connectionId().equals(id) ? Optional.of(session) : Optional.empty();
        }

        @Override
        public Optional<SessionView> findByClientId(UUID id) {
            return session.clientId().equals(id) ? Optional.of(session) : Optional.empty();
        }

        @Override
        public List<SessionView> activeSessions() {
            return List.of(session);
        }
    }

    private static final class CapturingSink implements OutboundSink {
        private OutboundFrame frame;

        @Override
        public EnqueueResult send(UUID connectionId, OutboundFrame outboundFrame) {
            frame = outboundFrame;
            return EnqueueResult.ACCEPTED;
        }
    }
}
