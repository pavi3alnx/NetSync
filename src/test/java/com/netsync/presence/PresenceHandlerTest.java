package com.netsync.presence;

import com.netsync.common.AuthorizationService;
import com.netsync.common.ConnectionContext;
import com.netsync.common.EnqueueResult;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageCodec;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundFrame;
import com.netsync.common.OutboundSink;
import com.netsync.common.PresenceState;
import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PresenceHandlerTest {
    private static final UUID REQUEST_ID =
            UUID.fromString("550e8400-e29b-41d4-a716-446655440100");
    private static final UUID CONNECTION_ID =
            UUID.fromString("550e8400-e29b-41d4-a716-446655440011");
    private static final UUID ALICE_ID =
            UUID.fromString("550e8400-e29b-41d4-a716-446655440003");
    private static final UUID BOB_ID =
            UUID.fromString("550e8400-e29b-41d4-a716-446655440004");
    private static final UUID ALICE_CONNECTION =
            UUID.fromString("550e8400-e29b-41d4-a716-446655440012");
    private static final UUID BOB_CONNECTION =
            UUID.fromString("550e8400-e29b-41d4-a716-446655440013");
    private static final String ALICE_TOKEN =
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    private static final String BOB_TOKEN =
            "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";

    @Test
    void unauthenticatedRequestReturnsUnAuthenticatedError() throws Exception {
        FakeSessionRegistry registry = new FakeSessionRegistry();
        HeartbeatTracker tracker = tracker(registry);
        CapturingSink sink = new CapturingSink();
        PresenceHandler handler = new PresenceHandler(
                new AuthorizationService() {
                    @Override
                    public Optional<SessionView> authenticatedSession(ConnectionContext context) {
                        return Optional.empty();
                    }

                    @Override
                    public boolean isActive(SessionView session) {
                        return false;
                    }
                },
                registry,
                tracker,
                sink);

        handler.handle(context(CONNECTION_ID, Optional.empty()),
                presenceQuery(REQUEST_ID));

        MessageEnvelope response = sink.single();
        assertEquals(MessageType.ERROR, response.type());
        assertEquals(REQUEST_ID, response.replyTo());
        MessageBody.Error error = assertInstanceOf(MessageBody.Error.class, response.body());
        assertEquals(ErrorCode.UNAUTHENTICATED, error.code());
    }

    @Test
    void authenticatedRequestReturnsEmptySnapshot() throws Exception {
        FakeSessionRegistry registry = new FakeSessionRegistry();
        HeartbeatTracker tracker = tracker(registry);
        CapturingSink sink = new CapturingSink();
        SessionView requestingSession = new SessionView(ALICE_ID, "alice", ALICE_CONNECTION);
        registry.add(requestingSession);
        tracker.registerSession(ALICE_ID, ALICE_TOKEN);

        PresenceHandler handler = authenticatedHandler(registry, tracker, sink);
        handler.handle(context(ALICE_CONNECTION, Optional.of(requestingSession)),
                presenceQuery(REQUEST_ID));

        assertSnapshot(sink.single(), REQUEST_ID, 1);
    }

    @Test
    void snapshotIncludesOnlineAndOfflineActiveSessions() throws Exception {
        FakeSessionRegistry registry = new FakeSessionRegistry();
        SessionView alice = new SessionView(ALICE_ID, "alice", ALICE_CONNECTION);
        SessionView bob = new SessionView(BOB_ID, "bob", BOB_CONNECTION);
        registry.add(alice);
        registry.add(bob);

        MutableTimeSource time = new MutableTimeSource();
        HeartbeatTracker tracker = tracker(registry, time);
        tracker.registerSession(ALICE_ID, ALICE_TOKEN);
        tracker.registerSession(BOB_ID, BOB_TOKEN);

        time.advanceSeconds(15);
        tracker.checkTimeouts();

        CapturingSink sink = new CapturingSink();
        PresenceHandler handler = authenticatedHandler(registry, tracker, sink);
        handler.handle(context(ALICE_CONNECTION, Optional.of(alice)), presenceQuery(REQUEST_ID));

        MessageEnvelope response = sink.single();
        MessageBody.PresenceSnapshot snapshot =
                assertInstanceOf(MessageBody.PresenceSnapshot.class, response.body());
        assertEquals(2, snapshot.clients().size());

        MessageBody.PresenceEntry aliceEntry = find(snapshot, ALICE_ID);
        MessageBody.PresenceEntry bobEntry = find(snapshot, BOB_ID);
        assertEquals(PresenceState.OFFLINE, aliceEntry.state());
        assertEquals(PresenceState.OFFLINE, bobEntry.state());
    }

    @Test
    void snapshotIncludesOnlineStateAndNoTrackerEntryDefaultsOffline() throws Exception {
        FakeSessionRegistry registry = new FakeSessionRegistry();
        SessionView alice = new SessionView(ALICE_ID, "alice", ALICE_CONNECTION);
        SessionView bob = new SessionView(BOB_ID, "bob", BOB_CONNECTION);
        registry.add(alice);
        registry.add(bob);

        MutableTimeSource time = new MutableTimeSource();
        HeartbeatTracker tracker = tracker(registry, time);
        tracker.registerSession(ALICE_ID, ALICE_TOKEN);

        CapturingSink sink = new CapturingSink();
        PresenceHandler handler = authenticatedHandler(registry, tracker, sink);
        handler.handle(context(ALICE_CONNECTION, Optional.of(alice)), presenceQuery(REQUEST_ID));

        MessageEnvelope response = sink.single();
        MessageBody.PresenceSnapshot snapshot =
                assertInstanceOf(MessageBody.PresenceSnapshot.class, response.body());
        assertEquals(PresenceState.ONLINE, find(snapshot, ALICE_ID).state());
        assertEquals(PresenceState.OFFLINE, find(snapshot, BOB_ID).state());
    }

    @Test
    void disconnectedUserIsExcludedAndEmptyListIsSafe() throws Exception {
        FakeSessionRegistry registry = new FakeSessionRegistry();
        MutableTimeSource time = new MutableTimeSource();
        HeartbeatTracker tracker = tracker(registry, time);
        CapturingSink sink = new CapturingSink();
        SessionView requestingSession =
                new SessionView(ALICE_ID, "alice", ALICE_CONNECTION);
        registry.add(requestingSession);
        PresenceHandler handler = new PresenceHandler(
                new AuthorizationService() {
                    @Override
                    public Optional<SessionView> authenticatedSession(
                            ConnectionContext context) {
                        return Optional.of(requestingSession);
                    }

                    @Override
                    public boolean isActive(SessionView session) {
                        return true;
                    }
                },
                registry,
                tracker,
                sink);

        registry.remove(ALICE_ID);
        handler.handle(
                context(ALICE_CONNECTION, Optional.of(requestingSession)),
                presenceQuery(REQUEST_ID));

        assertSnapshot(sink.single(), REQUEST_ID, 0);
    }

    @Test
    void unexpectedMessageTypeReturnsInvalidRequest() {
        FakeSessionRegistry registry = new FakeSessionRegistry();
        HeartbeatTracker tracker = tracker(registry);
        CapturingSink sink = new CapturingSink();
        SessionView session = new SessionView(ALICE_ID, "alice", ALICE_CONNECTION);

        PresenceHandler handler = authenticatedHandler(registry, tracker, sink, session);
        handler.handle(
                context(ALICE_CONNECTION, Optional.of(session)),
                new MessageEnvelope(
                        1,
                        MessageType.CHAT_SEND,
                        REQUEST_ID,
                        null,
                        new MessageBody.ChatSend(BOB_ID, "hello")));

        MessageEnvelope response = sink.single();
        assertEquals(MessageType.ERROR, response.type());
        assertEquals(REQUEST_ID, response.replyTo());
        MessageBody.Error error = assertInstanceOf(MessageBody.Error.class, response.body());
        assertEquals(ErrorCode.INVALID_REQUEST, error.code());
    }

    @Test
    void malformedPresenceQueryBodyReturnsInvalidRequest() {
        FakeSessionRegistry registry = new FakeSessionRegistry();
        HeartbeatTracker tracker = tracker(registry);
        CapturingSink sink = new CapturingSink();
        SessionView session = new SessionView(ALICE_ID, "alice", ALICE_CONNECTION);
        PresenceHandler handler = authenticatedHandler(registry, tracker, sink, session);

        handler.handle(
                context(ALICE_CONNECTION, Optional.of(session)),
                new MessageEnvelope(
                        1,
                        MessageType.PRESENCE_QUERY,
                        REQUEST_ID,
                        null,
                        new MessageBody.ChatSend(BOB_ID, "not-a-query-body")));

        MessageEnvelope response = sink.single();
        assertEquals(MessageType.ERROR, response.type());
        assertEquals(REQUEST_ID, response.replyTo());
        MessageBody.Error error = assertInstanceOf(MessageBody.Error.class, response.body());
        assertEquals(ErrorCode.INVALID_REQUEST, error.code());
    }

    @Test
    void snapshotResponseWireFormatPreservesRequestId() throws Exception {
        FakeSessionRegistry registry = new FakeSessionRegistry();
        SessionView alice = new SessionView(ALICE_ID, "alice", ALICE_CONNECTION);
        registry.add(alice);
        HeartbeatTracker tracker = tracker(registry);
        tracker.registerSession(ALICE_ID, ALICE_TOKEN);
        CapturingSink sink = new CapturingSink();

        PresenceHandler handler = authenticatedHandler(registry, tracker, sink);
        handler.handle(context(ALICE_CONNECTION, Optional.of(alice)), presenceQuery(REQUEST_ID));

        MessageEnvelope response = sink.single();
        assertEquals(1, response.version());
        assertEquals(MessageType.PRESENCE_SNAPSHOT, response.type());
        assertEquals(REQUEST_ID, response.replyTo());

        MessageCodec codec = new MessageCodec();
        MessageEnvelope decoded = codec.decode(codec.encode(response));
        assertEquals(response, decoded);
    }

    private static PresenceHandler authenticatedHandler(
            FakeSessionRegistry registry,
            HeartbeatTracker tracker,
            CapturingSink sink) {
        return authenticatedHandler(
                registry,
                tracker,
                sink,
                new SessionView(ALICE_ID, "alice", ALICE_CONNECTION));
    }

    private static PresenceHandler authenticatedHandler(
            FakeSessionRegistry registry,
            HeartbeatTracker tracker,
            CapturingSink sink,
            SessionView session) {
        AuthorizationService authorization = new AuthorizationService() {
            @Override
            public Optional<SessionView> authenticatedSession(ConnectionContext context) {
                if (context.session().isEmpty()) {
                    return Optional.empty();
                }
                SessionView candidate = context.session().orElseThrow();
                return candidate.clientId().equals(session.clientId())
                        ? Optional.of(candidate)
                        : Optional.empty();
            }

            @Override
            public boolean isActive(SessionView candidate) {
                return registry.findByClientId(candidate.clientId()).isPresent();
            }
        };
        return new PresenceHandler(authorization, registry, tracker, sink);
    }

    private static HeartbeatTracker tracker(FakeSessionRegistry registry) {
        return tracker(registry, new MutableTimeSource());
    }

    private static HeartbeatTracker tracker(
            FakeSessionRegistry registry,
            MutableTimeSource time) {
        return new HeartbeatTracker(
                time,
                (clientId, state) -> { },
                registry,
                (clientId, token) -> ALICE_ID.equals(clientId) && ALICE_TOKEN.equals(token)
                        || BOB_ID.equals(clientId) && BOB_TOKEN.equals(token));
    }

    private static MessageEnvelope presenceQuery(UUID requestId) {
        return new MessageEnvelope(
                1,
                MessageType.PRESENCE_QUERY,
                requestId,
                null,
                new MessageBody.Empty());
    }

    private static ConnectionContext context(
            UUID connectionId,
            Optional<SessionView> session) {
        return new ConnectionContext() {
            @Override
            public UUID connectionId() {
                return connectionId;
            }

            @Override
            public Optional<SessionView> session() {
                return session;
            }
        };
    }

    private static void assertSnapshot(
            MessageEnvelope response,
            UUID requestId,
            int expectedSize) {
        assertEquals(MessageType.PRESENCE_SNAPSHOT, response.type());
        assertEquals(requestId, response.replyTo());
        MessageBody.PresenceSnapshot snapshot =
                assertInstanceOf(MessageBody.PresenceSnapshot.class, response.body());
        assertEquals(expectedSize, snapshot.clients().size());
    }

    private static MessageBody.PresenceEntry find(
            MessageBody.PresenceSnapshot snapshot,
            UUID clientId) {
        return snapshot.clients().stream()
                .filter(entry -> entry.clientId().equals(clientId))
                .findFirst()
                .orElseThrow();
    }

    private static final class CapturingSink implements OutboundSink {
        private final List<OutboundFrame> frames = new ArrayList<>();

        @Override
        public EnqueueResult send(UUID connectionId, OutboundFrame frame) {
            frames.add(frame);
            return EnqueueResult.ACCEPTED;
        }

        private MessageEnvelope single() {
            assertEquals(1, frames.size());
            return (MessageEnvelope) frames.getFirst();
        }
    }

    private static final class MutableTimeSource implements HeartbeatTracker.TimeSource {
        private long nanoTime;

        @Override
        public synchronized long nanoTime() {
            return nanoTime;
        }

        private synchronized void advanceSeconds(long seconds) {
            nanoTime += TimeUnit.SECONDS.toNanos(seconds);
        }
    }

    private static final class FakeSessionRegistry implements SessionRegistry {
        private final ConcurrentHashMap<UUID, SessionView> sessions =
                new ConcurrentHashMap<>();

        private void add(SessionView session) {
            sessions.put(session.clientId(), session);
        }

        private void remove(UUID clientId) {
            sessions.remove(clientId);
        }

        @Override
        public Optional<SessionView> findByConnectionId(UUID id) {
            return sessions.values().stream()
                    .filter(session -> session.connectionId().equals(id))
                    .findFirst();
        }

        @Override
        public Optional<SessionView> findByClientId(UUID id) {
            return Optional.ofNullable(sessions.get(id));
        }

        @Override
        public List<SessionView> activeSessions() {
            return List.copyOf(sessions.values());
        }
    }
}
