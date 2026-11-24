package com.netsync.presence;

import com.netsync.common.EnqueueResult;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundFrame;
import com.netsync.common.OutboundSink;
import com.netsync.common.PresenceState;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SelectorTasks;
import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PresenceEventPublisherTest {
    private static final UUID ALICE_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440003");
    private static final UUID BOB_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440004");
    private static final SessionView ALICE =
            new SessionView(ALICE_ID, "alice", UUID.fromString("550e8400-e29b-41d4-a716-446655440011"));
    private static final SessionView BOB =
            new SessionView(BOB_ID, "bob", UUID.fromString("550e8400-e29b-41d4-a716-446655440012"));

    @Test
    void queuesAndFormatsEventsForSelectorThread() throws Exception {
        TestSessionRegistry sessions = new TestSessionRegistry(ALICE, BOB);
        TestSelectorTasks selectorTasks = new TestSelectorTasks();
        CapturingSink outbound = new CapturingSink();
        PresenceEventPublisher publisher = new PresenceEventPublisher(selectorTasks, sessions, outbound);

        Thread udpThread = new Thread(
                () -> publisher.onPresenceChanged(ALICE_ID, PresenceState.ONLINE),
                "test-udp-thread");
        udpThread.start();
        udpThread.join();

        assertTrue(outbound.sent.isEmpty());
        assertEquals(1, selectorTasks.size());
        Thread selectorThread = new Thread(selectorTasks::runAll, "test-selector-thread");
        selectorThread.start();
        selectorThread.join();

        assertEquals(2, outbound.sent.size());
        assertTrue(outbound.sent.stream().allMatch(sent -> sent.thread() == selectorThread));
        assertEquals(
                List.of(ALICE.connectionId(), BOB.connectionId()).stream().sorted().toList(),
                outbound.sent.stream().map(SentFrame::connectionId).sorted().toList());
        for (SentFrame sent : outbound.sent) {
            MessageEnvelope event = assertInstanceOf(MessageEnvelope.class, sent.frame());
            assertEquals(ProtocolLimits.VERSION, event.version());
            assertEquals(MessageType.PRESENCE_UPDATE, event.type());
            assertNull(event.replyTo());
            MessageBody.PresenceUpdate update =
                    assertInstanceOf(MessageBody.PresenceUpdate.class, event.body());
            assertEquals(ALICE_ID, update.clientId());
            assertEquals("alice", update.username());
            assertEquals(PresenceState.ONLINE, update.state());
        }
    }

    @Test
    void disconnectedUserOfflineEventUsesCachedIdentityAndSkipsDisconnectedSession() {
        TestSessionRegistry sessions = new TestSessionRegistry(ALICE, BOB);
        TestSelectorTasks selectorTasks = new TestSelectorTasks();
        CapturingSink outbound = new CapturingSink();
        PresenceEventPublisher publisher = new PresenceEventPublisher(selectorTasks, sessions, outbound);

        publisher.onPresenceChanged(ALICE_ID, PresenceState.ONLINE);
        selectorTasks.runAll();
        outbound.sent.clear();
        sessions.remove(ALICE_ID);

        publisher.onPresenceChanged(ALICE_ID, PresenceState.OFFLINE);
        publisher.forgetSession(ALICE_ID);
        assertEquals(1, selectorTasks.size());
        selectorTasks.runAll();

        assertEquals(1, outbound.sent.size());
        SentFrame sent = outbound.sent.getFirst();
        assertEquals(BOB.connectionId(), sent.connectionId());
        MessageEnvelope event = assertInstanceOf(MessageEnvelope.class, sent.frame());
        MessageBody.PresenceUpdate update =
                assertInstanceOf(MessageBody.PresenceUpdate.class, event.body());
        assertEquals(ALICE_ID, update.clientId());
        assertEquals("alice", update.username());
        assertEquals(PresenceState.OFFLINE, update.state());

        publisher.onPresenceChanged(ALICE_ID, PresenceState.OFFLINE);
        assertEquals(0, selectorTasks.size());
    }

    @Test
    void canForgetSessionWhenLogoutHasNoNewOfflineTransition() {
        TestSessionRegistry sessions = new TestSessionRegistry(ALICE, BOB);
        TestSelectorTasks selectorTasks = new TestSelectorTasks();
        PresenceEventPublisher publisher =
                new PresenceEventPublisher(selectorTasks, sessions, new CapturingSink());

        publisher.onPresenceChanged(ALICE_ID, PresenceState.ONLINE);
        selectorTasks.runAll();
        sessions.remove(ALICE_ID);
        publisher.onPresenceChanged(ALICE_ID, PresenceState.OFFLINE);
        publisher.forgetSession(ALICE_ID);
        selectorTasks.runAll();

        assertEquals(0, selectorTasks.size());
    }

    @Test
    void logoutCancelsQueuedOnlineThatWasNeverPublished() {
        TestSessionRegistry sessions = new TestSessionRegistry(ALICE, BOB);
        TestSelectorTasks selectorTasks = new TestSelectorTasks();
        CapturingSink outbound = new CapturingSink();
        PresenceEventPublisher publisher = new PresenceEventPublisher(selectorTasks, sessions, outbound);
        publisher.onPresenceChanged(ALICE_ID, PresenceState.ONLINE);
        sessions.remove(ALICE_ID);
        publisher.forgetSession(ALICE_ID);
        selectorTasks.runAll();
        assertTrue(outbound.sent.isEmpty());
    }

    @Test
    void boundsRetainedEntriesAndRetriesAfterSelectorRejectsWork() {
        TestSessionRegistry sessions = new TestSessionRegistry(ALICE, BOB);
        TestSelectorTasks selectorTasks = new TestSelectorTasks();
        PresenceEventPublisher publisher = new PresenceEventPublisher(selectorTasks, sessions, new CapturingSink(), 1);
        assertTrue(publisher.registerSession(ALICE));
        assertFalse(publisher.registerSession(BOB));
        assertEquals(1, publisher.retainedSessions());
        selectorTasks.reject = true;
        publisher.onPresenceChanged(ALICE_ID, PresenceState.ONLINE);
        assertEquals(0, selectorTasks.size());
        selectorTasks.reject = false;
        publisher.retryPending();
        assertEquals(1, selectorTasks.size());
        selectorTasks.runAll();
    }

    @Test
    void newerOfflineStateSurvivesSelectorSubmissionRejection() {
        TestSessionRegistry sessions = new TestSessionRegistry(ALICE, BOB);
        TestSelectorTasks selectorTasks = new TestSelectorTasks();
        CapturingSink outbound = new CapturingSink();
        PresenceEventPublisher publisher = new PresenceEventPublisher(selectorTasks, sessions, outbound, 2);
        publisher.registerSession(ALICE);
        selectorTasks.reject = true;
        publisher.onPresenceChanged(ALICE_ID, PresenceState.ONLINE);
        publisher.onPresenceChanged(ALICE_ID, PresenceState.OFFLINE);
        selectorTasks.reject = false;
        publisher.retryPending();
        selectorTasks.runAll();
        assertEquals(2, outbound.sent.size());
        for (SentFrame sent : outbound.sent) {
            MessageEnvelope event = assertInstanceOf(MessageEnvelope.class, sent.frame());
            assertEquals(PresenceState.OFFLINE,
                    assertInstanceOf(MessageBody.PresenceUpdate.class, event.body()).state());
        }
    }

    @Test
    void logoutOrDisconnectDuringQueuedOnlineCancelsStalePublicationAndCleansUp() {
        TestSessionRegistry sessions = new TestSessionRegistry(ALICE, BOB);
        TestSelectorTasks selectorTasks = new TestSelectorTasks();
        CapturingSink outbound = new CapturingSink();
        PresenceEventPublisher publisher = new PresenceEventPublisher(selectorTasks, sessions, outbound);
        publisher.registerSession(ALICE);
        publisher.onPresenceChanged(ALICE_ID, PresenceState.ONLINE);
        sessions.remove(ALICE_ID);
        publisher.onPresenceChanged(ALICE_ID, PresenceState.OFFLINE);
        publisher.forgetSession(ALICE_ID);
        selectorTasks.runAll();
        assertTrue(outbound.sent.isEmpty());
        assertEquals(0, publisher.retainedSessions());
    }

    @Test
    void repeatedPublishedStateDoesNotScheduleDuplicateNotificationsAndCleanupIsComplete() {
        TestSessionRegistry sessions = new TestSessionRegistry(ALICE, BOB);
        TestSelectorTasks selectorTasks = new TestSelectorTasks();
        CapturingSink outbound = new CapturingSink();
        PresenceEventPublisher publisher = new PresenceEventPublisher(selectorTasks, sessions, outbound);
        publisher.registerSession(ALICE);
        publisher.onPresenceChanged(ALICE_ID, PresenceState.ONLINE);
        selectorTasks.runAll();
        int delivered = outbound.sent.size();
        publisher.onPresenceChanged(ALICE_ID, PresenceState.ONLINE);
        assertEquals(0, selectorTasks.size());
        sessions.remove(ALICE_ID);
        publisher.onPresenceChanged(ALICE_ID, PresenceState.OFFLINE);
        publisher.forgetSession(ALICE_ID);
        selectorTasks.runAll();
        assertEquals(delivered + 1, outbound.sent.size());
        assertEquals(0, publisher.retainedSessions());
    }

    @Test
    void closeClearsRejectedPendingTransitionsAndPreventsLaterRetry() {
        TestSessionRegistry sessions = new TestSessionRegistry(ALICE, BOB);
        TestSelectorTasks selectorTasks = new TestSelectorTasks();
        PresenceEventPublisher publisher = new PresenceEventPublisher(selectorTasks, sessions, new CapturingSink());
        publisher.registerSession(ALICE);
        selectorTasks.reject = true;
        publisher.onPresenceChanged(ALICE_ID, PresenceState.ONLINE);
        assertEquals(1, publisher.retainedSessions());
        publisher.close();
        selectorTasks.reject = false;
        publisher.retryPending();
        assertEquals(0, publisher.retainedSessions());
        assertEquals(0, selectorTasks.size());
    }

    private record SentFrame(UUID connectionId, OutboundFrame frame, Thread thread) {}

    private static final class CapturingSink implements OutboundSink {
        private final List<SentFrame> sent = new ArrayList<>();

        @Override
        public EnqueueResult send(UUID connectionId, OutboundFrame frame) {
            sent.add(new SentFrame(connectionId, frame, Thread.currentThread()));
            return EnqueueResult.ACCEPTED;
        }
    }

    private static final class TestSelectorTasks implements SelectorTasks {
        private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
        private volatile boolean reject;

        @Override
        public boolean submit(Runnable task) {
            if (reject) return false;
            return tasks.add(task);
        }

        @Override
        public Optional<Reservation> tryReserveCompletion() {
            return Optional.empty();
        }

        private int size() {
            return tasks.size();
        }

        private void runAll() {
            Runnable task;
            while ((task = tasks.poll()) != null) {
                task.run();
            }
        }
    }

    private static final class TestSessionRegistry implements SessionRegistry {
        private final ConcurrentHashMap<UUID, SessionView> sessions = new ConcurrentHashMap<>();

        private TestSessionRegistry(SessionView... initialSessions) {
            for (SessionView session : initialSessions) {
                sessions.put(session.clientId(), session);
            }
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
