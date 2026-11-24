package com.netsync.presence;

import com.netsync.common.Heartbeat;
import com.netsync.common.PresenceListener;
import com.netsync.common.PresenceState;
import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeartbeatTrackerTest {
    private static final UUID ALICE_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440003");
    private static final UUID BOB_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440004");
    private static final UUID ALICE_CONNECTION = UUID.fromString("550e8400-e29b-41d4-a716-446655440011");
    private static final UUID BOB_CONNECTION = UUID.fromString("550e8400-e29b-41d4-a716-446655440012");
    private static final String ALICE_TOKEN = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    private static final String BOB_TOKEN = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";

    private final MutableTimeSource time = new MutableTimeSource();
    private final FakeSessionRegistry registry = new FakeSessionRegistry();
    private final List<PresenceUpdate> updates = new ArrayList<>();
    private final HeartbeatTracker tracker = new HeartbeatTracker(
            time,
            (clientId, state) -> updates.add(new PresenceUpdate(clientId, state)),
            registry,
            (clientId, presenceToken) -> ALICE_ID.equals(clientId) && ALICE_TOKEN.equals(presenceToken)
                    || BOB_ID.equals(clientId) && BOB_TOKEN.equals(presenceToken));

    @Test
    void acceptsValidHeartbeat() {
        registry.add(ALICE_ID, ALICE_CONNECTION);

        assertTrue(tracker.registerSession(ALICE_ID, ALICE_TOKEN));
        assertTrue(tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, 1)));

        assertEquals(Optional.of(PresenceState.ONLINE), tracker.getPresenceState(ALICE_ID));
    }

    @Test
    void rejectsDuplicateDelayedAndOutOfOrderSequences() {
        registry.add(ALICE_ID, ALICE_CONNECTION);
        assertTrue(tracker.registerSession(ALICE_ID, ALICE_TOKEN));

        assertTrue(tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, 5)));
        assertFalse(tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, 5)));
        assertFalse(tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, 4)));
        assertFalse(tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, -1)));
    }

    @Test
    void marksInactiveSessionOfflineAfter15Seconds() {
        registry.add(ALICE_ID, ALICE_CONNECTION);
        assertTrue(tracker.registerSession(ALICE_ID, ALICE_TOKEN));
        assertTrue(tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, 1)));

        time.advanceSeconds(14);
        tracker.checkTimeouts();
        assertEquals(Optional.of(PresenceState.ONLINE), tracker.getPresenceState(ALICE_ID));

        time.advanceSeconds(1);
        tracker.checkTimeouts();
        assertEquals(Optional.of(PresenceState.OFFLINE), tracker.getPresenceState(ALICE_ID));
        assertTrue(updates.contains(new PresenceUpdate(ALICE_ID, PresenceState.OFFLINE)));
    }

    @Test
    void newerHeartbeatRestoresOfflineSession() {
        registry.add(ALICE_ID, ALICE_CONNECTION);
        assertTrue(tracker.registerSession(ALICE_ID, ALICE_TOKEN));

        time.advanceSeconds(15);
        tracker.checkTimeouts();
        assertEquals(Optional.of(PresenceState.OFFLINE), tracker.getPresenceState(ALICE_ID));

        assertTrue(tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, 1)));
        assertEquals(Optional.of(PresenceState.ONLINE), tracker.getPresenceState(ALICE_ID));
        assertTrue(updates.contains(new PresenceUpdate(ALICE_ID, PresenceState.ONLINE)));
    }

    @Test
    void repeatedTimeoutChecksNotifyOfflineExactlyOnceAndRecoveryNotifiesOnline() {
        registry.add(ALICE_ID, ALICE_CONNECTION);
        assertTrue(tracker.registerSession(ALICE_ID, ALICE_TOKEN));

        time.advanceSeconds(15);
        tracker.checkTimeouts();
        tracker.checkTimeouts();
        tracker.checkTimeouts();

        long offlineCount = updates.stream()
                .filter(update -> update.clientId().equals(ALICE_ID)
                        && update.state() == PresenceState.OFFLINE)
                .count();
        assertEquals(1, offlineCount);

        assertTrue(tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, 1)));
        assertTrue(updates.stream()
                .anyMatch(update -> update.clientId().equals(ALICE_ID)
                        && update.state() == PresenceState.ONLINE));
    }

    @Test
    void revokedSessionCannotReceiveHeartbeats() {
        registry.add(ALICE_ID, ALICE_CONNECTION);
        assertTrue(tracker.registerSession(ALICE_ID, ALICE_TOKEN));

        tracker.revokeSession(ALICE_ID);
        assertEquals(Optional.empty(), tracker.getPresenceState(ALICE_ID));
        assertFalse(tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, 1)));
    }

    @Test
    void tracksMultipleUsersIndependently() {
        registry.add(ALICE_ID, ALICE_CONNECTION);
        registry.add(BOB_ID, BOB_CONNECTION);
        assertTrue(tracker.registerSession(ALICE_ID, ALICE_TOKEN));
        assertTrue(tracker.registerSession(BOB_ID, BOB_TOKEN));

        assertTrue(tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, 1)));
        assertTrue(tracker.processHeartbeat(heartbeat(BOB_ID, BOB_TOKEN, 1)));
        time.advanceSeconds(14);
        assertTrue(tracker.processHeartbeat(heartbeat(BOB_ID, BOB_TOKEN, 2)));
        time.advanceSeconds(1);
        tracker.checkTimeouts();

        assertEquals(Optional.of(PresenceState.OFFLINE), tracker.getPresenceState(ALICE_ID));
        assertEquals(Optional.of(PresenceState.ONLINE), tracker.getPresenceState(BOB_ID));
    }

    @Test
    void rejectsInvalidOrInactiveTokens() {
        registry.add(ALICE_ID, ALICE_CONNECTION);
        assertFalse(tracker.registerSession(ALICE_ID, "invalid"));
        assertFalse(tracker.processHeartbeat(heartbeat(ALICE_ID, "invalid", 1)));

        assertTrue(tracker.registerSession(ALICE_ID, ALICE_TOKEN));
        registry.remove(ALICE_ID);
        assertFalse(tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, 1)));
    }

    @Test
    void concurrentRevokeAndHeartbeatNeverRevivesRevokedSession() throws Exception {
        registry.add(ALICE_ID, ALICE_CONNECTION);
        assertTrue(tracker.registerSession(ALICE_ID, ALICE_TOKEN));

        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            var futures = java.util.stream.LongStream.range(1, 101)
                    .mapToObj(sequence -> executor.submit(() -> {
                        try {
                            start.await();
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        tracker.processHeartbeat(
                                heartbeat(ALICE_ID, ALICE_TOKEN, sequence));
                    }))
                    .toList();
            executor.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
                tracker.revokeSession(ALICE_ID);
            });

            start.countDown();
            for (var future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals(Optional.empty(), tracker.getPresenceState(ALICE_ID));
        assertFalse(tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, 101)));
    }

    @Test
    void heartbeatRecoveryRacingWithExpiryLeavesTheFreshHeartbeatAuthoritative() throws Exception {
        registry.add(ALICE_ID, ALICE_CONNECTION);
        assertTrue(tracker.registerSession(ALICE_ID, ALICE_TOKEN));
        time.advanceSeconds(15);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var expiry = executor.submit(() -> { await(start); tracker.checkTimeouts(); });
            var recovery = executor.submit(() -> { await(start); return tracker.processHeartbeat(heartbeat(ALICE_ID, ALICE_TOKEN, 1)); });
            start.countDown();
            expiry.get(2, TimeUnit.SECONDS);
            assertTrue(recovery.get(2, TimeUnit.SECONDS));
        } finally { executor.shutdownNow(); }
        assertEquals(Optional.of(PresenceState.ONLINE), tracker.getPresenceState(ALICE_ID));
        long onlineCount = updates.stream().filter(update -> update.clientId().equals(ALICE_ID)
                && update.state() == PresenceState.ONLINE).count();
        assertTrue(onlineCount >= 1);
        assertEquals(PresenceState.ONLINE, updates.getLast().state());
    }

    private static void await(CountDownLatch latch) {
        try { latch.await(); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
    }

    private static Heartbeat heartbeat(UUID clientId, String token, long sequence) {
        return new Heartbeat(1, "HEARTBEAT", clientId, token, sequence);
    }

    private static final class MutableTimeSource implements HeartbeatTracker.TimeSource {
        private long nanoTime;

        @Override
        public long nanoTime() {
            return nanoTime;
        }

        private void advanceSeconds(long seconds) {
            nanoTime += TimeUnit.SECONDS.toNanos(seconds);
        }
    }

    private static final class FakeSessionRegistry implements SessionRegistry {
        private final Map<UUID, SessionView> sessions = new ConcurrentHashMap<>();

        private void add(UUID clientId, UUID connectionId) {
            sessions.put(clientId, new SessionView(clientId, clientId.equals(ALICE_ID) ? "alice" : "bob", connectionId));
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

    private record PresenceUpdate(UUID clientId, PresenceState state) {}
}
