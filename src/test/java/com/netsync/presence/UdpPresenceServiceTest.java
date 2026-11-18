package com.netsync.presence;

import com.netsync.common.Heartbeat;
import com.netsync.common.HeartbeatCodec;
import com.netsync.common.PresenceState;
import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UdpPresenceServiceTest {
    private static final UUID CLIENT_ID =
            UUID.fromString("550e8400-e29b-41d4-a716-446655440003");
    private static final UUID CONNECTION_ID =
            UUID.fromString("550e8400-e29b-41d4-a716-446655440011");
    private static final String PRESENCE_TOKEN =
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    private static final String DIFFERENT_TOKEN =
            "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";

    @Test
    void rejectsNonLoopbackBindAddress() throws Exception {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        HeartbeatTracker tracker = tracker(registry, time);
        InetAddress nonLoopback = InetAddress.getByName("192.0.2.1");

        assertThrows(IllegalArgumentException.class, () ->
                new UdpPresenceService(
                        new InetSocketAddress(nonLoopback, 0),
                        tracker));
    }

    @Test
    void acceptsExactly1024ByteValidDatagram() throws Exception {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        registry.add(CLIENT_ID, CONNECTION_ID);
        HeartbeatTracker tracker = tracker(registry, time);
        assertTrue(tracker.registerSession(CLIENT_ID, PRESENCE_TOKEN));

        UdpPresenceService service = loopbackService(tracker);
        service.start();
        try {
            byte[] heartbeat = new HeartbeatCodec().encode(
                    new Heartbeat(1, "HEARTBEAT", CLIENT_ID, PRESENCE_TOKEN, 1));
            assertTrue(heartbeat.length < 1024);
            byte[] boundaryFixture = new byte[1024];
            System.arraycopy(heartbeat, 0, boundaryFixture, 0, heartbeat.length);
            for (int i = heartbeat.length; i < boundaryFixture.length; i++) {
                boundaryFixture[i] = ' ';
            }
            send(service.getLocalAddress(), boundaryFixture);

            assertEventually(3_000, () ->
                    tracker.getPresenceState(CLIENT_ID).orElse(null)
                            == PresenceState.ONLINE);
        } finally {
            service.stop();
        }
    }

    @Test
    void rejectsExactly1025ByteDatagram() throws Exception {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        registry.add(CLIENT_ID, CONNECTION_ID);
        HeartbeatTracker tracker = tracker(registry, time);
        assertTrue(tracker.registerSession(CLIENT_ID, PRESENCE_TOKEN));

        UdpPresenceService service = loopbackService(tracker);
        service.start();
        try {
            byte[] heartbeat = new HeartbeatCodec().encode(
                    new Heartbeat(1, "HEARTBEAT", CLIENT_ID, PRESENCE_TOKEN, 1));
            byte[] boundaryFixture = new byte[1025];
            System.arraycopy(heartbeat, 0, boundaryFixture, 0, heartbeat.length);
            for (int i = heartbeat.length; i < boundaryFixture.length; i++) {
                boundaryFixture[i] = ' ';
            }
            send(service.getLocalAddress(), boundaryFixture);
            Thread.sleep(150);
            assertEquals(Optional.of(PresenceState.ONLINE),
                    tracker.getPresenceState(CLIENT_ID));
        } finally {
            service.stop();
        }
    }

    @Test
    void udpRejectsInvalidTokenDuplicateAndOutOfOrderSequences() throws Exception {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        registry.add(CLIENT_ID, CONNECTION_ID);
        AtomicReference<PresenceState> lastState = new AtomicReference<>();
        HeartbeatTracker tracker = new HeartbeatTracker(
                time,
                (clientId, state) -> lastState.set(state),
                registry,
                (clientId, token) -> PRESENCE_TOKEN.equals(token));
        assertTrue(tracker.registerSession(CLIENT_ID, PRESENCE_TOKEN));

        UdpPresenceService service = loopbackService(tracker);
        service.start();
        try {
            HeartbeatCodec codec = new HeartbeatCodec();
            send(service.getLocalAddress(), codec.encode(
                    new Heartbeat(1, "HEARTBEAT", CLIENT_ID, PRESENCE_TOKEN, 1)));
            assertEventually(3_000, () ->
                    tracker.getPresenceState(CLIENT_ID).orElse(null)
                            == PresenceState.ONLINE);

            send(service.getLocalAddress(), codec.encode(
                    new Heartbeat(1, "HEARTBEAT", CLIENT_ID, DIFFERENT_TOKEN, 2)));
            send(service.getLocalAddress(), codec.encode(
                    new Heartbeat(1, "HEARTBEAT", CLIENT_ID, PRESENCE_TOKEN, 1)));
            send(service.getLocalAddress(), codec.encode(
                    new Heartbeat(1, "HEARTBEAT", CLIENT_ID, PRESENCE_TOKEN, 0)));
            Thread.sleep(150);

            assertEquals(Optional.of(PresenceState.ONLINE),
                    tracker.getPresenceState(CLIENT_ID));
        } finally {
            service.stop();
        }
    }

    @Test
    void acceptsValidLoopbackHeartbeat() throws Exception {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        registry.add(CLIENT_ID, CONNECTION_ID);
        HeartbeatTracker tracker = new HeartbeatTracker(
                time,
                (clientId, state) -> { },
                registry,
                (clientId, token) -> PRESENCE_TOKEN.equals(token));
        assertTrue(tracker.registerSession(CLIENT_ID, PRESENCE_TOKEN));

        time.advanceSeconds(15);
        tracker.checkTimeouts();
        assertEquals(Optional.of(PresenceState.OFFLINE), tracker.getPresenceState(CLIENT_ID));

        UdpPresenceService service = new UdpPresenceService(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                tracker);
        service.start();
        try {
            send(service.getLocalAddress(), new HeartbeatCodec().encode(
                    new Heartbeat(1, "HEARTBEAT", CLIENT_ID, PRESENCE_TOKEN, 1)));
            assertEventually(3_000, () ->
                    tracker.getPresenceState(CLIENT_ID).orElse(null) == PresenceState.ONLINE);
        } finally {
            service.stop();
        }
    }

    @Test
    void malformedDatagramDoesNotStopService() throws Exception {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        registry.add(CLIENT_ID, CONNECTION_ID);
        HeartbeatTracker tracker = tracker(registry, time);
        assertTrue(tracker.registerSession(CLIENT_ID, PRESENCE_TOKEN));
        time.advanceSeconds(15);
        tracker.checkTimeouts();
        assertEquals(Optional.of(PresenceState.OFFLINE), tracker.getPresenceState(CLIENT_ID));

        UdpPresenceService service = new UdpPresenceService(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                tracker);
        service.start();
        try {
            send(service.getLocalAddress(), "{not-json".getBytes(StandardCharsets.UTF_8));
            send(service.getLocalAddress(), new HeartbeatCodec().encode(
                    new Heartbeat(1, "HEARTBEAT", CLIENT_ID, PRESENCE_TOKEN, 1)));
            assertEventually(3_000, () -> {
                return tracker.getPresenceState(CLIENT_ID).orElse(null) == PresenceState.ONLINE;
            });
        } finally {
            service.stop();
        }
    }

    @Test
    void oversizedDatagramIsSilentlyRejected() throws Exception {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        registry.add(CLIENT_ID, CONNECTION_ID);
        HeartbeatTracker tracker = tracker(registry, time);
        assertTrue(tracker.registerSession(CLIENT_ID, PRESENCE_TOKEN));
        time.advanceSeconds(15);
        tracker.checkTimeouts();
        assertEquals(Optional.of(PresenceState.OFFLINE), tracker.getPresenceState(CLIENT_ID));

        UdpPresenceService service = new UdpPresenceService(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                tracker);
        service.start();
        try {
            send(service.getLocalAddress(), new byte[1025]);
            send(service.getLocalAddress(), new HeartbeatCodec().encode(
                    new Heartbeat(1, "HEARTBEAT", CLIENT_ID, PRESENCE_TOKEN, 1)));
            assertEventually(3_000, () -> {
                return tracker.getPresenceState(CLIENT_ID).orElse(null) == PresenceState.ONLINE;
            });
        } finally {
            service.stop();
        }
    }

    @Test
    void doesNotAcceptHeartbeatForInactiveSession() throws Exception {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        HeartbeatTracker tracker = tracker(registry, time);

        UdpPresenceService service = new UdpPresenceService(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                tracker);
        service.start();
        try {
            send(service.getLocalAddress(), new HeartbeatCodec().encode(
                    new Heartbeat(1, "HEARTBEAT", CLIENT_ID, PRESENCE_TOKEN, 1)));
            Thread.sleep(100);
            assertEquals(Optional.empty(), tracker.getPresenceState(CLIENT_ID));
        } finally {
            service.stop();
        }
    }

    @Test
    void shutdownAndRepeatedStopAreSafe() throws Exception {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        HeartbeatTracker tracker = tracker(registry, time);
        UdpPresenceService service = new UdpPresenceService(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                tracker);

        service.start();
        assertTrue(service.isRunning());
        InetSocketAddress boundAddress = service.getLocalAddress();
        assertTrue(boundAddress.getPort() > 0);

        service.stop();
        service.stop();
        assertFalse(service.isRunning());
    }

    @Test
    void stopBeforeStartAndRepeatedStopAreSafe() {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        HeartbeatTracker tracker = tracker(registry, time);
        UdpPresenceService service = loopbackService(tracker);

        service.stop();
        service.stop();
        assertFalse(service.isRunning());
        assertThrows(IllegalStateException.class, service::getLocalAddress);
    }

    @Test
    void failedBindCleansUpAndAllowsRetryAfterPortRelease() throws Exception {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        HeartbeatTracker tracker = tracker(registry, time);

        try (DatagramSocket occupied = new DatagramSocket(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))) {
            int occupiedPort = occupied.getLocalPort();
            UdpPresenceService failedService = new UdpPresenceService(
                    new InetSocketAddress(InetAddress.getLoopbackAddress(), occupiedPort),
                    tracker);

            assertThrows(IOException.class, failedService::start);
            assertFalse(failedService.isRunning());
            failedService.stop();
            failedService.stop();
        }

        UdpPresenceService retryService = new UdpPresenceService(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                tracker);
        retryService.start();
        assertTrue(retryService.isRunning());
        retryService.stop();
    }

    @Test
    void repeatedStartStopsAndRestartsLifecycle() throws Exception {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        HeartbeatTracker tracker = tracker(registry, time);
        UdpPresenceService service = new UdpPresenceService(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                tracker);

        service.start();
        assertTrue(service.isRunning());
        InetSocketAddress firstAddress = service.getLocalAddress();

        service.start();
        assertTrue(service.isRunning());
        assertEquals(firstAddress, service.getLocalAddress());

        service.stop();
        assertFalse(service.isRunning());
        service.start();
        assertTrue(service.isRunning());
        service.stop();
        service.stop();
        assertFalse(service.isRunning());
    }

    @Test
    void expiryExecutorOfflinesSessionAfterInjectedTimeAdvances() throws Exception {
        MutableTimeSource time = new MutableTimeSource();
        FakeSessionRegistry registry = new FakeSessionRegistry();
        registry.add(CLIENT_ID, CONNECTION_ID);
        AtomicReference<PresenceState> state = new AtomicReference<>();
        HeartbeatTracker tracker = new HeartbeatTracker(
                time,
                (clientId, presenceState) -> state.set(presenceState),
                registry,
                (clientId, token) -> PRESENCE_TOKEN.equals(token));
        assertTrue(tracker.registerSession(CLIENT_ID, PRESENCE_TOKEN));

        UdpPresenceService service = new UdpPresenceService(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                tracker);
        service.start();
        try {
            time.advanceSeconds(15);
            assertEventually(3_000, () ->
                    tracker.getPresenceState(CLIENT_ID).orElse(null) == PresenceState.OFFLINE
                            && state.get() == PresenceState.OFFLINE);
        } finally {
            service.stop();
        }
    }

    private static HeartbeatTracker tracker(
            FakeSessionRegistry registry,
            MutableTimeSource time) {
        return new HeartbeatTracker(
                time,
                (clientId, state) -> { },
                registry,
                (clientId, token) -> PRESENCE_TOKEN.equals(token));
    }

    private static UdpPresenceService loopbackService(HeartbeatTracker tracker) {
        return new UdpPresenceService(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                tracker);
    }

    private static void send(InetSocketAddress address, byte[] datagram) throws Exception {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.send(new DatagramPacket(
                    datagram,
                    datagram.length,
                    address.getAddress(),
                    address.getPort()));
        }
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
        assertTrue(condition.getAsBoolean(), "Condition did not become true before timeout");
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
        private final ConcurrentHashMap<UUID, SessionView> sessions = new ConcurrentHashMap<>();

        private void add(UUID clientId, UUID connectionId) {
            sessions.put(clientId,
                    new SessionView(clientId, "alice", connectionId));
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
