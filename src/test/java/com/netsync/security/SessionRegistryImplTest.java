package com.netsync.security;

import com.netsync.common.SessionView;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionRegistryImplTest {
    @Test
    void createsSessionAndGeneratesUniqueClientIdentity() {
        SessionRegistryImpl registry = new SessionRegistryImpl();

        SessionView first = registry.createSession(UUID.randomUUID(), "alice");
        SessionView second = registry.createSession(UUID.randomUUID(), "bob");

        assertEquals("alice", first.username());
        assertEquals("bob", second.username());
        assertNotEquals(first.clientId(), second.clientId());
        assertTrue(registry.isActive(first));
        assertEquals(2, registry.activeSessionCount());
    }

    @Test
    void rejectsDuplicateUsername() {
        SessionRegistryImpl registry = new SessionRegistryImpl();

        registry.createSession(UUID.randomUUID(), "alice");

        assertThrows(IllegalStateException.class,
                () -> registry.createSession(UUID.randomUUID(), "alice"));
        assertEquals(1, registry.activeSessionCount());
    }

    @Test
    void rejectsDuplicateConnectionAndInvalidUsernames() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        UUID connectionId = UUID.randomUUID();

        registry.createSession(connectionId, "alice");

        assertThrows(IllegalStateException.class,
                () -> registry.createSession(connectionId, "bob"));
        assertThrows(IllegalArgumentException.class,
                () -> registry.createSession(UUID.randomUUID(), ""));
        assertThrows(IllegalArgumentException.class,
                () -> registry.createSession(UUID.randomUUID(), "bad-name"));
        assertThrows(IllegalArgumentException.class,
                () -> registry.createSession(UUID.randomUUID(), "a".repeat(33)));
        assertThrows(IllegalArgumentException.class,
                () -> registry.createSession(null, "alice"));
    }

    @Test
    void sessionMutationMethodsAreNotPublic() {
        assertThrows(NoSuchMethodException.class,
                () -> SessionRegistryImpl.class.getMethod("createSession", UUID.class, String.class));
        assertThrows(NoSuchMethodException.class,
                () -> SessionRegistryImpl.class.getMethod(
                        "revokeByConnectionId", UUID.class));
        assertThrows(NoSuchMethodException.class,
                () -> SessionRegistryImpl.class.getMethod("revokeByClientId", UUID.class));
        assertThrows(NoSuchMethodException.class,
                () -> SessionRegistryImpl.class.getMethod("expireExpiredSessions"));
    }

    @Test
    void looksUpByConnectionAndClientId() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        UUID connectionId = UUID.randomUUID();
        SessionView session = registry.createSession(connectionId, "alice");

        assertEquals(Optional.of(session), registry.findByConnectionId(connectionId));
        assertEquals(Optional.of(session), registry.findByClientId(session.clientId()));
    }

    @Test
    void activeSessionsReturnsImmutableSnapshot() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        registry.createSession(UUID.randomUUID(), "alice");
        registry.createSession(UUID.randomUUID(), "bob");

        List<SessionView> sessions = registry.activeSessions();

        assertEquals(2, sessions.size());
        assertThrows(UnsupportedOperationException.class,
                () -> sessions.add(sessions.get(0)));
        assertEquals(2, registry.activeSessionCount());
    }

    @Test
    void revokesByConnectionIdAndMakesRepeatedCleanupIdempotent() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        UUID connectionId = UUID.randomUUID();
        SessionView session = registry.createSession(connectionId, "alice");

        assertTrue(registry.revokeByConnectionId(connectionId));
        assertFalse(registry.revokeByConnectionId(connectionId));
        assertFalse(registry.isActive(session));
        assertTrue(registry.findByConnectionId(connectionId).isEmpty());
        assertTrue(registry.findByClientId(session.clientId()).isEmpty());

        registry.createSession(connectionId, "alice");
        assertEquals(1, registry.activeSessionCount());
    }

    @Test
    void revokesByClientId() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        UUID connectionId = UUID.randomUUID();
        SessionView session = registry.createSession(connectionId, "alice");

        assertTrue(registry.revokeByClientId(session.clientId()));
        assertFalse(registry.revokeByClientId(session.clientId()));
        assertTrue(registry.findByConnectionId(connectionId).isEmpty());
        assertTrue(registry.findByClientId(session.clientId()).isEmpty());
    }

    @Test
    void expiresSessionsOnlyWhenConfiguredLifetimeElapses() {
        MutableClock clock = new MutableClock(Instant.parse("2026-11-20T12:00:00Z"));
        SessionRegistryImpl registry = new SessionRegistryImpl(clock, Duration.ofMinutes(5));
        UUID connectionId = UUID.randomUUID();
        SessionView session = registry.createSession(connectionId, "alice");

        clock.advance(Duration.ofMinutes(4));
        assertTrue(registry.isActive(session));
        assertEquals(0, registry.expireExpiredSessions());

        clock.advance(Duration.ofMinutes(1));
        assertEquals(1, registry.expireExpiredSessions());
        assertEquals(0, registry.activeSessionCount());
        assertTrue(registry.findByConnectionId(connectionId).isEmpty());
        assertTrue(registry.findByClientId(session.clientId()).isEmpty());

        registry.createSession(connectionId, "alice");
        assertEquals(1, registry.activeSessionCount());
    }

    @Test
    void defaultSessionsDoNotExpireAndReconnectionCreatesNewIdentity() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        UUID firstConnection = UUID.randomUUID();
        SessionView first = registry.createSession(firstConnection, "alice");

        assertTrue(registry.revokeByConnectionId(firstConnection));
        SessionView reconnected = registry.createSession(UUID.randomUUID(), "alice");

        assertNotEquals(first.clientId(), reconnected.clientId());
    }

    @Test
    void concurrentCreationUsesUniqueUsernamesAtomically() throws Exception {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        ExecutorService executor = Executors.newFixedThreadPool(16);
        try {
            List<Callable<Optional<SessionView>>> tasks = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                tasks.add(() -> {
                    try {
                        return Optional.of(registry.createSession(UUID.randomUUID(), "alice"));
                    } catch (IllegalStateException duplicate) {
                        return Optional.empty();
                    }
                });
            }

            int successful = 0;
            Set<UUID> clientIds = new HashSet<>();
            for (Future<Optional<SessionView>> future : executor.invokeAll(tasks)) {
                Optional<SessionView> session = future.get();
                if (session.isPresent()) {
                    successful++;
                    clientIds.add(session.get().clientId());
                }
            }

            assertEquals(1, successful);
            assertEquals(1, clientIds.size());
            assertEquals(1, registry.activeSessionCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentCreationProducesDistinctClientsAndConnections() throws Exception {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        ExecutorService executor = Executors.newFixedThreadPool(16);
        try {
            List<Callable<SessionView>> tasks = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                String username = "user_" + i;
                tasks.add(() -> registry.createSession(UUID.randomUUID(), username));
            }

            Set<UUID> clientIds = new HashSet<>();
            Set<UUID> connectionIds = new HashSet<>();
            for (Future<SessionView> future : executor.invokeAll(tasks)) {
                SessionView session = future.get();
                clientIds.add(session.clientId());
                connectionIds.add(session.connectionId());
            }

            assertEquals(50, clientIds.size());
            assertEquals(50, connectionIds.size());
            assertEquals(50, registry.activeSessionCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentLookupAndRemovalCannotLeaveStaleIndexes() throws Exception {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        List<SessionView> sessions = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            sessions.add(registry.createSession(UUID.randomUUID(), "user_" + i));
        }

        ExecutorService executor = Executors.newFixedThreadPool(16);
        AtomicInteger successfulRemovals = new AtomicInteger();
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (SessionView session : sessions) {
                tasks.add(() -> {
                    assertTrue(registry.findByConnectionId(session.connectionId()).isPresent()
                            || registry.findByConnectionId(session.connectionId()).isEmpty());
                    if (registry.revokeByConnectionId(session.connectionId())) {
                        successfulRemovals.incrementAndGet();
                    }
                    assertTrue(registry.findByConnectionId(session.connectionId()).isEmpty());
                    assertTrue(registry.findByClientId(session.clientId()).isEmpty());
                    return null;
                });
            }
            executor.invokeAll(tasks).forEach(future -> {
                try {
                    future.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            executor.shutdownNow();
        }

        assertEquals(100, successfulRemovals.get());
        assertEquals(0, registry.activeSessionCount());
        assertTrue(registry.activeSessions().isEmpty());
    }

    @Test
    void unknownAndNullIdentifiersAreSafe() {
        SessionRegistryImpl registry = new SessionRegistryImpl();

        assertTrue(registry.findByConnectionId(UUID.randomUUID()).isEmpty());
        assertTrue(registry.findByClientId(UUID.randomUUID()).isEmpty());
        assertTrue(registry.findByConnectionId(null).isEmpty());
        assertTrue(registry.findByClientId(null).isEmpty());
        assertFalse(registry.revokeByConnectionId(null));
        assertFalse(registry.revokeByClientId(null));
        assertFalse(registry.revokeByConnectionId(UUID.randomUUID()));
        assertFalse(registry.revokeByClientId(UUID.randomUUID()));
        assertFalse(registry.isActive(null));

        assertThrows(IllegalArgumentException.class,
                () -> new SessionRegistryImpl(Clock.systemUTC(), Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> new SessionRegistryImpl(Clock.systemUTC(), Duration.ofSeconds(-1)));
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        @Override
        public synchronized ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public synchronized Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public synchronized Instant instant() {
            return instant;
        }

        private synchronized void advance(Duration duration) {
            instant = instant.plus(duration);
        }
    }
}
