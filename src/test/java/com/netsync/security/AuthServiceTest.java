package com.netsync.security;

import com.netsync.common.SessionView;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuthServiceTest {
    @Test
    void successfulLoginCreatesSessionAndBindsPresenceToken() {
        Fixture fixture = new Fixture();
        UUID connectionId = UUID.randomUUID();

        AuthService.LoginResult result = fixture.auth.authenticate(
                connectionId, "alice", "demo-alice-pass".toCharArray());

        assertEquals(AuthService.Status.AUTHENTICATED, result.status());
        assertEquals(connectionId, result.session().connectionId());
        assertTrue(fixture.registry.isActive(result.session()));
        assertTrue(fixture.auth.isPresenceTokenValid(result.session().clientId(), result.presenceToken()));
        assertTrue(fixture.auth.isAuthenticatedConnection(connectionId));
        assertTrue(fixture.tokens.isValidToken(result.presenceToken()));
    }

    @Test
    void failedCredentialsAreGenericAndDoNotCreateSession() {
        Fixture fixture = new Fixture();
        UUID wrongPasswordConnection = UUID.randomUUID();
        UUID unknownUserConnection = UUID.randomUUID();

        AuthService.LoginResult wrongPassword = fixture.auth.authenticate(
                wrongPasswordConnection, "alice", "wrong-password".toCharArray());
        AuthService.LoginResult unknownUser = fixture.auth.authenticate(
                unknownUserConnection, "unknown", "wrong-password".toCharArray());

        assertEquals(AuthService.Status.AUTH_FAILED, wrongPassword.status());
        assertEquals(AuthService.Status.AUTH_FAILED, unknownUser.status());
        assertFalse(fixture.auth.isAuthenticatedConnection(wrongPasswordConnection));
        assertFalse(fixture.auth.isAuthenticatedConnection(unknownUserConnection));
        assertEquals(wrongPassword.status(), unknownUser.status());
    }

    @Test
    void rejectsNullEmptyMalformedAndOversizedCredentials() {
        Fixture fixture = new Fixture();

        assertEquals(AuthService.Status.AUTH_FAILED,
                fixture.auth.authenticate(UUID.randomUUID(), "alice", null).status());
        assertEquals(AuthService.Status.AUTH_FAILED,
                fixture.auth.authenticate(UUID.randomUUID(), "alice", new char[0]).status());
        assertEquals(AuthService.Status.AUTH_FAILED,
                fixture.auth.authenticate(UUID.randomUUID(), "alice", "x".repeat(129).toCharArray()).status());
        assertEquals(AuthService.Status.AUTH_FAILED,
                fixture.auth.authenticate(UUID.randomUUID(), "bad-name", "demo-alice-pass".toCharArray()).status());
        assertEquals(AuthService.Status.INVALID_STATE,
                fixture.auth.authenticate(null, "alice", "demo-alice-pass".toCharArray()).status());
    }

    @Test
    void duplicateActiveUsernameIsRejectedWithoutReplacingSession() {
        Fixture fixture = new Fixture();
        AuthService.LoginResult original = fixture.auth.authenticate(
                UUID.randomUUID(), "alice", "demo-alice-pass".toCharArray());

        AuthService.LoginResult duplicate = fixture.auth.authenticate(
                UUID.randomUUID(), "alice", "demo-alice-pass".toCharArray());

        assertTrue(original.authenticated());
        assertEquals(AuthService.Status.AUTH_FAILED, duplicate.status());
        assertEquals(1, fixture.registry.activeSessionCount());
        assertTrue(fixture.registry.isActive(original.session()));
    }

    @Test
    void connectionCannotAuthenticateTwice() {
        Fixture fixture = new Fixture();
        UUID connectionId = UUID.randomUUID();
        AuthService.LoginResult first = fixture.auth.authenticate(
                connectionId, "alice", "demo-alice-pass".toCharArray());

        AuthService.LoginResult second = fixture.auth.authenticate(
                connectionId, "bob", "demo-bob-pass".toCharArray());

        assertTrue(first.authenticated());
        assertEquals(AuthService.Status.INVALID_STATE, second.status());
        assertEquals("alice", fixture.registry.findByConnectionId(connectionId).orElseThrow().username());
    }

    @Test
    void rejectsInvalidTokenAndTokenForAnotherClient() {
        Fixture fixture = new Fixture();
        AuthService.LoginResult alice = fixture.auth.authenticate(
                UUID.randomUUID(), "alice", "demo-alice-pass".toCharArray());
        AuthService.LoginResult bob = fixture.auth.authenticate(
                UUID.randomUUID(), "bob", "demo-bob-pass".toCharArray());

        assertFalse(fixture.auth.isPresenceTokenValid(alice.session().clientId(), "malformed"));
        assertFalse(fixture.auth.isPresenceTokenValid(bob.session().clientId(), alice.presenceToken()));
        assertFalse(fixture.auth.isPresenceTokenValid(UUID.randomUUID(), alice.presenceToken()));
        assertTrue(fixture.auth.isPresenceTokenValid(alice.session().clientId(), alice.presenceToken()));
    }

    @Test
    void logoutRevokesSessionAndTokenAndIsIdempotent() {
        Fixture fixture = new Fixture();
        UUID connectionId = UUID.randomUUID();
        AuthService.LoginResult result = fixture.auth.authenticate(
                connectionId, "alice", "demo-alice-pass".toCharArray());

        assertTrue(fixture.auth.logout(connectionId));
        assertFalse(fixture.auth.logout(connectionId));
        assertFalse(fixture.registry.isActive(result.session()));
        assertFalse(fixture.auth.isPresenceTokenValid(result.session().clientId(), result.presenceToken()));
        assertFalse(fixture.auth.isAuthenticatedConnection(connectionId));
    }

    @Test
    void disconnectRevokesSessionAndAllowsReconnectWithNewIdentity() {
        Fixture fixture = new Fixture();
        UUID connectionId = UUID.randomUUID();
        AuthService.LoginResult first = fixture.auth.authenticate(
                connectionId, "alice", "demo-alice-pass".toCharArray());

        assertTrue(fixture.auth.disconnect(connectionId));
        assertFalse(fixture.auth.disconnect(connectionId));
        AuthService.LoginResult reconnected = fixture.auth.authenticate(
                UUID.randomUUID(), "alice", "demo-alice-pass".toCharArray());

        assertTrue(reconnected.authenticated());
        assertNotEquals(first.session().clientId(), reconnected.session().clientId());
        assertFalse(fixture.auth.isPresenceTokenValid(first.session().clientId(), first.presenceToken()));
    }

    @Test
    void expiredSessionTokenIsRejectedAndUsernameCanBeReused() {
        MutableClock clock = new MutableClock(Instant.parse("2026-11-20T12:00:00Z"));
        CredentialStore credentials = credentialStore();
        SessionRegistryImpl registry = new SessionRegistryImpl(clock, Duration.ofMinutes(1));
        SessionTokenGenerator tokens = new SessionTokenGenerator();
        AuthService auth = new AuthService(credentials, registry, tokens);
        AuthService.LoginResult first = auth.authenticate(
                UUID.randomUUID(), "alice", "demo-alice-pass".toCharArray());

        clock.advance(Duration.ofMinutes(1));

        assertFalse(auth.isPresenceTokenValid(first.session().clientId(), first.presenceToken()));
        AuthService.LoginResult next = auth.authenticate(
                UUID.randomUUID(), "alice", "demo-alice-pass".toCharArray());
        assertTrue(next.authenticated());
    }

    @Test
    void concurrentLoginsCreateIndependentSessionsAndTokens() throws Exception {
        Fixture fixture = new Fixture();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Callable<AuthService.LoginResult>> tasks = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                String username = i % 2 == 0 ? "alice" : "bob";
                tasks.add(() -> {
                    start.await();
                    return fixture.auth.authenticate(
                            UUID.randomUUID(), username,
                            (username.equals("alice") ? "demo-alice-pass" : "demo-bob-pass").toCharArray());
                });
            }
            List<Future<AuthService.LoginResult>> futures = tasks.stream().map(executor::submit).toList();
            start.countDown();
            List<AuthService.LoginResult> results = new ArrayList<>();
            for (Future<AuthService.LoginResult> future : futures) {
                results.add(future.get());
            }

            List<AuthService.LoginResult> successful = results.stream()
                    .filter(AuthService.LoginResult::authenticated).toList();
            assertEquals(2, successful.size());
            assertEquals(Set.of("alice", "bob"), successful.stream()
                    .map(result -> result.session().username()).collect(java.util.stream.Collectors.toSet()));
            assertEquals(2, fixture.registry.activeSessionCount());
            assertNotEquals(successful.get(0).presenceToken(), successful.get(1).presenceToken());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentAttemptsOnOneConnectionCanOnlyCreateOneSession() throws Exception {
        Fixture fixture = new Fixture();
        UUID connectionId = UUID.randomUUID();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<AuthService.LoginResult> first = executor.submit(() -> {
                start.await();
                return fixture.auth.authenticate(connectionId, "alice", "demo-alice-pass".toCharArray());
            });
            Future<AuthService.LoginResult> second = executor.submit(() -> {
                start.await();
                return fixture.auth.authenticate(connectionId, "bob", "demo-bob-pass".toCharArray());
            });
            start.countDown();

            AuthService.LoginResult firstResult = first.get();
            AuthService.LoginResult secondResult = second.get();
            long successful = List.of(firstResult, secondResult).stream()
                    .filter(AuthService.LoginResult::authenticated).count();

            assertEquals(1, successful);
            assertEquals(1, fixture.registry.activeSessionCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void disconnectCancelsAReservedLoginBeforeWorkerVerification() {
        Fixture fixture = new Fixture();
        UUID connectionId = UUID.randomUUID();

        assertEquals(AuthService.ReservationStatus.RESERVED,
                fixture.auth.reserveAuthentication(connectionId));
        assertFalse(fixture.auth.disconnect(connectionId));
        AuthService.LoginResult result = fixture.auth.authenticateReserved(
                connectionId, "alice", "demo-alice-pass".toCharArray());

        assertEquals(AuthService.Status.CANCELLED, result.status());
        assertFalse(fixture.auth.isAuthenticatedConnection(connectionId));
    }

    @Test
    void resultFormattingDoesNotRevealPresenceToken() {
        Fixture fixture = new Fixture();
        AuthService.LoginResult result = fixture.auth.authenticate(
                UUID.randomUUID(), "alice", "demo-alice-pass".toCharArray());

        assertFalse(result.toString().contains(result.presenceToken()));
    }

    private static CredentialStore credentialStore() {
        return new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray()),
                new CredentialStore.DemoAccountSpec("bob", "demo-bob-pass".toCharArray())));
    }

    private static final class Fixture {
        private final SessionRegistryImpl registry = new SessionRegistryImpl();
        private final SessionTokenGenerator tokens = new SessionTokenGenerator();
        private final AuthService auth = new AuthService(credentialStore(), registry, tokens);
    }

    private static final class MutableClock extends Clock {
        private Instant current;

        private MutableClock(Instant current) {
            this.current = current;
        }

        @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public synchronized Instant instant() { return current; }
        synchronized void advance(Duration duration) { current = current.plus(duration); }
    }
}
