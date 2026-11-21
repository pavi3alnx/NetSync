package com.netsync.security;

import com.netsync.common.Heartbeat;
import com.netsync.common.PresenceState;
import com.netsync.common.SessionView;
import com.netsync.presence.HeartbeatTracker;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecurityIntegrationTest {
    @Test
    void loginAuthorizesPresenceAndLogoutRevokesTokenAndPresence() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        AuthService auth = new AuthService(
                new CredentialStore(List.of(
                        new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray()))),
                registry,
                new SessionTokenGenerator());
        List<PresenceChange> changes = new ArrayList<>();
        HeartbeatTracker tracker = new HeartbeatTracker(
                System::nanoTime,
                (clientId, state) -> changes.add(new PresenceChange(clientId, state)),
                registry,
                auth::isPresenceTokenValid);

        AuthService.LoginResult login = auth.authenticate(
                UUID.randomUUID(), "alice", "demo-alice-pass".toCharArray());

        assertTrue(tracker.registerSession(login.session().clientId(), login.presenceToken()));
        assertTrue(tracker.processHeartbeat(new Heartbeat(
                1, "HEARTBEAT", login.session().clientId(), login.presenceToken(), 1)));
        assertEquals(Optional.of(PresenceState.ONLINE),
                tracker.getPresenceState(login.session().clientId()));

        assertTrue(auth.logout(login.session().connectionId()));
        tracker.revokeSession(login.session().clientId());

        assertFalse(auth.isPresenceTokenValid(login.session().clientId(), login.presenceToken()));
        assertFalse(tracker.processHeartbeat(new Heartbeat(
                1, "HEARTBEAT", login.session().clientId(), login.presenceToken(), 2)));
        assertTrue(tracker.getPresenceState(login.session().clientId()).isEmpty());
        assertEquals(List.of(
                new PresenceChange(login.session().clientId(), PresenceState.ONLINE),
                new PresenceChange(login.session().clientId(), PresenceState.OFFLINE)), changes);
    }

    @Test
    void udpHeartbeatTimeoutDoesNotRevokeTcpAuthentication() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        AuthService auth = new AuthService(
                new CredentialStore(List.of(
                        new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray()))),
                registry,
                new SessionTokenGenerator());
        MutableTimeSource time = new MutableTimeSource();
        HeartbeatTracker tracker = new HeartbeatTracker(
                time,
                (clientId, state) -> {},
                registry,
                auth::isPresenceTokenValid);
        AuthService.LoginResult login = auth.authenticate(
                UUID.randomUUID(), "alice", "demo-alice-pass".toCharArray());
        tracker.registerSession(login.session().clientId(), login.presenceToken());

        time.advanceSeconds(16);
        tracker.checkTimeouts();

        assertEquals(Optional.of(PresenceState.OFFLINE),
                tracker.getPresenceState(login.session().clientId()));
        assertTrue(auth.isAuthenticatedConnection(login.session().connectionId()));
        assertTrue(auth.isPresenceTokenValid(login.session().clientId(), login.presenceToken()));
    }

    @Test
    void authorizationAndActiveRecipientChecksUseRegistrySnapshots() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        AuthService auth = new AuthService(
                new CredentialStore(List.of(
                        new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray()),
                        new CredentialStore.DemoAccountSpec("bob", "demo-bob-pass".toCharArray()))),
                registry,
                new SessionTokenGenerator());
        AuthorizationServiceImpl authorization = new AuthorizationServiceImpl(registry);
        AuthService.LoginResult alice = auth.authenticate(
                UUID.randomUUID(), "alice", "demo-alice-pass".toCharArray());
        AuthService.LoginResult bob = auth.authenticate(
                UUID.randomUUID(), "bob", "demo-bob-pass".toCharArray());
        TestContext aliceContext = new TestContext(registry, alice.session().connectionId());

        assertTrue(authorization.authorizedChatSender(aliceContext, bob.session().clientId()).isPresent());
        assertTrue(authorization.canTransfer(aliceContext, bob.session().clientId()));

        auth.disconnect(bob.session().connectionId());
        assertTrue(authorization.authorizedChatSender(aliceContext, bob.session().clientId()).isEmpty());
        assertFalse(authorization.canTransfer(aliceContext, bob.session().clientId()));
    }

    private record PresenceChange(UUID clientId, PresenceState state) {}

    private record TestContext(SessionRegistryImpl registry, UUID connectionId)
            implements com.netsync.common.ConnectionContext {
        @Override public Optional<SessionView> session() {
            return registry.findByConnectionId(connectionId);
        }
    }

    private static final class MutableTimeSource implements HeartbeatTracker.TimeSource {
        private long nowNanos;

        @Override public synchronized long nanoTime() {
            return nowNanos;
        }

        private synchronized void advanceSeconds(long seconds) {
            nowNanos += TimeUnit.SECONDS.toNanos(seconds);
        }
    }
}
