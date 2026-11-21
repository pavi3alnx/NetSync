package com.netsync.security;

import com.netsync.common.ConnectionContext;
import com.netsync.common.SessionView;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuthorizationServiceImplTest {
    @Test
    void acceptsAnAuthenticatedServerConnection() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        SessionView session = registry.createSession(UUID.randomUUID(), "alice");
        AuthorizationServiceImpl authorization = new AuthorizationServiceImpl(registry);

        assertTrue(authorization.authenticatedSession(new RegistryContext(registry, session.connectionId())).isPresent());
        assertTrue(authorization.isActive(session));
    }

    @Test
    void rejectsNullAndUnauthenticatedConnections() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        AuthorizationServiceImpl authorization = new AuthorizationServiceImpl(registry);

        assertTrue(authorization.authenticatedSession(null).isEmpty());
        assertTrue(authorization.authenticatedSession(new RegistryContext(registry, UUID.randomUUID())).isEmpty());
        assertFalse(authorization.isActive(null));
    }

    @Test
    void rejectsForgedOrStaleContextIdentity() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        SessionView alice = registry.createSession(UUID.randomUUID(), "alice");
        SessionView bob = registry.createSession(UUID.randomUUID(), "bob");
        AuthorizationServiceImpl authorization = new AuthorizationServiceImpl(registry);

        assertTrue(authorization.authenticatedSession(
                new FixedContext(alice.connectionId(), Optional.of(bob))).isEmpty());
        assertTrue(authorization.authenticatedSession(
                new FixedContext(alice.connectionId(), Optional.empty())).isEmpty());
        assertFalse(authorization.isActive(new SessionView(
                UUID.randomUUID(), alice.username(), alice.connectionId())));
        assertFalse(authorization.isActive(new SessionView(
                alice.clientId(), "forged", alice.connectionId())));
    }

    @Test
    void rejectsRevokedAndExpiredSessions() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        SessionView revoked = registry.createSession(UUID.randomUUID(), "alice");
        AuthorizationServiceImpl authorization = new AuthorizationServiceImpl(registry);

        registry.revokeByConnectionId(revoked.connectionId());

        assertTrue(authorization.authenticatedSession(new RegistryContext(registry, revoked.connectionId())).isEmpty());
        assertFalse(authorization.isActive(revoked));
    }

    @Test
    void allowsOnlyActiveSenderAndRecipientForChatAndTransfer() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        SessionView alice = registry.createSession(UUID.randomUUID(), "alice");
        SessionView bob = registry.createSession(UUID.randomUUID(), "bob");
        AuthorizationServiceImpl authorization = new AuthorizationServiceImpl(registry);
        ConnectionContext context = new RegistryContext(registry, alice.connectionId());

        assertTrue(authorization.authorizedChatSender(context, bob.clientId()).isPresent());
        assertTrue(authorization.canTransfer(context, bob.clientId()));
        assertTrue(authorization.authorizedChatSender(context, UUID.randomUUID()).isEmpty());
        assertFalse(authorization.canTransfer(context, UUID.randomUUID()));

        registry.revokeByClientId(bob.clientId());
        assertTrue(authorization.authorizedChatSender(context, bob.clientId()).isEmpty());
        assertFalse(authorization.canTransfer(context, bob.clientId()));
    }

    @Test
    void rejectsSenderAndRecipientIdentitySpoofing() {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        SessionView alice = registry.createSession(UUID.randomUUID(), "alice");
        SessionView bob = registry.createSession(UUID.randomUUID(), "bob");
        AuthorizationServiceImpl authorization = new AuthorizationServiceImpl(registry);
        ConnectionContext forgedAsBob = new FixedContext(alice.connectionId(), Optional.of(bob));

        assertTrue(authorization.authenticatedSession(forgedAsBob).isEmpty());
        assertTrue(authorization.authorizedChatSender(forgedAsBob, alice.clientId()).isEmpty());
        assertFalse(authorization.canTransfer(forgedAsBob, alice.clientId()));
    }

    @Test
    void concurrentAuthorizationAndRevocationRemainConsistent() throws Exception {
        SessionRegistryImpl registry = new SessionRegistryImpl();
        SessionView alice = registry.createSession(UUID.randomUUID(), "alice");
        SessionView bob = registry.createSession(UUID.randomUUID(), "bob");
        AuthorizationServiceImpl authorization = new AuthorizationServiceImpl(registry);
        ConnectionContext context = new RegistryContext(registry, alice.connectionId());
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<?> checks = executor.submit(() -> {
                start.await();
                for (int i = 0; i < 1_000; i++) {
                    authorization.authorizedChatSender(context, bob.clientId());
                }
                return null;
            });
            Future<?> revoke = executor.submit(() -> {
                start.await();
                registry.revokeByClientId(bob.clientId());
                return null;
            });
            start.countDown();
            checks.get();
            revoke.get();
            assertTrue(authorization.authorizedChatSender(context, bob.clientId()).isEmpty());
        } finally {
            executor.shutdownNow();
        }
    }

    private record RegistryContext(SessionRegistryImpl registry, UUID connectionId) implements ConnectionContext {
        @Override public Optional<SessionView> session() {
            return registry.findByConnectionId(connectionId);
        }
    }

    private record FixedContext(UUID connectionId, Optional<SessionView> session) implements ConnectionContext {}
}
