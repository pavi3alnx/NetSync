package com.netsync.security;

import com.netsync.common.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogoutHandlerTest {
    @Test
    void enqueuesCorrelatedAcknowledgementAndRevokesSessionAndToken() {
        Fixture fixture = new Fixture();
        AuthService.LoginResult login = fixture.authenticateAlice();
        UUID requestId = UUID.randomUUID();

        fixture.handler.handle(fixture.context(login.session().connectionId()), logout(requestId));

        MessageEnvelope response = fixture.outbound.last();
        assertEquals(MessageType.ACK, response.type());
        assertEquals(requestId, response.replyTo());
        MessageBody.Ack ack = (MessageBody.Ack) response.body();
        assertEquals(MessageType.LOGOUT_REQUEST, ack.operation());
        assertEquals(AckStatus.ACCEPTED, ack.status());
        assertFalse(fixture.auth.isAuthenticatedConnection(login.session().connectionId()));
        assertFalse(fixture.auth.isPresenceTokenValid(login.session().clientId(), login.presenceToken()));
        assertTrue(fixture.revoked.get());
    }

    @Test
    void rejectsUnauthenticatedLogout() {
        Fixture fixture = new Fixture();
        UUID requestId = UUID.randomUUID();

        fixture.handler.handle(fixture.context(UUID.randomUUID()), logout(requestId));

        MessageEnvelope response = fixture.outbound.last();
        assertEquals(MessageType.ERROR, response.type());
        assertEquals(requestId, response.replyTo());
        assertEquals(ErrorCode.UNAUTHENTICATED, ((MessageBody.Error) response.body()).code());
    }

    @Test
    void rejectsMalformedLogoutRequest() {
        Fixture fixture = new Fixture();
        UUID requestId = UUID.randomUUID();
        MessageEnvelope malformed = new MessageEnvelope(1, MessageType.LOGOUT_REQUEST,
                requestId, null, new MessageBody.LoginRequest("alice", "secret"));

        fixture.handler.handle(fixture.context(UUID.randomUUID()), malformed);

        assertEquals(ErrorCode.INVALID_REQUEST, ((MessageBody.Error) fixture.outbound.last().body()).code());
    }

    @Test
    void revokesSessionEvenWhenAcknowledgementCannotBeQueued() {
        Fixture fixture = new Fixture();
        AuthService.LoginResult login = fixture.authenticateAlice();
        fixture.outbound.result = EnqueueResult.REJECTED;

        fixture.handler.handle(fixture.context(login.session().connectionId()), logout(UUID.randomUUID()));

        assertFalse(fixture.auth.isAuthenticatedConnection(login.session().connectionId()));
        assertFalse(fixture.auth.isPresenceTokenValid(login.session().clientId(), login.presenceToken()));
        assertTrue(fixture.revoked.get());
    }

    @Test
    void repeatedLogoutCannotRevokeOrNotifyTwice() {
        Fixture fixture = new Fixture();
        AuthService.LoginResult login = fixture.authenticateAlice();

        fixture.handler.handle(fixture.context(login.session().connectionId()), logout(UUID.randomUUID()));
        fixture.handler.handle(fixture.context(login.session().connectionId()), logout(UUID.randomUUID()));

        assertEquals(1, fixture.revocationCount);
        assertEquals(ErrorCode.UNAUTHENTICATED, ((MessageBody.Error) fixture.outbound.last().body()).code());
    }

    private static MessageEnvelope logout(UUID requestId) {
        return new MessageEnvelope(1, MessageType.LOGOUT_REQUEST, requestId, null, new MessageBody.Empty());
    }

    private static final class Fixture {
        private final SessionRegistryImpl registry = new SessionRegistryImpl();
        private final AuthService auth = new AuthService(
                new CredentialStore(List.of(
                        new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray()))),
                registry,
                new SessionTokenGenerator());
        private final AuthorizationServiceImpl authorization = new AuthorizationServiceImpl(registry);
        private final FakeOutbound outbound = new FakeOutbound();
        private final AtomicBoolean revoked = new AtomicBoolean();
        private int revocationCount;
        private final LogoutHandler handler = new LogoutHandler(auth, authorization, outbound,
                connectionId -> true, session -> {
            revoked.set(true);
            revocationCount++;
        });

        private AuthService.LoginResult authenticateAlice() {
            return auth.authenticate(UUID.randomUUID(), "alice", "demo-alice-pass".toCharArray());
        }

        private ConnectionContext context(UUID connectionId) {
            return new TestContext(registry, connectionId);
        }
    }

    private record TestContext(SessionRegistryImpl registry, UUID connectionId) implements ConnectionContext {
        @Override public Optional<SessionView> session() {
            return registry.findByConnectionId(connectionId);
        }
    }

    private static final class FakeOutbound implements OutboundSink {
        private final List<OutboundFrame> frames = new ArrayList<>();
        private EnqueueResult result = EnqueueResult.ACCEPTED;

        @Override public EnqueueResult send(UUID connectionId, OutboundFrame frame) {
            frames.add(frame);
            return result;
        }

        private MessageEnvelope last() {
            return (MessageEnvelope) frames.get(frames.size() - 1);
        }
    }
}
