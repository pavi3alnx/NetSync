package com.netsync.security;

import com.netsync.common.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LoginHandlerTest {
    @Test
    void verifiesCredentialsOffHandlerCallAndReturnsCorrelatedLoginResponse() {
        Fixture fixture = new Fixture();
        UUID connectionId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();

        fixture.handler.handle(fixture.context(connectionId), login(requestId, "alice", "demo-alice-pass"));

        assertEquals(1, fixture.worker.tasks.size());
        assertTrue(fixture.outbound.frames.isEmpty());
        fixture.worker.runNext();
        assertEquals(1, fixture.selector.tasks.size());
        fixture.selector.runNext();

        MessageEnvelope response = fixture.outbound.last();
        assertEquals(MessageType.LOGIN_RESPONSE, response.type());
        assertEquals(requestId, response.replyTo());
        MessageBody.LoginResponse body = (MessageBody.LoginResponse) response.body();
        assertEquals(5, body.heartbeatIntervalSeconds());
        assertEquals(15, body.presenceTimeoutSeconds());
        assertTrue(fixture.auth.isPresenceTokenValid(body.clientId(), body.presenceToken()));
        assertTrue(fixture.authenticatedCallback.get());
    }

    @Test
    void invalidCredentialsReturnGenericAuthenticationFailure() {
        Fixture fixture = new Fixture();
        UUID connectionId = UUID.randomUUID();
        // Deliberately contains the submitted password substring. The envelope's
        // generated record string includes replyTo, so scanning that whole string
        // would produce a false positive unrelated to the error payload.
        UUID requestId = UUID.fromString("00000000-0000-0000-0000-00000000bad0");

        fixture.handler.handle(fixture.context(connectionId), login(requestId, "unknown", "bad"));
        fixture.worker.runNext();
        fixture.selector.runNext();

        MessageEnvelope response = fixture.outbound.last();
        assertEquals(MessageType.ERROR, response.type());
        assertEquals(requestId, response.replyTo());
        MessageBody.Error error = (MessageBody.Error) response.body();
        assertEquals(ErrorCode.AUTH_FAILED, error.code());
        assertEquals("Authentication failed", error.message());
        assertNull(error.transferId());
        assertFalse(error.message().contains("bad"));
        assertTrue(response.toString().contains("bad"), "The deterministic correlation UUID reproduces the old false positive");
        assertFalse(fixture.auth.isAuthenticatedConnection(connectionId));
        assertTrue(fixture.auth.sessionForConnection(connectionId).isEmpty());
        assertTrue(fixture.registry.activeSessions().isEmpty());
        assertFalse(fixture.auth.isPresenceTokenValid(UUID.fromString("00000000-0000-0000-0000-00000000bad0"), "bad"));
    }

    @Test
    void rejectsSecondLoginAndConcurrentPendingLogin() {
        Fixture fixture = new Fixture();
        UUID connectionId = UUID.randomUUID();

        fixture.handler.handle(fixture.context(connectionId),
                login(UUID.randomUUID(), "alice", "demo-alice-pass"));
        fixture.handler.handle(fixture.context(connectionId),
                login(UUID.randomUUID(), "bob", "demo-bob-pass"));

        assertEquals(1, fixture.worker.tasks.size());
        assertEquals(ErrorCode.INVALID_STATE, ((MessageBody.Error) fixture.outbound.last().body()).code());

        fixture.worker.runNext();
        fixture.selector.runNext();
        fixture.handler.handle(fixture.context(connectionId),
                login(UUID.randomUUID(), "bob", "demo-bob-pass"));
        assertEquals(ErrorCode.INVALID_STATE, ((MessageBody.Error) fixture.outbound.last().body()).code());
    }

    @Test
    void disconnectBeforeWorkerStartsCancelsPendingAuthentication() {
        Fixture fixture = new Fixture();
        UUID connectionId = UUID.randomUUID();

        fixture.handler.handle(fixture.context(connectionId),
                login(UUID.randomUUID(), "alice", "demo-alice-pass"));
        fixture.handler.onDisconnected(connectionId);
        fixture.worker.runNext();
        fixture.selector.runNext();

        assertFalse(fixture.auth.isAuthenticatedConnection(connectionId));
        assertTrue(fixture.outbound.frames.isEmpty());
    }

    @Test
    void failedLoginCannotBeRetriedOnSameConnection() {
        Fixture fixture = new Fixture();
        UUID connectionId = UUID.randomUUID();

        fixture.handler.handle(fixture.context(connectionId),
                login(UUID.randomUUID(), "alice", "wrong"));
        fixture.worker.runNext();
        fixture.selector.runNext();
        fixture.handler.handle(fixture.context(connectionId),
                login(UUID.randomUUID(), "alice", "demo-alice-pass"));

        assertTrue(fixture.worker.tasks.isEmpty());
        assertEquals(ErrorCode.INVALID_STATE, ((MessageBody.Error) fixture.outbound.last().body()).code());
    }

    @Test
    void returnsBusyWhenCompletionCapacityOrWorkerCapacityIsUnavailable() {
        Fixture noCompletionCapacity = new Fixture();
        noCompletionCapacity.selector.rejectReservations = true;
        UUID firstConnection = UUID.randomUUID();
        noCompletionCapacity.handler.handle(noCompletionCapacity.context(firstConnection),
                login(UUID.randomUUID(), "alice", "demo-alice-pass"));
        assertEquals(ErrorCode.BUSY,
                ((MessageBody.Error) noCompletionCapacity.outbound.last().body()).code());
        assertTrue(noCompletionCapacity.worker.tasks.isEmpty());

        Fixture noWorkerCapacity = new Fixture();
        noWorkerCapacity.worker.reject = true;
        noWorkerCapacity.handler.handle(noWorkerCapacity.context(UUID.randomUUID()),
                login(UUID.randomUUID(), "alice", "demo-alice-pass"));
        assertEquals(ErrorCode.BUSY,
                ((MessageBody.Error) noWorkerCapacity.outbound.last().body()).code());
        assertTrue(noWorkerCapacity.selector.tasks.isEmpty());
    }

    @Test
    void rejectedResponseRevokesSessionAndNotifiesPresenceCleanup() {
        Fixture fixture = new Fixture();
        fixture.outbound.result = EnqueueResult.REJECTED;
        UUID connectionId = UUID.randomUUID();

        fixture.handler.handle(fixture.context(connectionId), login(UUID.randomUUID(), "alice", "demo-alice-pass"));
        fixture.worker.runNext();
        fixture.selector.runNext();

        assertFalse(fixture.auth.isAuthenticatedConnection(connectionId));
        assertTrue(fixture.revoked.get());
    }

    @Test
    void malformedHandlerRequestGetsSafeInvalidRequestError() {
        Fixture fixture = new Fixture();
        UUID requestId = UUID.randomUUID();
        MessageEnvelope wrongType = new MessageEnvelope(1, MessageType.LOGOUT_REQUEST,
                requestId, null, new MessageBody.Empty());

        fixture.handler.handle(fixture.context(UUID.randomUUID()), wrongType);

        assertEquals(ErrorCode.INVALID_REQUEST, ((MessageBody.Error) fixture.outbound.last().body()).code());
        assertEquals(requestId, fixture.outbound.last().replyTo());
    }

    private static MessageEnvelope login(UUID requestId, String username, String password) {
        return new MessageEnvelope(1, MessageType.LOGIN_REQUEST, requestId, null,
                new MessageBody.LoginRequest(username, password));
    }

    private static final class Fixture {
        private final SessionRegistryImpl registry = new SessionRegistryImpl();
        private final AuthService auth = new AuthService(
                new CredentialStore(List.of(
                        new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray()),
                        new CredentialStore.DemoAccountSpec("bob", "demo-bob-pass".toCharArray()))),
                registry,
                new SessionTokenGenerator());
        private final QueuedExecutor worker = new QueuedExecutor();
        private final FakeSelectorTasks selector = new FakeSelectorTasks();
        private final FakeOutbound outbound = new FakeOutbound();
        private final java.util.concurrent.atomic.AtomicBoolean authenticatedCallback =
                new java.util.concurrent.atomic.AtomicBoolean();
        private final java.util.concurrent.atomic.AtomicBoolean revoked =
                new java.util.concurrent.atomic.AtomicBoolean();
        private final LoginHandler handler = new LoginHandler(auth, worker, selector, outbound,
                connectionId -> true,
                result -> authenticatedCallback.set(true), session -> revoked.set(true));

        private ConnectionContext context(UUID connectionId) {
            return new TestContext(registry, connectionId);
        }
    }

    private record TestContext(SessionRegistryImpl registry, UUID connectionId) implements ConnectionContext {
        @Override public Optional<SessionView> session() {
            return registry.findByConnectionId(connectionId);
        }
    }

    private static final class QueuedExecutor implements Executor {
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        private boolean reject;

        @Override public void execute(Runnable command) {
            if (reject) throw new RejectedExecutionException();
            tasks.add(command);
        }

        private void runNext() {
            tasks.remove().run();
        }
    }

    private static final class FakeSelectorTasks implements SelectorTasks {
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        private boolean rejectReservations;

        @Override public boolean submit(Runnable task) {
            tasks.add(task);
            return true;
        }

        @Override public Optional<Reservation> tryReserveCompletion() {
            if (rejectReservations) return Optional.empty();
            return Optional.of(new Reservation() {
                private boolean submitted;

                @Override public void submit(Runnable completion) {
                    if (submitted) throw new IllegalStateException();
                    submitted = true;
                    tasks.add(completion);
                }

                @Override public void close() {
                    submitted = true;
                }
            });
        }

        private void runNext() {
            tasks.remove().run();
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
