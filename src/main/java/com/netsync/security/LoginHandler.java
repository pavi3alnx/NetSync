package com.netsync.security;

import com.netsync.common.ConnectionContext;
import com.netsync.common.ConnectionCloser;
import com.netsync.common.EnqueueResult;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageHandler;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundSink;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SelectorTasks;
import com.netsync.common.SessionView;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

/**
 * Asynchronous login adapter. Password verification runs on an injected bounded executor;
 * all response enqueueing and lifecycle callbacks run through the Selector task queue.
 */
public final class LoginHandler implements MessageHandler {
    private static final System.Logger LOG = System.getLogger(LoginHandler.class.getName());

    private final AuthService authService;
    private final Executor credentialWorkers;
    private final SelectorTasks selectorTasks;
    private final OutboundSink outboundSink;
    private final ConnectionCloser connectionCloser;
    private final Consumer<AuthService.LoginResult> onAuthenticated;
    private final Consumer<SessionView> onRevoked;
    private final Set<UUID> pendingConnections = new HashSet<>();
    private final Set<UUID> failedConnections = new HashSet<>();

    public LoginHandler(
            AuthService authService,
            Executor credentialWorkers,
            SelectorTasks selectorTasks,
            OutboundSink outboundSink,
            ConnectionCloser connectionCloser) {
        this(authService, credentialWorkers, selectorTasks, outboundSink,
                connectionCloser, result -> {}, session -> {});
    }

    public LoginHandler(
            AuthService authService,
            Executor credentialWorkers,
            SelectorTasks selectorTasks,
            OutboundSink outboundSink,
            ConnectionCloser connectionCloser,
            Consumer<AuthService.LoginResult> onAuthenticated,
            Consumer<SessionView> onRevoked) {
        this.authService = Objects.requireNonNull(authService);
        this.credentialWorkers = Objects.requireNonNull(credentialWorkers);
        this.selectorTasks = Objects.requireNonNull(selectorTasks);
        this.outboundSink = Objects.requireNonNull(outboundSink);
        this.connectionCloser = Objects.requireNonNull(connectionCloser);
        this.onAuthenticated = Objects.requireNonNull(onAuthenticated);
        this.onRevoked = Objects.requireNonNull(onRevoked);
    }

    @Override
    public void handle(ConnectionContext context, MessageEnvelope message) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(message);
        UUID connectionId = context.connectionId();

        if (message.type() != MessageType.LOGIN_REQUEST
                || !(message.body() instanceof MessageBody.LoginRequest request)) {
            sendError(connectionId, message.messageId(), ErrorCode.INVALID_REQUEST, "Invalid login request");
            return;
        }
        if (connectionId == null) {
            sendError(null, message.messageId(), ErrorCode.INVALID_STATE, "Invalid connection state");
            return;
        }
        if (authService.isAuthenticatedConnection(connectionId) || !markPending(connectionId)) {
            sendError(connectionId, message.messageId(), ErrorCode.INVALID_STATE, "Login is not allowed in this state");
            return;
        }

        AuthService.ReservationStatus loginReservation = authService.reserveAuthentication(connectionId);
        if (loginReservation != AuthService.ReservationStatus.RESERVED) {
            clearPending(connectionId);
            ErrorCode code = loginReservation == AuthService.ReservationStatus.BUSY
                    ? ErrorCode.BUSY : ErrorCode.INVALID_STATE;
            sendError(connectionId, message.messageId(), code,
                    code == ErrorCode.BUSY ? "Server is busy" : "Login is not allowed in this state");
            return;
        }

        var reservation = selectorTasks.tryReserveCompletion();
        if (reservation.isEmpty()) {
            authService.cancelAuthentication(connectionId);
            clearPending(connectionId);
            sendError(connectionId, message.messageId(), ErrorCode.BUSY, "Server is busy");
            return;
        }

        SelectorTasks.Reservation completion = reservation.get();
        try {
            credentialWorkers.execute(() -> verifyAndComplete(
                    message.messageId(), connectionId, request, completion));
        } catch (RejectedExecutionException rejected) {
            completion.close();
            authService.cancelAuthentication(connectionId);
            clearPending(connectionId);
            sendError(connectionId, message.messageId(), ErrorCode.BUSY, "Server is busy");
        }
    }

    private void verifyAndComplete(
            UUID requestId,
            UUID connectionId,
            MessageBody.LoginRequest request,
            SelectorTasks.Reservation completion) {
        char[] password = request.password() == null ? null : request.password().toCharArray();
        AuthService.LoginResult result;
        try {
            result = authService.authenticateReserved(connectionId, request.username(), password);
        } catch (RuntimeException failure) {
            try {
                completion.submit(() -> {
                    if (!clearPending(connectionId)) {
                        return;
                    }
                    failedConnections.add(connectionId);
                    sendError(connectionId, requestId, ErrorCode.AUTH_FAILED, "Authentication failed");
                    connectionCloser.closeAfterDrain(connectionId);
                });
            } catch (IllegalStateException unavailable) {
                completion.close();
                var session = authService.sessionForConnection(connectionId);
                if (authService.disconnect(connectionId)) {
                    session.ifPresent(onRevoked);
                }
                clearPending(connectionId);
                LOG.log(System.Logger.Level.DEBUG, "Login failure response was cancelled during shutdown");
            }
            LOG.log(System.Logger.Level.ERROR,
                    "Credential verification failed (" + failure.getClass().getName() + ")");
            return;
        } finally {
            if (password != null) {
                Arrays.fill(password, '\0');
            }
        }

        try {
            completion.submit(() -> complete(requestId, connectionId, result));
        } catch (IllegalStateException unavailable) {
            completion.close();
            if (authService.disconnect(connectionId) && result.authenticated()) {
                onRevoked.accept(result.session());
            }
            clearPending(connectionId);
            LOG.log(System.Logger.Level.DEBUG, "Login completion was cancelled during shutdown");
        }
    }

    private void complete(
            UUID requestId,
            UUID connectionId,
            AuthService.LoginResult result) {
        if (!clearPending(connectionId)) {
            if (result.authenticated() && authService.disconnect(connectionId)) {
                onRevoked.accept(result.session());
            }
            return;
        }
        switch (result.status()) {
            case AUTHENTICATED -> {
                try {
                    onAuthenticated.accept(result);
                } catch (RuntimeException failure) {
                    if (authService.disconnect(connectionId)) {
                        onRevoked.accept(result.session());
                    }
                    throw failure;
                }
                MessageEnvelope response = new MessageEnvelope(
                        ProtocolLimits.VERSION,
                        MessageType.LOGIN_RESPONSE,
                        UUID.randomUUID(),
                        requestId,
                        new MessageBody.LoginResponse(
                                result.session().clientId(),
                                result.session().username(),
                                result.presenceToken(),
                                ProtocolLimits.HEARTBEAT_INTERVAL_SECONDS,
                                ProtocolLimits.PRESENCE_TIMEOUT_SECONDS));
                if (outboundSink.send(connectionId, response) == EnqueueResult.REJECTED) {
                    if (authService.disconnect(connectionId)) {
                        onRevoked.accept(result.session());
                    }
                }
            }
            case AUTH_FAILED -> {
                failedConnections.add(connectionId);
                sendError(connectionId, requestId, ErrorCode.AUTH_FAILED, "Authentication failed");
                connectionCloser.closeAfterDrain(connectionId);
            }
            case INVALID_STATE -> sendError(connectionId, requestId, ErrorCode.INVALID_STATE,
                    "Login is not allowed in this state");
            case BUSY -> sendError(connectionId, requestId, ErrorCode.BUSY, "Server is busy");
            case CANCELLED -> { }
        }
    }

    private synchronized boolean markPending(UUID connectionId) {
        return !failedConnections.contains(connectionId) && pendingConnections.add(connectionId);
    }

    private synchronized boolean clearPending(UUID connectionId) {
        return pendingConnections.remove(connectionId);
    }

    /** Must be wired to the server's Selector-thread disconnect callback. */
    public synchronized void onDisconnected(UUID connectionId) {
        pendingConnections.remove(connectionId);
        failedConnections.remove(connectionId);
        var session = authService.sessionForConnection(connectionId);
        if (authService.disconnect(connectionId)) {
            session.ifPresent(onRevoked);
        }
    }

    private void sendError(UUID connectionId, UUID requestId, ErrorCode code, String message) {
        if (connectionId == null) {
            return;
        }
        outboundSink.send(connectionId, new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.ERROR,
                UUID.randomUUID(),
                requestId,
                new MessageBody.Error(code, message, null)));
    }
}
