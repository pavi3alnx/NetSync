package com.netsync.security;

import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionView;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Coordinates credential verification and the authoritative session/token lifecycle.
 * Call {@link #authenticate(UUID, String, char[])} from a bounded worker, never from
 * the NIO Selector thread, because credential verification uses PBKDF2.
 */
public final class AuthService {
    public enum Status {
        AUTHENTICATED,
        AUTH_FAILED,
        INVALID_STATE,
        BUSY,
        CANCELLED
    }

    public enum ReservationStatus {
        RESERVED,
        INVALID_STATE,
        BUSY
    }

    public record LoginResult(Status status, SessionView session, String presenceToken) {
        public LoginResult {
            Objects.requireNonNull(status);
            if (status == Status.AUTHENTICATED
                    && (session == null || presenceToken == null)) {
                throw new IllegalArgumentException("Successful authentication requires a session");
            }
            if (status != Status.AUTHENTICATED && (session != null || presenceToken != null)) {
                throw new IllegalArgumentException("Failed authentication must not include session data");
            }
        }

        public boolean authenticated() {
            return status == Status.AUTHENTICATED;
        }

        @Override
        public String toString() {
            return "LoginResult[status=" + status + ", session="
                    + (session == null ? "none" : "present")
                    + ", presenceToken=REDACTED]";
        }
    }

    private final Object lifecycleLock = new Object();
    private final CredentialStore credentialStore;
    private final SessionRegistryImpl sessionRegistry;
    private final SessionTokenGenerator tokenGenerator;
    private final Set<UUID> pendingConnections = new HashSet<>();

    public AuthService(
            CredentialStore credentialStore,
            SessionRegistryImpl sessionRegistry,
            SessionTokenGenerator tokenGenerator) {
        this.credentialStore = Objects.requireNonNull(credentialStore);
        this.sessionRegistry = Objects.requireNonNull(sessionRegistry);
        this.tokenGenerator = Objects.requireNonNull(tokenGenerator);
    }

    /**
     * Verifies credentials off-selector, then atomically creates the session and
     * associates its token with that same authoritative session record.
     */
    public LoginResult authenticate(UUID connectionId, String username, char[] password) {
        ReservationStatus reservation = reserveAuthentication(connectionId);
        if (reservation != ReservationStatus.RESERVED) {
            return failure(reservation == ReservationStatus.BUSY ? Status.BUSY : Status.INVALID_STATE);
        }
        return authenticateReserved(connectionId, username, password);
    }

    /** Reserves one bounded login slot before a worker task is submitted. */
    public ReservationStatus reserveAuthentication(UUID connectionId) {
        if (connectionId == null) {
            return ReservationStatus.INVALID_STATE;
        }
        synchronized (lifecycleLock) {
            if (sessionRegistry.findByConnectionId(connectionId).isPresent()
                    || pendingConnections.contains(connectionId)) {
                return ReservationStatus.INVALID_STATE;
            }
            if (pendingConnections.size() >= ProtocolLimits.MAX_CONNECTIONS) {
                return ReservationStatus.BUSY;
            }
            pendingConnections.add(connectionId);
        }
        return ReservationStatus.RESERVED;
    }

    /** Performs PBKDF2 for a previously reserved login; invoke only on a worker thread. */
    public LoginResult authenticateReserved(UUID connectionId, String username, char[] password) {
        synchronized (lifecycleLock) {
            if (connectionId == null || !pendingConnections.contains(connectionId)) {
                return failure(Status.CANCELLED);
            }
        }
        char[] passwordCopy = password == null ? null : Arrays.copyOf(password, password.length);
        try {
            if (!credentialStore.verify(username, passwordCopy)) {
                return failure(Status.AUTH_FAILED);
            }

            synchronized (lifecycleLock) {
                if (!pendingConnections.remove(connectionId)) {
                    return failure(Status.CANCELLED);
                }
                if (sessionRegistry.findByConnectionId(connectionId).isPresent()) {
                    return failure(Status.INVALID_STATE);
                }
                String presenceToken = tokenGenerator.generateToken();
                try {
                    SessionView session = sessionRegistry.createSession(
                            connectionId, username, presenceToken);
                    return new LoginResult(Status.AUTHENTICATED, session, presenceToken);
                } catch (SessionRegistryImpl.SessionConflictException conflict) {
                    return failure(conflict.connectionConflict()
                            ? Status.INVALID_STATE : Status.AUTH_FAILED);
                }
            }
        } finally {
            if (passwordCopy != null) {
                Arrays.fill(passwordCopy, '\0');
            }
            synchronized (lifecycleLock) {
                pendingConnections.remove(connectionId);
            }
        }
    }

    /** Revokes a completed session and cancels any in-flight login for this connection. */
    public boolean logout(UUID connectionId) {
        return revokeConnection(connectionId);
    }

    /** Idempotent disconnect cleanup; UDP heartbeat timeout must not call this method. */
    public boolean disconnect(UUID connectionId) {
        return revokeConnection(connectionId);
    }

    /** Cancels a reserved but not-yet-authenticated login, for queue rejection/closure. */
    public boolean cancelAuthentication(UUID connectionId) {
        if (connectionId == null) {
            return false;
        }
        synchronized (lifecycleLock) {
            return pendingConnections.remove(connectionId);
        }
    }

    public boolean isPresenceTokenValid(UUID clientId, String presenceToken) {
        return sessionRegistry.isPresenceTokenValid(clientId, presenceToken);
    }

    public boolean isAuthenticatedConnection(UUID connectionId) {
        return sessionRegistry.findByConnectionId(connectionId).isPresent();
    }

    public Optional<SessionView> sessionForConnection(UUID connectionId) {
        return sessionRegistry.findByConnectionId(connectionId);
    }

    private boolean revokeConnection(UUID connectionId) {
        if (connectionId == null) {
            return false;
        }
        synchronized (lifecycleLock) {
            pendingConnections.remove(connectionId);
            return sessionRegistry.revokeByConnectionId(connectionId);
        }
    }

    private static LoginResult failure(Status status) {
        return new LoginResult(status, null, null);
    }
}
