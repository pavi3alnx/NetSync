package com.netsync.security;

import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Security-owned, thread-safe in-memory session registry.
 *
 * <p>The protocol has no general session-TTL setting. By default, sessions live until
 * logout or TCP disconnect. A finite lifetime may be supplied explicitly for deployments
 * or tests that require additional expiration behavior.</p>
 */
public final class SessionRegistryImpl implements SessionRegistry {
    private static final int UUID_RETRY_LIMIT = 1_000;
    private static final SessionTokenGenerator TOKEN_VALIDATOR = new SessionTokenGenerator();

    static final class SessionConflictException extends IllegalStateException {
        private final boolean connectionConflict;

        private SessionConflictException(boolean connectionConflict) {
            super("Active session conflict");
            this.connectionConflict = connectionConflict;
        }

        boolean connectionConflict() {
            return connectionConflict;
        }
    }

    private final Clock clock;
    private final Duration sessionLifetime;
    private final Map<UUID, SessionRecord> byConnectionId = new LinkedHashMap<>();
    private final Map<UUID, SessionRecord> byClientId = new LinkedHashMap<>();
    private final Map<String, SessionRecord> byUsername = new LinkedHashMap<>();

    public SessionRegistryImpl() {
        this(Clock.systemUTC(), null);
    }

    public SessionRegistryImpl(Clock clock, Duration sessionLifetime) {
        this.clock = Objects.requireNonNull(clock, "clock");
        if (sessionLifetime != null && sessionLifetime.isNegative()) {
            throw new IllegalArgumentException("Session lifetime must not be negative");
        }
        if (sessionLifetime != null && sessionLifetime.isZero()) {
            throw new IllegalArgumentException("Session lifetime must be positive");
        }
        this.sessionLifetime = sessionLifetime;
    }

    synchronized SessionView createSession(UUID connectionId, String username) {
        return createSession(connectionId, username, null);
    }

    synchronized SessionView createSession(UUID connectionId, String username, String presenceToken) {
        if (connectionId == null) {
            throw new IllegalArgumentException("Connection id must not be null");
        }
        if (username == null || !username.matches("[A-Za-z0-9_]{1,32}")) {
            throw new IllegalArgumentException("Invalid username");
        }

        expireExpiredSessionsLocked();
        if (byConnectionId.containsKey(connectionId)) {
            throw new SessionConflictException(true);
        }
        if (byUsername.containsKey(username)) {
            throw new SessionConflictException(false);
        }
        if (presenceToken != null && !TOKEN_VALIDATOR.isValidToken(presenceToken)) {
            throw new IllegalArgumentException("Invalid presence token");
        }

        UUID clientId = uniqueClientIdLocked();
        Instant expiresAt = sessionLifetime == null
                ? null
                : clock.instant().plus(sessionLifetime);
        SessionView view = new SessionView(clientId, username, connectionId);
        SessionRecord record = new SessionRecord(view, expiresAt, presenceToken);
        byConnectionId.put(connectionId, record);
        byClientId.put(clientId, record);
        byUsername.put(username, record);
        return view;
    }

    @Override
    public synchronized Optional<SessionView> findByConnectionId(UUID id) {
        if (id == null) {
            return Optional.empty();
        }
        expireExpiredSessionsLocked();
        SessionRecord record = byConnectionId.get(id);
        return record == null ? Optional.empty() : Optional.of(record.view());
    }

    @Override
    public synchronized Optional<SessionView> findByClientId(UUID id) {
        if (id == null) {
            return Optional.empty();
        }
        expireExpiredSessionsLocked();
        SessionRecord record = byClientId.get(id);
        return record == null ? Optional.empty() : Optional.of(record.view());
    }

    @Override
    public synchronized List<SessionView> activeSessions() {
        expireExpiredSessionsLocked();
        List<SessionView> sessions = new ArrayList<>(byConnectionId.size());
        for (SessionRecord record : byConnectionId.values()) {
            sessions.add(record.view());
        }
        return List.copyOf(sessions);
    }

    synchronized boolean revokeByConnectionId(UUID connectionId) {
        if (connectionId == null) {
            return false;
        }
        expireExpiredSessionsLocked();
        SessionRecord record = byConnectionId.remove(connectionId);
        if (record == null) {
            return false;
        }
        removeLocked(record);
        return true;
    }

    synchronized boolean revokeByClientId(UUID clientId) {
        if (clientId == null) {
            return false;
        }
        expireExpiredSessionsLocked();
        SessionRecord record = byClientId.remove(clientId);
        if (record == null) {
            return false;
        }
        removeLocked(record);
        return true;
    }

    public synchronized boolean isActive(SessionView session) {
        if (session == null) {
            return false;
        }
        expireExpiredSessionsLocked();
        SessionRecord record = byConnectionId.get(session.connectionId());
        return record != null
                && record.view().equals(session)
                && byClientId.containsKey(session.clientId());
    }

    synchronized boolean isPresenceTokenValid(UUID clientId, String presenceToken) {
        if (clientId == null || !TOKEN_VALIDATOR.isValidToken(presenceToken)) {
            return false;
        }
        expireExpiredSessionsLocked();
        SessionRecord record = byClientId.get(clientId);
        return record != null
                && record.presenceToken() != null
                && MessageDigest.isEqual(
                        record.presenceToken().getBytes(StandardCharsets.US_ASCII),
                        presenceToken.getBytes(StandardCharsets.US_ASCII));
    }

    synchronized int expireExpiredSessions() {
        return expireExpiredSessionsLocked();
    }

    public synchronized int activeSessionCount() {
        expireExpiredSessionsLocked();
        return byConnectionId.size();
    }

    private UUID uniqueClientIdLocked() {
        for (int attempt = 0; attempt < UUID_RETRY_LIMIT; attempt++) {
            UUID candidate = UUID.randomUUID();
            if (!byClientId.containsKey(candidate)) {
                return candidate;
            }
        }
        throw new IllegalStateException("Unable to generate a unique client id");
    }

    private int expireExpiredSessionsLocked() {
        if (sessionLifetime == null) {
            return 0;
        }

        Instant now = clock.instant();
        int removed = 0;
        for (SessionRecord record : List.copyOf(byConnectionId.values())) {
            if (record.expiresAt() != null && !now.isBefore(record.expiresAt())) {
                removeLocked(record);
                removed++;
            }
        }
        return removed;
    }

    private void removeLocked(SessionRecord record) {
        SessionView view = record.view();
        byConnectionId.remove(view.connectionId(), record);
        byClientId.remove(view.clientId(), record);
        byUsername.remove(view.username(), record);
    }

    private record SessionRecord(SessionView view, Instant expiresAt, String presenceToken) {
        @Override
        public String toString() {
            return "SessionRecord[view=" + view + ", expiresAt=" + expiresAt
                    + ", presenceToken=REDACTED]";
        }
    }
}
