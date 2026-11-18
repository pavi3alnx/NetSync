package com.netsync.presence;

import com.netsync.common.Heartbeat;
import com.netsync.common.PresenceListener;
import com.netsync.common.PresenceState;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.Base64;

public final class HeartbeatTracker {
    public interface TimeSource {
        long nanoTime();
    }

    public interface TokenValidator {
        boolean isValid(UUID clientId, String presenceToken);
    }

    private final TimeSource timeSource;
    private final PresenceListener listener;
    private final SessionRegistry sessionRegistry;
    private final TokenValidator tokenValidator;
    private final Map<UUID, SessionPresence> sessions = new HashMap<>();

    public HeartbeatTracker(
            TimeSource timeSource,
            PresenceListener listener,
            SessionRegistry sessionRegistry,
            TokenValidator tokenValidator) {
        this.timeSource = Objects.requireNonNull(timeSource);
        this.listener = Objects.requireNonNull(listener);
        this.sessionRegistry = Objects.requireNonNull(sessionRegistry);
        this.tokenValidator = Objects.requireNonNull(tokenValidator);
    }

    public boolean registerSession(UUID clientId, String presenceToken) {
        Objects.requireNonNull(clientId);
        Objects.requireNonNull(presenceToken);

        if (!isCanonicalPresenceToken(presenceToken)) {
            return false;
        }
        if (sessionRegistry.findByClientId(clientId).isEmpty()
                || !tokenValidator.isValid(clientId, presenceToken)) {
            return false;
        }

        boolean notifyOnline;
        synchronized (sessions) {
            SessionPresence existing = sessions.get(clientId);
            if (existing != null
                    && existing.active
                    && existing.presenceToken.equals(presenceToken)
                    && existing.state == PresenceState.ONLINE) {
                return true;
            }

            sessions.put(clientId, new SessionPresence(
                    presenceToken,
                    -1,
                    timeSource.nanoTime(),
                    PresenceState.ONLINE,
                    true));
            notifyOnline = existing == null || existing.state == PresenceState.OFFLINE;
        }

        if (notifyOnline) {
            listener.onPresenceChanged(clientId, PresenceState.ONLINE);
        }
        return true;
    }

    public boolean processHeartbeat(Heartbeat heartbeat) {
        if (heartbeat == null
                || heartbeat.version() != ProtocolLimits.VERSION
                || !"HEARTBEAT".equals(heartbeat.type())
                || heartbeat.sequence() < 0) {
            return false;
        }

        UUID clientId = heartbeat.clientId();

        synchronized (sessions) {
            SessionPresence initial = sessions.get(clientId);
            if (initial == null || !initial.active) {
                return false;
            }
        }

        if (!isCanonicalPresenceToken(heartbeat.presenceToken())
                || sessionRegistry.findByClientId(clientId).isEmpty()
                || !tokenValidator.isValid(clientId, heartbeat.presenceToken())) {
            return false;
        }

        List<PresenceChange> changes = new ArrayList<>(1);
        synchronized (sessions) {
            SessionPresence session = sessions.get(clientId);
            if (session == null
                    || !session.active
                    || !session.presenceToken.equals(heartbeat.presenceToken())
                    || heartbeat.sequence() <= session.highestAcceptedSequence) {
                return false;
            }

            session.highestAcceptedSequence = heartbeat.sequence();
            session.lastValidHeartbeatNanoTime = timeSource.nanoTime();
            if (session.state != PresenceState.ONLINE) {
                session.state = PresenceState.ONLINE;
                changes.add(new PresenceChange(clientId, PresenceState.ONLINE));
            }
        }

        notify(changes);
        return true;
    }

    public void checkTimeouts() {
        long now = timeSource.nanoTime();
        long timeoutNanos = TimeUnit.SECONDS.toNanos(ProtocolLimits.PRESENCE_TIMEOUT_SECONDS);
        List<PresenceChange> changes = new ArrayList<>();

        synchronized (sessions) {
            for (Map.Entry<UUID, SessionPresence> entry : sessions.entrySet()) {
                SessionPresence session = entry.getValue();
                if (session.active
                        && session.state == PresenceState.ONLINE
                        && now - session.lastValidHeartbeatNanoTime >= timeoutNanos) {
                    session.state = PresenceState.OFFLINE;
                    changes.add(new PresenceChange(entry.getKey(), PresenceState.OFFLINE));
                }
            }
        }

        notify(changes);
    }

    public void revokeSession(UUID clientId) {
        Objects.requireNonNull(clientId);

        PresenceChange change = null;
        synchronized (sessions) {
            SessionPresence session = sessions.remove(clientId);
            if (session != null && session.state == PresenceState.ONLINE) {
                change = new PresenceChange(clientId, PresenceState.OFFLINE);
            }
        }

        if (change != null) {
            listener.onPresenceChanged(change.clientId(), change.state());
        }
    }

    public Optional<PresenceState> getPresenceState(UUID clientId) {
        Objects.requireNonNull(clientId);
        synchronized (sessions) {
            SessionPresence session = sessions.get(clientId);
            return session == null ? Optional.empty() : Optional.of(session.state);
        }
    }

    private void notify(List<PresenceChange> changes) {
        for (PresenceChange change : changes) {
            listener.onPresenceChanged(change.clientId(), change.state());
        }
    }

    private static boolean isCanonicalPresenceToken(String presenceToken) {
        try {
            byte[] decoded = Base64.getDecoder().decode(presenceToken);
            return decoded.length == 32
                    && Base64.getEncoder().encodeToString(decoded).equals(presenceToken);
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    private record PresenceChange(UUID clientId, PresenceState state) {}

    private static final class SessionPresence {
        private final String presenceToken;
        private long highestAcceptedSequence;
        private long lastValidHeartbeatNanoTime;
        private PresenceState state;
        private boolean active;

        private SessionPresence(
                String presenceToken,
                long highestAcceptedSequence,
                long lastValidHeartbeatNanoTime,
                PresenceState state,
                boolean active) {
            this.presenceToken = presenceToken;
            this.highestAcceptedSequence = highestAcceptedSequence;
            this.lastValidHeartbeatNanoTime = lastValidHeartbeatNanoTime;
            this.state = state;
            this.active = active;
        }
    }
}
