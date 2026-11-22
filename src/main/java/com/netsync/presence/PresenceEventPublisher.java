package com.netsync.presence;

import com.netsync.common.EnqueueResult;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import com.netsync.common.OutboundSink;
import com.netsync.common.PresenceListener;
import com.netsync.common.PresenceState;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SelectorTasks;
import com.netsync.common.SessionRegistry;
import com.netsync.common.SessionView;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Queues presence transitions onto the Selector before broadcasting TCP events. */
public final class PresenceEventPublisher implements PresenceListener {
    private static final System.Logger LOG = System.getLogger(PresenceEventPublisher.class.getName());

    private final SelectorTasks selectorTasks;
    private final SessionRegistry sessions;
    private final OutboundSink outbound;
    private final ConcurrentMap<UUID, String> knownUsernames = new ConcurrentHashMap<>();

    public PresenceEventPublisher(SelectorTasks selectorTasks, SessionRegistry sessions, OutboundSink outbound) {
        this.selectorTasks = Objects.requireNonNull(selectorTasks);
        this.sessions = Objects.requireNonNull(sessions);
        this.outbound = Objects.requireNonNull(outbound);
    }

    @Override
    public void onPresenceChanged(UUID clientId, PresenceState state) {
        Objects.requireNonNull(clientId);
        Objects.requireNonNull(state);

        SessionView activeSession = sessions.findByClientId(clientId).orElse(null);
        if (activeSession != null) {
            knownUsernames.put(clientId, activeSession.username());
        } else if (state == PresenceState.ONLINE) {
            LOG.log(System.Logger.Level.WARNING, "Cannot publish ONLINE for an inactive session");
            return;
        }

        String username = activeSession == null ? knownUsernames.get(clientId) : activeSession.username();
        if (username == null) {
            LOG.log(System.Logger.Level.WARNING, "Cannot publish presence transition without a known username");
            return;
        }

        PresenceUpdate update = new PresenceUpdate(clientId, username, state);
        if (!selectorTasks.submit(() -> publish(update))) {
            LOG.log(System.Logger.Level.WARNING,
                    "Selector rejected presence transition for client " + clientId);
        }
    }

    /**
     * Call after tracker revocation has issued its transition. The queued event
     * captures its username; this also clears sessions revoked while already OFFLINE.
     */
    public void forgetSession(UUID clientId) {
        Objects.requireNonNull(clientId);
        knownUsernames.remove(clientId);
    }

    private void publish(PresenceUpdate update) {
        MessageEnvelope event = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.PRESENCE_UPDATE,
                UUID.randomUUID(),
                null,
                new MessageBody.PresenceUpdate(update.clientId(), update.username(), update.state()));
        try {
            for (SessionView recipient : sessions.activeSessions()) {
                if (outbound.send(recipient.connectionId(), event) == EnqueueResult.REJECTED) {
                    LOG.log(System.Logger.Level.WARNING,
                            "Presence event was rejected for connection " + recipient.connectionId());
                }
            }
        } finally {
            if (update.state() == PresenceState.OFFLINE) {
                knownUsernames.remove(update.clientId(), update.username());
            }
        }
    }

    private record PresenceUpdate(UUID clientId, String username, PresenceState state) {}
}
