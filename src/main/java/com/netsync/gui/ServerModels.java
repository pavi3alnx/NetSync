package com.netsync.gui;

import com.netsync.common.PresenceState;
import com.netsync.common.ProtocolLimits;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Immutable UI view models for the NetSync graphical server dashboard. */
public final class ServerModels {
    private ServerModels() {}

    public enum ServerState {
        STOPPED,
        STARTING,
        RUNNING,
        STOPPING,
        FAILED
    }

    public record MonitoredSession(UUID clientId, String username, UUID connectionId, PresenceState presenceState) {}

    public record ActivityLogEntry(Instant timestamp, String level, String message) {}

    public record ServerSnapshot(
            ServerState state,
            String bindAddress,
            int tcpPort,
            int udpPort,
            int configuredAccountCount,
            long uptimeSeconds,
            List<MonitoredSession> sessions,
            int activeTransferCount,
            String statusMessage
    ) {
        public ServerSnapshot {
            sessions = List.copyOf(sessions);
        }

        public static ServerSnapshot stopped(int tcpPort, int udpPort, int accountCount, String message) {
            return new ServerSnapshot(ServerState.STOPPED, ProtocolLimits.BIND_ADDRESS, tcpPort, udpPort,
                    accountCount, 0L, List.of(), 0, message);
        }
    }
}
