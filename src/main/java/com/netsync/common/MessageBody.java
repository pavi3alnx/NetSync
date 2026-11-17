package com.netsync.common;

import java.util.List;
import java.util.UUID;
/** Version 1 body shapes. Codec validates required values and message/body pairing. */
public sealed interface MessageBody {
    record Empty() implements MessageBody {}
    record LoginRequest(String username, String password) implements MessageBody {
        @Override public String toString() { return "LoginRequest[credentials=REDACTED]"; }
    }
    record LoginResponse(UUID clientId, String username, String presenceToken,
            int heartbeatIntervalSeconds, int presenceTimeoutSeconds) implements MessageBody {
        @Override public String toString() { return "LoginResponse[presenceToken=REDACTED]"; }
    }
    record ChatSend(UUID recipientClientId, String text) implements MessageBody {}
    record ChatDeliver(UUID senderClientId, String text, UUID sourceMessageId) implements MessageBody {}
    record PresenceEntry(UUID clientId, String username, PresenceState state) {}
    record PresenceSnapshot(List<PresenceEntry> clients) implements MessageBody {
        public PresenceSnapshot { clients=List.copyOf(clients); }
    }
    record PresenceUpdate(UUID clientId, String username, PresenceState state) implements MessageBody {}
    record FileOffer(UUID recipientClientId, String fileName, long sizeBytes) implements MessageBody {}
    record FileOffered(UUID transferId, UUID senderClientId, String fileName, long sizeBytes) implements MessageBody {}
    record TransferReference(UUID transferId) implements MessageBody {}
    record FileComplete(UUID transferId, int chunkCount, String sha256) implements MessageBody {}
    record FileReceived(UUID transferId, String sha256) implements MessageBody {}
    record FileStatus(UUID transferId, FileStatusState state, String reason) implements MessageBody {}
    record Ack(MessageType operation, AckStatus status, UUID transferId) implements MessageBody {}
    record Error(ErrorCode code, String message, UUID transferId) implements MessageBody {}
}
