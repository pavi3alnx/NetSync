package com.netsync.gui;

import com.netsync.common.PresenceState;

import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

/** Immutable UI view models for the NetSync graphical client application. */
public final class ClientModels {
    private ClientModels() {}

    public enum ConnectionPhase {
        DISCONNECTED,
        CONNECTING,
        AUTHENTICATED,
        LOGGING_OUT
    }

    public enum DeliveryStatus {
        SENDING,
        QUEUED,
        FAILED,
        UNKNOWN_OUTCOME,
        RECEIVED
    }

    public enum TransferDirection {
        OUTGOING,
        INCOMING
    }

    public enum TransferStatus {
        OFFERING,
        OFFERED,
        TRANSFERRING,
        COMPLETED,
        REJECTED,
        CANCELLED,
        FAILED
    }

    public record UserItem(UUID clientId, String username, PresenceState state) {
        public UserItem withState(PresenceState newState) {
            return new UserItem(clientId, username, newState);
        }
    }

    public record ChatMessageItem(
            UUID requestId,
            UUID peerClientId,
            UUID senderClientId,
            String senderLabel,
            String text,
            Instant timestamp,
            boolean outgoing,
            DeliveryStatus status,
            String statusDetail
    ) {
        public ChatMessageItem withStatus(DeliveryStatus newStatus, String newDetail) {
            return new ChatMessageItem(requestId, peerClientId, senderClientId, senderLabel, text,
                    timestamp, outgoing, newStatus, newDetail);
        }
    }

    public record TransferItem(
            UUID itemKey,
            UUID transferId,
            TransferDirection direction,
            UUID peerClientId,
            String peerLabel,
            String fileName,
            Path localFile,
            long bytesTransferred,
            long totalBytes,
            TransferStatus status,
            String statusDetail,
            String sha256,
            boolean sha256Verified,
            long offerReceivedNanos
    ) {
        public TransferItem(
                UUID itemKey,
                UUID transferId,
                TransferDirection direction,
                UUID peerClientId,
                String peerLabel,
                String fileName,
                Path localFile,
                long bytesTransferred,
                long totalBytes,
                TransferStatus status,
                String statusDetail,
                String sha256,
                boolean sha256Verified
        ) {
            this(itemKey, transferId, direction, peerClientId, peerLabel, fileName, localFile,
                    bytesTransferred, totalBytes, status, statusDetail, sha256, sha256Verified, 0L);
        }

        public double progressRatio() {
            if (status == TransferStatus.COMPLETED) {
                return 1.0;
            }
            if (totalBytes <= 0) {
                return 0.0;
            }
            return Math.min(1.0, Math.max(0.0, (double) bytesTransferred / (double) totalBytes));
        }

        public double percentage() {
            return totalBytes == 0 && status == TransferStatus.COMPLETED ? 100.0 : progressRatio() * 100.0;
        }
    }
}
