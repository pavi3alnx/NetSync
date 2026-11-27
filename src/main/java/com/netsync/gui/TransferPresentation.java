package com.netsync.gui;

import com.netsync.common.ProtocolLimits;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Pure presentation logic for transfer cards, ensuring accurate sender/receiver labeling,
 * strict gating of SHA-256 and Save As controls, and monotonic incoming-offer countdown estimates.
 */
public final class TransferPresentation {
    private TransferPresentation() {}

    /**
     * Returns the accurately labeled file path line for a completed transfer, or empty for any
     * non-completed (failed, cancelled, rejected, offering, transferring) transfer.
     */
    public static Optional<String> completedFilePathLabel(ClientModels.TransferItem item) {
        if (item == null || item.status() != ClientModels.TransferStatus.COMPLETED || item.localFile() == null) {
            return Optional.empty();
        }
        return item.direction() == ClientModels.TransferDirection.OUTGOING
                ? Optional.of("Source file: " + item.localFile())
                : Optional.of("Saved to: " + item.localFile());
    }

    /** Returns the folder action button text tailored to transfer direction. */
    public static String openFolderButtonText(ClientModels.TransferItem item) {
        if (item != null && item.direction() == ClientModels.TransferDirection.OUTGOING) {
            return "Open Source Folder";
        }
        return "Open Folder";
    }

    /** Returns true only when SHA-256 verification may be displayed truthfully. */
    public static boolean shouldShowSha256Verified(ClientModels.TransferItem item) {
        return item != null
                && item.status() == ClientModels.TransferStatus.COMPLETED
                && item.sha256Verified()
                && item.sha256() != null
                && !item.sha256().isBlank();
    }

    /** Returns true only for completed, SHA-256-verified incoming transfers with an existing local path. */
    public static boolean canSaveAs(ClientModels.TransferItem item) {
        return item != null
                && item.direction() == ClientModels.TransferDirection.INCOMING
                && item.status() == ClientModels.TransferStatus.COMPLETED
                && item.sha256Verified()
                && item.localFile() != null
                && item.transferId() != null;
    }

    /**
     * Computes estimated remaining whole seconds for a pending incoming offer using monotonic time
     * against the authoritative {@link ProtocolLimits#TRANSFER_TIMEOUT_SECONDS} (30s) limit.
     * Returns empty for non-pending or outgoing transfers.
     */
    public static Optional<Long> estimatedRemainingOfferSeconds(ClientModels.TransferItem item, long nowNanos) {
        if (item == null
                || item.direction() != ClientModels.TransferDirection.INCOMING
                || item.status() != ClientModels.TransferStatus.OFFERED
                || item.offerReceivedNanos() == 0L) {
            return Optional.empty();
        }
        long elapsedNanos = Math.max(0L, nowNanos - item.offerReceivedNanos());
        long timeoutNanos = TimeUnit.SECONDS.toNanos(ProtocolLimits.TRANSFER_TIMEOUT_SECONDS);
        long remainingNanos = Math.max(0L, timeoutNanos - elapsedNanos);
        long remainingSeconds = (remainingNanos + 999_999_999L) / 1_000_000_000L;
        return Optional.of(Math.min(ProtocolLimits.TRANSFER_TIMEOUT_SECONDS, remainingSeconds));
    }

    /**
     * Formats an advisory countdown string for a pending incoming offer.
     * Never synthesizes an authoritative terminal state when the estimate reaches zero.
     */
    public static Optional<String> countdownLabel(ClientModels.TransferItem item, long nowNanos) {
        Optional<Long> remainingOpt = estimatedRemainingOfferSeconds(item, nowNanos);
        if (remainingOpt.isEmpty()) {
            return Optional.empty();
        }
        long seconds = remainingOpt.get();
        if (seconds <= 0L) {
            return Optional.of("Offer expiring shortly — awaiting server timeout");
        }
        if (seconds == 1L) {
            return Optional.of("Respond within approximately 1 second");
        }
        return Optional.of("Respond within approximately " + seconds + " seconds");
    }
}
