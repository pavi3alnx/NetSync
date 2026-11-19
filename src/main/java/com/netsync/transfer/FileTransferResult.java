package com.netsync.transfer;

import java.nio.file.Path;
import java.util.UUID;

/**
 * Result carrier representing the final status of a client-side file transfer.
 */
public record FileTransferResult(
        UUID transferId,
        boolean success,
        String message,
        Path localFile,
        long bytesTransferred,
        String sha256
) {
    public static FileTransferResult completed(UUID transferId, Path localFile, long bytes, String sha256) {
        return new FileTransferResult(transferId, true, "Transfer completed successfully", localFile, bytes, sha256);
    }

    public static FileTransferResult rejected(UUID transferId, String reason) {
        return new FileTransferResult(transferId, false, "Transfer rejected: " + reason, null, 0, null);
    }

    public static FileTransferResult cancelled(UUID transferId, String reason) {
        return new FileTransferResult(transferId, false, "Transfer cancelled: " + reason, null, 0, null);
    }

    public static FileTransferResult failed(UUID transferId, String message) {
        return new FileTransferResult(transferId, false, "Transfer failed: " + message, null, 0, null);
    }
}
