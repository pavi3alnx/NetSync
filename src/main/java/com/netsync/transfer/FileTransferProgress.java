package com.netsync.transfer;

import java.util.UUID;

/**
 * Progress update carrier emitted during file streaming.
 */
public record FileTransferProgress(
        UUID transferId,
        long bytesTransferred,
        long totalBytes,
        int chunkSequence
) {
    public double percentage() {
        return totalBytes == 0 ? 100.0 : ((double) bytesTransferred / totalBytes) * 100.0;
    }
}
