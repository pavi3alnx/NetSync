package com.netsync.common;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
/** Immutable binary chunk; sequence progression is the transfer owner's responsibility. */
public final class FileChunk implements OutboundFrame {
    private final UUID messageId, transferId;
    private final int sequence;
    private final byte[] data;
    public FileChunk(UUID messageId, UUID transferId, int sequence, byte[] data) {
        this.messageId=Objects.requireNonNull(messageId); this.transferId=Objects.requireNonNull(transferId);
        Objects.requireNonNull(data);
        if(sequence<0 || data.length<1 || data.length>ProtocolLimits.MAX_CHUNK_BYTES)
            throw new IllegalArgumentException("Invalid chunk bounds");
        this.sequence=sequence; this.data=data.clone();
    }
    public UUID messageId() { return messageId; }
    public UUID transferId() { return transferId; }
    public int sequence() { return sequence; }
    public byte[] data() { return data.clone(); }
    @Override public boolean equals(Object o) {
        return o instanceof FileChunk c && messageId.equals(c.messageId) && transferId.equals(c.transferId)
            && sequence==c.sequence && Arrays.equals(data,c.data);
    }
    @Override public int hashCode() { return Objects.hash(messageId,transferId,sequence,Arrays.hashCode(data)); }
}
