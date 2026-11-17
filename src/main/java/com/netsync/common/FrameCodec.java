package com.netsync.common;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.UUID;

/** Stateless complete-frame codec shared by server and client.
 * Transport owns partial prefix/payload buffers, allocation, EOF and I/O budgets.
 * Decode methods require exactly one complete input and consume it on success only.
 * Returned encoded buffers have position zero and are ready for partial writes.
 */
public final class FrameCodec {
    private final MessageCodec messages;
    public FrameCodec(MessageCodec messages) { this.messages=Objects.requireNonNull(messages); }
    public ByteBuffer encode(OutboundFrame frame) throws ProtocolException {
        Objects.requireNonNull(frame);
        if(frame instanceof MessageEnvelope envelope) {
            byte[] json=messages.encode(envelope);
            return ByteBuffer.allocate(5+json.length).order(ByteOrder.BIG_ENDIAN)
                .putInt(1+json.length).put(ProtocolLimits.JSON_KIND).put(json).flip();
        }
        FileChunk chunk=(FileChunk)frame;
        byte[] data=chunk.data();
        ByteBuffer result=ByteBuffer.allocate(41+data.length).order(ByteOrder.BIG_ENDIAN);
        result.putInt(37+data.length).put(ProtocolLimits.CHUNK_KIND);
        putUuid(result,chunk.messageId()); putUuid(result,chunk.transferId());
        return result.putInt(chunk.sequence()).put(data).flip();
    }
    /** Validate prefix before allocating N payload bytes; caller byte order is ignored. */
    public int decodeLength(ByteBuffer prefix) throws ProtocolException {
        if(prefix.remaining()!=4) throw invalidFrame();
        int n=prefix.duplicate().order(ByteOrder.BIG_ENDIAN).getInt();
        validateLength(n); prefix.position(prefix.position()+4); return n;
    }
    /** Call as soon as kind arrives, before waiting for the rest of the payload. */
    public void validateKindLength(byte kind,int length) throws ProtocolException {
        validateLength(length);
        if(kind==ProtocolLimits.JSON_KIND) return;
        if(kind!=ProtocolLimits.CHUNK_KIND || length<38 || length>16421) throw invalidFrame();
    }
    public OutboundFrame decodePayload(ByteBuffer payload) throws ProtocolException {
        int n=payload.remaining(); validateLength(n);
        ByteBuffer view=payload.duplicate().order(ByteOrder.BIG_ENDIAN);
        byte kind=view.get(); validateKindLength(kind,n);
        OutboundFrame frame;
        if(kind==ProtocolLimits.JSON_KIND) {
            byte[] bytes=new byte[view.remaining()]; view.get(bytes); frame=messages.decode(bytes);
        } else {
            UUID messageId=getUuid(view), transferId=getUuid(view);
            int sequence=view.getInt();
            if(sequence<0) throw invalidFrame();
            byte[] data=new byte[view.remaining()]; view.get(data);
            frame=new FileChunk(messageId,transferId,sequence,data);
        }
        payload.position(payload.limit()); return frame;
    }
    private static void validateLength(int n) throws ProtocolException {
        if(n<ProtocolLimits.MIN_FRAME_LENGTH || n>ProtocolLimits.MAX_FRAME_LENGTH) throw invalidFrame();
    }
    private static ProtocolException invalidFrame() {
        return new ProtocolException(null,null,ProtocolException.Action.CLOSE);
    }
    private static void putUuid(ByteBuffer b,UUID id) { b.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID getUuid(ByteBuffer b) { return new UUID(b.getLong(),b.getLong()); }
}
