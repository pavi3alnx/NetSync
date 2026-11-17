package com.netsync.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class FrameCodecTest {
    final FrameCodec codec=new FrameCodec(new MessageCodec());
    final UUID id=MessageCodecTest.ID;
    MessageEnvelope message() { return new MessageEnvelope(1,MessageType.LOGOUT_REQUEST,id,null,new MessageBody.Empty()); }
    OutboundFrame read(ByteBuffer wire) throws Exception {
        int n=codec.decodeLength(wire.slice(wire.position(),4)); wire.position(wire.position()+4);
        ByteBuffer payload=wire.slice(wire.position(),n); OutboundFrame frame=codec.decodePayload(payload);
        assertFalse(payload.hasRemaining()); wire.position(wire.position()+n); return frame;
    }
    @Test void jsonRoundTripAndLength() throws Exception {
        ByteBuffer wire=codec.encode(message());
        assertEquals(0,wire.position()); assertEquals(wire.remaining()-4,wire.getInt(0)); assertEquals(1,wire.get(4));
        assertEquals(message(),read(wire)); assertFalse(wire.hasRemaining());
    }
    @Test void exactBinaryLayout() throws Exception {
        UUID transfer=UUID.fromString("01234567-89ab-cdef-0123-456789abcdef");
        var chunk=new FileChunk(id,transfer,0x01020304,new byte[16384]);
        ByteBuffer wire=codec.encode(chunk);
        assertArrayEquals(new byte[]{0,0,0x40,0x25,2},Arrays.copyOf(wire.array(),5));
        assertEquals(id.getMostSignificantBits(),wire.getLong(5));
        assertEquals(id.getLeastSignificantBits(),wire.getLong(13));
        assertEquals(transfer.getMostSignificantBits(),wire.getLong(21));
        assertEquals(transfer.getLeastSignificantBits(),wire.getLong(29));
        assertEquals(0x01020304,wire.getInt(37)); assertEquals(chunk,read(wire));
    }
    @Test void interleavedCoalescedFramesPreserveBinary() throws Exception {
        var chunk=new FileChunk(id,id,0,"{\"looks\":\"json\"}\n".getBytes(StandardCharsets.UTF_8));
        ByteBuffer a=codec.encode(message()),b=codec.encode(chunk),c=codec.encode(message());
        ByteBuffer combined=ByteBuffer.allocate(a.remaining()+b.remaining()+c.remaining()).put(a).put(b).put(c).flip();
        assertEquals(message(),read(combined)); assertEquals(chunk,read(combined)); assertEquals(message(),read(combined));
        assertFalse(combined.hasRemaining());
    }
    @ParameterizedTest @ValueSource(ints={Integer.MIN_VALUE,-1,0,1,65538,Integer.MAX_VALUE})
    void invalidLengthsCloseBeforeAllocation(int n) {
        ByteBuffer prefix=ByteBuffer.allocate(4).putInt(n).flip();
        var e=assertThrows(ProtocolException.class,()->codec.decodeLength(prefix));
        assertEquals(ProtocolException.Action.CLOSE,e.action()); assertTrue(e.code().isEmpty()); assertEquals(0,prefix.position());
    }
    @Test void validLengthBoundaries() throws Exception {
        for(int n:new int[]{2,38,16421,65537}) assertEquals(n,codec.decodeLength(ByteBuffer.allocate(4).putInt(n).flip()));
        codec.validateKindLength((byte)1,2); codec.validateKindLength((byte)1,65537);
        codec.validateKindLength((byte)2,38); codec.validateKindLength((byte)2,16421);
    }
    @Test void invalidKindsAndKindLengths() {
        for(int kind:new int[]{0,3,127,255}) assertThrows(ProtocolException.class,()->codec.validateKindLength((byte)kind,38));
        for(int n:new int[]{2,37,16422,65537}) assertThrows(ProtocolException.class,()->codec.validateKindLength((byte)2,n));
    }
    @Test void truncatedCompleteInputsRejected() {
        for(int len:new int[]{0,1,2,3,5}) assertThrows(ProtocolException.class,()->codec.decodeLength(ByteBuffer.allocate(len)));
        assertThrows(ProtocolException.class,()->codec.decodePayload(ByteBuffer.allocate(0)));
        assertThrows(ProtocolException.class,()->codec.decodePayload(ByteBuffer.wrap(new byte[]{2,0})));
    }
    @Test void negativeBinarySequenceCloses() throws Exception {
        ByteBuffer frame=codec.encode(new FileChunk(id,id,0,new byte[]{1})); frame.putInt(37,-1); frame.position(4);
        assertEquals(ProtocolException.Action.CLOSE,assertThrows(ProtocolException.class,()->codec.decodePayload(frame)).action());
    }
    @Test void slicedDirectReadOnlyAndLittleEndianInputs() throws Exception {
        ByteBuffer encoded=codec.encode(new FileChunk(id,id,Integer.MAX_VALUE,new byte[]{(byte)255}));
        ByteBuffer direct=ByteBuffer.allocateDirect(encoded.remaining()+7).order(ByteOrder.LITTLE_ENDIAN);
        direct.position(7); direct.put(encoded).flip().position(7);
        assertEquals(new FileChunk(id,id,Integer.MAX_VALUE,new byte[]{(byte)255}),read(direct.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN)));
    }
    @Test void jsonErrorsKeepTheirClassification() {
        ByteBuffer payload=ByteBuffer.wrap(new byte[]{1,'{','}'});
        assertEquals(Optional.of(ErrorCode.MALFORMED_MESSAGE),assertThrows(ProtocolException.class,()->codec.decodePayload(payload)).code());
    }
    @Test void maximumJsonFrameUses65537Length() throws Exception {
        String base=MessageCodecTest.json("ERROR","{\"code\":\"BUSY\",\"message\":\"\"}");
        String exact=base.replace("\"message\":\"\"","\"message\":\""+"x".repeat(65536-MessageCodecTest.bytes(base).length)+"\"");
        var message=new MessageCodec().decode(MessageCodecTest.bytes(exact));
        ByteBuffer encoded=codec.encode(message);
        assertEquals(65541,encoded.remaining()); assertEquals(65537,encoded.getInt(0));
        assertEquals(message,read(encoded));
    }
    @Test void partialWritePositionsCanBeRetained() throws Exception {
        ByteBuffer encoded=codec.encode(message()); byte[] original=encoded.array().clone();
        ByteBuffer output=ByteBuffer.allocate(original.length);
        while(encoded.hasRemaining()) {
            int count=Math.min(3,encoded.remaining());
            var part=encoded.slice(); part.limit(count); output.put(part); encoded.position(encoded.position()+count);
        }
        assertArrayEquals(original,output.array());
    }
}
