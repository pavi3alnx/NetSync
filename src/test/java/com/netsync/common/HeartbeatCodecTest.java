package com.netsync.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.*;

class HeartbeatCodecTest {
    final HeartbeatCodec codec=new HeartbeatCodec();
    String json() { return new String(codec.encode(new Heartbeat(1,"HEARTBEAT",MessageCodecTest.ID,MessageCodecTest.TOKEN,1)),StandardCharsets.UTF_8); }
    @Test void sequenceBoundariesRoundTrip() {
        for(long seq:new long[]{0,1,Long.MAX_VALUE}) {
            var h=new Heartbeat(1,"HEARTBEAT",MessageCodecTest.ID,MessageCodecTest.TOKEN,seq);
            assertEquals(h,codec.decode(codec.encode(h)).orElseThrow());
        }
    }
    @ParameterizedTest @ValueSource(strings={"-1","1.0","\"1\"","9223372036854775808","null"})
    void invalidSequences(String seq) { assertTrue(codec.decode(MessageCodecTest.bytes(json().replace("\"sequence\":1","\"sequence\":"+seq))).isEmpty()); }
    @Test void datagramSizeBoundary() {
        String j=json();
        assertTrue(codec.decode(MessageCodecTest.bytes(j+" ".repeat(1024-j.length()))).isPresent());
        assertTrue(codec.decode(MessageCodecTest.bytes(j+" ".repeat(1025-j.length()))).isEmpty());
    }
    @Test void malformedSchemaSilentlyDrops() {
        for(String j:new String[]{"{}",json()+"{}",json().replace("HEARTBEAT","CHAT_SEND"),json().replace("\"version\":1","\"version\":2"),
                json().replace(MessageCodecTest.TOKEN,"bad"),json().replace("\"sequence\":1","\"sequence\":1,\"sequence\":2"),
                json().replace("\"sequence\":1","\"extra\":0,\"sequence\":1"),json().replace(MessageCodecTest.ID.toString(),"bad")})
            assertTrue(codec.decode(MessageCodecTest.bytes(j)).isEmpty());
        assertTrue(codec.decode(new byte[]{(byte)0xff}).isEmpty());
        assertTrue(codec.decode(null).isEmpty());
    }
    @Test void invalidOutboundRejected() {
        assertThrows(IllegalArgumentException.class,()->codec.encode(new Heartbeat(1,"HEARTBEAT",MessageCodecTest.ID,"bad",0)));
    }
}
