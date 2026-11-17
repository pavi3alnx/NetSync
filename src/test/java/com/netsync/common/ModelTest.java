package com.netsync.common;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ModelTest {
    private final UUID id=UUID.randomUUID();
    @Test void chunkCopiesBothDirections() {
        byte[] bytes={1,2}; FileChunk c=new FileChunk(id,id,0,bytes); bytes[0]=9;
        assertArrayEquals(new byte[]{1,2},c.data()); c.data()[0]=8;
        assertArrayEquals(new byte[]{1,2},c.data());
        assertEquals(c,new FileChunk(id,id,0,new byte[]{1,2}));
        assertEquals(c.hashCode(),new FileChunk(id,id,0,new byte[]{1,2}).hashCode());
    }
    @Test void chunkBounds() {
        assertThrows(IllegalArgumentException.class,()->new FileChunk(id,id,-1,new byte[]{1}));
        assertThrows(IllegalArgumentException.class,()->new FileChunk(id,id,0,new byte[0]));
        assertThrows(IllegalArgumentException.class,()->new FileChunk(id,id,0,new byte[16385]));
        assertDoesNotThrow(()->new FileChunk(id,id,Integer.MAX_VALUE,new byte[16384]));
    }
    @Test void snapshotIsImmutable() {
        var list=new ArrayList<MessageBody.PresenceEntry>();
        var snapshot=new MessageBody.PresenceSnapshot(list);
        list.add(new MessageBody.PresenceEntry(id,"alice",PresenceState.ONLINE));
        assertTrue(snapshot.clients().isEmpty());
        assertThrows(UnsupportedOperationException.class,()->snapshot.clients().clear());
    }
    @Test void credentialsAreRedacted() {
        assertFalse(new MessageBody.LoginRequest("alice","secret").toString().contains("secret"));
        assertFalse(new MessageBody.LoginResponse(id,"alice","secret",5,15).toString().contains("secret"));
        assertFalse(new Heartbeat(1,"HEARTBEAT",id,"secret",0).toString().contains("secret"));
    }
    @Test void safeExceptionMetadata() {
        var e=new ProtocolException(ErrorCode.INVALID_REQUEST,id,ProtocolException.Action.KEEP_OPEN);
        assertEquals(Optional.of(id),e.replyTo()); assertEquals(Optional.of(ErrorCode.INVALID_REQUEST),e.code());
        assertEquals(ProtocolException.Action.KEEP_OPEN,e.action()); assertNull(e.getCause());
    }
}
