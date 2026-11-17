package com.netsync.common;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Optional;

/** Invalid datagrams return empty and must be silently dropped. Does not verify credentials/replay. */
public final class HeartbeatCodec {
    public Optional<Heartbeat> decode(byte[] datagram) {
        try {
            JsonNode n=JsonSupport.parse(datagram,ProtocolLimits.MAX_HEARTBEAT_BYTES);
            JsonSupport.fields(n,"version","type","clientId","presenceToken","sequence");
            if(JsonSupport.integer(n.get("version"),Integer.MAX_VALUE)!=1 || !JsonSupport.text(n.get("type")).equals("HEARTBEAT"))
                return Optional.empty();
            String token=JsonSupport.text(n.get("presenceToken")); JsonSupport.token(token);
            return Optional.of(new Heartbeat(1,"HEARTBEAT",JsonSupport.uuid(n.get("clientId")),token,
                JsonSupport.integer(n.get("sequence"),Long.MAX_VALUE)));
        } catch(Exception e) { return Optional.empty(); }
    }
    public byte[] encode(Heartbeat heartbeat) {
        try {
            byte[] bytes=JsonSupport.MAPPER.writeValueAsBytes(heartbeat);
            if(decode(bytes).isEmpty()) throw new IllegalArgumentException();
            return bytes;
        } catch(Exception e) { throw new IllegalArgumentException("Invalid heartbeat"); }
    }
}
