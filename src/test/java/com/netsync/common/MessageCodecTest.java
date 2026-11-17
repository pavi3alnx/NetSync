package com.netsync.common;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

class MessageCodecTest {
    static final UUID ID=UUID.fromString("550e8400-e29b-41d4-a716-446655440001");
    static final String TOKEN=Base64.getEncoder().encodeToString(new byte[32]);
    final MessageCodec codec=new MessageCodec();
    static String json(String type,String body) {
        return "{\"version\":1,\"type\":\""+type+"\",\"messageId\":\""+ID+"\",\"body\":"+body+"}";
    }
    static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static Stream<Arguments> bodies() {
        return Stream.of(
            Arguments.of(MessageType.LOGIN_REQUEST,new MessageBody.LoginRequest("alice","dummy")),
            Arguments.of(MessageType.LOGIN_RESPONSE,new MessageBody.LoginResponse(ID,"alice",TOKEN,5,15)),
            Arguments.of(MessageType.LOGOUT_REQUEST,new MessageBody.Empty()),
            Arguments.of(MessageType.CHAT_SEND,new MessageBody.ChatSend(ID,"Hello \uD83D\uDE00")),
            Arguments.of(MessageType.CHAT_DELIVER,new MessageBody.ChatDeliver(ID,"hi",ID)),
            Arguments.of(MessageType.PRESENCE_QUERY,new MessageBody.Empty()),
            Arguments.of(MessageType.PRESENCE_SNAPSHOT,new MessageBody.PresenceSnapshot(List.of(new MessageBody.PresenceEntry(ID,"alice",PresenceState.OFFLINE)))),
            Arguments.of(MessageType.PRESENCE_UPDATE,new MessageBody.PresenceUpdate(ID,"alice",PresenceState.ONLINE)),
            Arguments.of(MessageType.FILE_OFFER,new MessageBody.FileOffer(ID,"demo.bin",10485760)),
            Arguments.of(MessageType.FILE_OFFERED,new MessageBody.FileOffered(ID,ID,"empty",0)),
            Arguments.of(MessageType.FILE_ACCEPT,new MessageBody.TransferReference(ID)),
            Arguments.of(MessageType.FILE_REJECT,new MessageBody.TransferReference(ID)),
            Arguments.of(MessageType.FILE_CANCEL,new MessageBody.TransferReference(ID)),
            Arguments.of(MessageType.FILE_COMPLETE,new MessageBody.FileComplete(ID,0,"0".repeat(64))),
            Arguments.of(MessageType.FILE_RECEIVED,new MessageBody.FileReceived(ID,"a".repeat(64))),
            Arguments.of(MessageType.FILE_STATUS,new MessageBody.FileStatus(ID,FileStatusState.COMPLETED,null)),
            Arguments.of(MessageType.ACK,new MessageBody.Ack(MessageType.FILE_CHUNK,AckStatus.STORED,ID)),
            Arguments.of(MessageType.ERROR,new MessageBody.Error(ErrorCode.BUSY,"Try later",ID)));
    }
    @ParameterizedTest @MethodSource("bodies") void allSchemasRoundTrip(MessageType type,MessageBody body) throws Exception {
        var m=new MessageEnvelope(1,type,ID,ID,body);
        assertEquals(m,codec.decode(codec.encode(m)));
    }
    @Test void documentedLoginAndOptionalReply() throws Exception {
        var m=codec.decode(bytes(json("LOGIN_REQUEST","{\"username\":\"alice\",\"password\":\"demo-only-secret\"}")));
        assertNull(m.replyTo()); assertFalse(new String(codec.encode(m),StandardCharsets.UTF_8).contains("replyTo"));
    }
    static Stream<Arguments> invalid() {
        String empty=json("LOGOUT_REQUEST","{}");
        return Stream.of(
            Arguments.of("{",ErrorCode.MALFORMED_MESSAGE),
            Arguments.of("[]",ErrorCode.MALFORMED_MESSAGE),
            Arguments.of(empty+" {}",ErrorCode.MALFORMED_MESSAGE),
            Arguments.of(empty.replace("\"version\":1","\"version\":1,\"version\":1"),ErrorCode.MALFORMED_MESSAGE),
            Arguments.of(empty.replace("\"version\":1","\"version\":1.0"),ErrorCode.MALFORMED_MESSAGE),
            Arguments.of(empty.replace("\"version\":1","\"version\":2"),ErrorCode.UNSUPPORTED_VERSION),
            Arguments.of(empty.replace("LOGOUT_REQUEST","FUTURE"),ErrorCode.UNKNOWN_TYPE),
            Arguments.of(empty.replace("LOGOUT_REQUEST","FILE_CHUNK"),ErrorCode.INVALID_REQUEST),
            Arguments.of(empty.replace("\"body\":{}","\"body\":[]"),ErrorCode.MALFORMED_MESSAGE),
            Arguments.of(empty.replace("\"body\":{}","\"body\":{},\"extra\":0"),ErrorCode.MALFORMED_MESSAGE),
            Arguments.of(json("LOGOUT_REQUEST","{\"extra\":0}"),ErrorCode.INVALID_REQUEST),
            Arguments.of(json("CHAT_SEND","{\"recipientClientId\":\""+ID+"\",\"text\":null}"),ErrorCode.INVALID_REQUEST),
            Arguments.of(json("CHAT_SEND","{\"recipientClientId\":\""+ID+"\",\"text\":42}"),ErrorCode.INVALID_REQUEST),
            Arguments.of(json("LOGIN_REQUEST","{\"username\":\"bad-name\",\"password\":\"dummy\"}"),ErrorCode.INVALID_REQUEST),
            Arguments.of(json("LOGIN_REQUEST","{\"username\":\"alice\",\"password\":\"\"}"),ErrorCode.INVALID_REQUEST),
            Arguments.of(json("LOGIN_REQUEST","{\"username\":\"alice\"}"),ErrorCode.INVALID_REQUEST),
            Arguments.of(json("FILE_OFFER","{\"recipientClientId\":\""+ID+"\",\"fileName\":\"../bad\",\"sizeBytes\":0}"),ErrorCode.INVALID_REQUEST),
            Arguments.of(json("FILE_OFFER","{\"recipientClientId\":\""+ID+"\",\"fileName\":\"a\",\"sizeBytes\":10485761}"),ErrorCode.INVALID_REQUEST),
            Arguments.of(json("FILE_OFFER","{\"recipientClientId\":\""+ID+"\",\"fileName\":\"a\",\"sizeBytes\":1.0}"),ErrorCode.INVALID_REQUEST),
            Arguments.of(json("FILE_OFFER","{\"recipientClientId\":\""+ID+"\",\"fileName\":\"a\",\"sizeBytes\":-1}"),ErrorCode.INVALID_REQUEST),
            Arguments.of(json("FILE_COMPLETE","{\"transferId\":\""+ID+"\",\"chunkCount\":0,\"sha256\":\""+"A".repeat(64)+"\"}"),ErrorCode.INVALID_REQUEST),
            Arguments.of(json("ACK","{\"operation\":\"CHAT_SEND\",\"status\":\"QUEUED\"}"),ErrorCode.INVALID_REQUEST),
            Arguments.of(empty.replace(ID.toString(),"1-1-1-1-1"),ErrorCode.MALFORMED_MESSAGE),
            Arguments.of(empty.replace("\"body\":{}","\"body\":{\"x\":"+"[".repeat(17)+"0"+"]".repeat(17)+"}"),ErrorCode.MALFORMED_MESSAGE));
    }
    @ParameterizedTest @MethodSource("invalid") void errorClassification(String input,ErrorCode code) {
        ProtocolException e=assertThrows(ProtocolException.class,()->codec.decode(bytes(input)));
        assertEquals(Optional.of(code),e.code());
        assertEquals(code==ErrorCode.MALFORMED_MESSAGE || code==ErrorCode.UNSUPPORTED_VERSION
            ? ProtocolException.Action.ERROR_THEN_CLOSE : ProtocolException.Action.KEEP_OPEN,e.action());
        assertFalse(e.getMessage().contains("dummy")); assertNull(e.getCause());
    }
    @Test void invalidUtf8AndSurrogate() {
        assertThrows(ProtocolException.class,()->codec.decode(new byte[]{(byte)0xc0,(byte)0xaf}));
        String bad=json("CHAT_SEND","{\"recipientClientId\":\""+ID+"\",\"text\":\""+"\\uD800"+"\"}");
        assertEquals(Optional.of(ErrorCode.MALFORMED_MESSAGE),assertThrows(ProtocolException.class,()->codec.decode(bytes(bad))).code());
    }
    @Test void chatByteLimit() throws Exception {
        var valid=new MessageEnvelope(1,MessageType.CHAT_SEND,ID,null,new MessageBody.ChatSend(ID,"\u00e9".repeat(2048)));
        assertEquals(valid,codec.decode(codec.encode(valid)));
        assertThrows(ProtocolException.class,()->codec.encode(new MessageEnvelope(1,MessageType.CHAT_SEND,ID,null,new MessageBody.ChatSend(ID,"\u00e9".repeat(2049)))));
    }
    @Test void jsonExactByteLimit() throws Exception {
        String base=json("ERROR","{\"code\":\"BUSY\",\"message\":\"\"}");
        String exact=base.replace("\"message\":\"\"","\"message\":\""+"x".repeat(65536-bytes(base).length)+"\"");
        assertEquals(65536,bytes(exact).length); assertNotNull(codec.decode(bytes(exact)));
        assertThrows(ProtocolException.class,()->codec.decode(bytes(exact+" ")));
    }
    @Test void bodyMismatchAndAckSemantics() {
        assertThrows(ProtocolException.class,()->codec.encode(new MessageEnvelope(1,MessageType.CHAT_SEND,ID,null,new MessageBody.Empty())));
        for(var b:List.of(new MessageBody.Ack(MessageType.FILE_OFFER,AckStatus.ACCEPTED,null),
            new MessageBody.Ack(MessageType.CHAT_SEND,AckStatus.STORED,null),
            new MessageBody.Ack(MessageType.PRESENCE_QUERY,AckStatus.ACCEPTED,null))) {
            assertThrows(ProtocolException.class,()->codec.encode(new MessageEnvelope(1,MessageType.ACK,ID,ID,b)));
        }
    }
    @Test void replyMetadata() {
        var e=assertThrows(ProtocolException.class,()->codec.decode(bytes(json("FUTURE","{}"))));
        assertEquals(Optional.of(ID),e.replyTo());
        e=assertThrows(ProtocolException.class,()->codec.decode(bytes(json("FUTURE","{}").replace(ID.toString(),"bad"))));
        assertTrue(e.replyTo().isEmpty());
    }
}
