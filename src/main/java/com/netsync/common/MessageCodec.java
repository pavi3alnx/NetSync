package com.netsync.common;

import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.RecordComponent;
import java.util.*;

/** Thread-safe strict version 1 JSON controls. No network/session/business state is retained. */
public final class MessageCodec {
    public byte[] encode(MessageEnvelope message) throws ProtocolException {
        try {
            byte[] bytes=JsonSupport.MAPPER.writeValueAsBytes(message);
            decode(bytes); return bytes;
        } catch(ProtocolException e) { throw e; }
        catch(Exception e) { throw failure(ErrorCode.INVALID_REQUEST,null); }
    }
    public MessageEnvelope decode(byte[] json) throws ProtocolException {
        JsonNode root;
        try { root=JsonSupport.parse(json,ProtocolLimits.MAX_JSON_BYTES); }
        catch(Exception e) { throw failure(ErrorCode.MALFORMED_MESSAGE,null); }
        UUID id=null;
        try { id=JsonSupport.uuid(root.get("messageId")); } catch(RuntimeException ignored) { }
        int version; String typeName; UUID reply=null;
        try {
            JsonSupport.fields(root,"version","type","messageId","replyTo","body");
            if(id==null || !root.has("body") || !root.get("body").isObject()) throw new IllegalArgumentException();
            // Version is an integer, not a coercible number/string.
            JsonNode v=root.get("version");
            if(v==null || !v.isIntegralNumber() || !v.canConvertToInt()) throw new IllegalArgumentException();
            version=v.intValue(); typeName=JsonSupport.text(root.get("type"));
            if(root.has("replyTo")) reply=JsonSupport.uuid(root.get("replyTo"));
        } catch(RuntimeException e) { throw failure(ErrorCode.MALFORMED_MESSAGE,id); }
        if(version!=1) throw failure(ErrorCode.UNSUPPORTED_VERSION,id);
        MessageType type;
        try { type=MessageType.valueOf(typeName); }
        catch(IllegalArgumentException e) { throw failure(ErrorCode.UNKNOWN_TYPE,id); }
        if(type==MessageType.FILE_CHUNK) throw failure(ErrorCode.INVALID_REQUEST,id);
        try {
            Class<? extends MessageBody> bodyClass=bodyClass(type);
            JsonNode body=root.get("body");
            validateRecord(body,bodyClass);
            MessageBody decoded=JsonSupport.MAPPER.treeToValue(body,bodyClass);
            validateBody(decoded);
            if((type==MessageType.ACK || type==MessageType.LOGIN_RESPONSE || type==MessageType.PRESENCE_SNAPSHOT) && reply==null)
                throw new IllegalArgumentException();
            return new MessageEnvelope(version,type,id,reply,decoded);
        } catch(Exception e) { throw failure(ErrorCode.INVALID_REQUEST,id); }
    }
    private static Class<? extends MessageBody> bodyClass(MessageType type) {
        return switch(type) {
            case LOGIN_REQUEST -> MessageBody.LoginRequest.class;
            case LOGIN_RESPONSE -> MessageBody.LoginResponse.class;
            case LOGOUT_REQUEST, PRESENCE_QUERY -> MessageBody.Empty.class;
            case CHAT_SEND -> MessageBody.ChatSend.class;
            case CHAT_DELIVER -> MessageBody.ChatDeliver.class;
            case PRESENCE_SNAPSHOT -> MessageBody.PresenceSnapshot.class;
            case PRESENCE_UPDATE -> MessageBody.PresenceUpdate.class;
            case FILE_OFFER -> MessageBody.FileOffer.class;
            case FILE_OFFERED -> MessageBody.FileOffered.class;
            case FILE_ACCEPT, FILE_REJECT, FILE_CANCEL -> MessageBody.TransferReference.class;
            case FILE_COMPLETE -> MessageBody.FileComplete.class;
            case FILE_RECEIVED -> MessageBody.FileReceived.class;
            case FILE_STATUS -> MessageBody.FileStatus.class;
            case ACK -> MessageBody.Ack.class;
            case ERROR -> MessageBody.Error.class;
            case FILE_CHUNK -> throw new IllegalArgumentException();
        };
    }
    private static void validateRecord(JsonNode node, Class<?> cls) {
        RecordComponent[] components=cls.getRecordComponents();
        JsonSupport.fields(node,Arrays.stream(components).map(RecordComponent::getName).toArray(String[]::new));
        for(RecordComponent c:components) {
            String name=c.getName(); JsonNode value=node.get(name);
            boolean optional=(cls==MessageBody.FileStatus.class && name.equals("reason"))
                || ((cls==MessageBody.Ack.class || cls==MessageBody.Error.class) && name.equals("transferId"));
            if(value==null && optional) continue;
            Class<?> t=c.getType();
            if(t==String.class) JsonSupport.text(value);
            else if(t==UUID.class) JsonSupport.uuid(value);
            else if(t==int.class) JsonSupport.integer(value,Integer.MAX_VALUE);
            else if(t==long.class) JsonSupport.integer(value,Long.MAX_VALUE);
            else if(t.isEnum()) {
                String v=JsonSupport.text(value);
                if(Arrays.stream(t.getEnumConstants()).noneMatch(e->((Enum<?>)e).name().equals(v))) throw new IllegalArgumentException();
            } else if(t==List.class) {
                if(value==null || !value.isArray()) throw new IllegalArgumentException();
                value.forEach(entry->validateRecord(entry,MessageBody.PresenceEntry.class));
            }
            if(name.equals("username")) JsonSupport.username(JsonSupport.text(value));
            if(name.equals("presenceToken")) JsonSupport.token(JsonSupport.text(value));
            if(name.equals("text")) {
                int len=JsonSupport.utf8(JsonSupport.text(value)).length;
                if(len<1 || len>ProtocolLimits.MAX_CHAT_BYTES) throw new IllegalArgumentException();
            }
            if(name.equals("password")) {
                String s=JsonSupport.text(value);
                if(s.isEmpty() || s.codePointCount(0,s.length())>128) throw new IllegalArgumentException();
            }
            if(name.equals("fileName")) {
                String s=JsonSupport.text(value);
                if(s.codePointCount(0,s.length())>128 || s.indexOf('/')>=0 || s.indexOf('\\')>=0
                    || s.codePoints().anyMatch(Character::isISOControl)) throw new IllegalArgumentException();
            }
            if(name.equals("sizeBytes")) JsonSupport.integer(value,ProtocolLimits.MAX_FILE_BYTES);
            if(name.equals("sha256") && !JsonSupport.text(value).matches("[0-9a-f]{64}")) throw new IllegalArgumentException();
        }
    }
    private static void validateBody(MessageBody body) {
        if(body instanceof MessageBody.LoginResponse b && (b.heartbeatIntervalSeconds()!=5 || b.presenceTimeoutSeconds()!=15))
            throw new IllegalArgumentException();
        if(body instanceof MessageBody.Ack b) {
            AckStatus expected=switch(b.operation()) {
                case CHAT_SEND -> AckStatus.QUEUED;
                case FILE_CHUNK -> AckStatus.STORED;
                case FILE_OFFER, FILE_ACCEPT, FILE_REJECT, FILE_CANCEL, FILE_COMPLETE, FILE_RECEIVED, LOGOUT_REQUEST -> AckStatus.ACCEPTED;
                default -> throw new IllegalArgumentException();
            };
            if(b.status()!=expected || (b.operation().name().startsWith("FILE_") && b.transferId()==null))
                throw new IllegalArgumentException();
        }
    }
    private static ProtocolException failure(ErrorCode code,UUID id) {
        boolean close=code==ErrorCode.MALFORMED_MESSAGE || code==ErrorCode.UNSUPPORTED_VERSION;
        return new ProtocolException(code,id,close ? ProtocolException.Action.ERROR_THEN_CLOSE : ProtocolException.Action.KEEP_OPEN);
    }
}
