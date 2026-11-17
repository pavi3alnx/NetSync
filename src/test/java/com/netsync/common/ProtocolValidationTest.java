package com.netsync.common;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.*;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

class ProtocolValidationTest {
    final MessageCodec codec = new MessageCodec();

    @ParameterizedTest @MethodSource("com.netsync.common.MessageCodecTest#bodies")
    void everyBodyRejectsUnknownFields(MessageType type, MessageBody body) throws Exception {
        var message = new MessageEnvelope(1, type, MessageCodecTest.ID, MessageCodecTest.ID, body);
        ObjectNode tree = (ObjectNode) JsonSupport.MAPPER.readTree(codec.encode(message));
        ((ObjectNode) tree.get("body")).put("unexpected", true);
        assertEquals(Optional.of(ErrorCode.INVALID_REQUEST),
                assertThrows(ProtocolException.class, () -> codec.decode(JsonSupport.MAPPER.writeValueAsBytes(tree))).code());
    }

    @ParameterizedTest @MethodSource("com.netsync.common.MessageCodecTest#bodies")
    void everyRequiredBodyFieldRejectsMissingOrNull(MessageType type, MessageBody body) throws Exception {
        var message = new MessageEnvelope(1, type, MessageCodecTest.ID, MessageCodecTest.ID, body);
        ObjectNode original = (ObjectNode) JsonSupport.MAPPER.readTree(codec.encode(message));
        List<String> fields = new ArrayList<>();
        original.get("body").fieldNames().forEachRemaining(fields::add);
        for (String field : fields) {
            if (field.equals("reason") || ((type == MessageType.ACK || type == MessageType.ERROR) && field.equals("transferId"))) continue;
            for (boolean explicitNull : new boolean[]{false, true}) {
                ObjectNode tree = original.deepCopy();
                ObjectNode b = (ObjectNode) tree.get("body");
                if (explicitNull) b.putNull(field); else b.remove(field);
                assertEquals(Optional.of(ErrorCode.INVALID_REQUEST),
                        assertThrows(ProtocolException.class, () -> codec.decode(JsonSupport.MAPPER.writeValueAsBytes(tree)), field).code());
            }
        }
    }

    @TestFactory Stream<DynamicTest> valueBoundaries() {
        UUIDHolder h = new UUIDHolder();
        List<MessageEnvelope> invalid = List.of(
            h.of(MessageType.LOGIN_REQUEST, new MessageBody.LoginRequest("a".repeat(33), "dummy")),
            h.of(MessageType.LOGIN_REQUEST, new MessageBody.LoginRequest("", "dummy")),
            h.of(MessageType.LOGIN_REQUEST, new MessageBody.LoginRequest("alice", "p".repeat(129))),
            h.of(MessageType.LOGIN_RESPONSE, new MessageBody.LoginResponse(h.id, "alice", "bad", 5, 15)),
            h.of(MessageType.LOGIN_RESPONSE, new MessageBody.LoginResponse(h.id, "alice", MessageCodecTest.TOKEN, 6, 15)),
            h.of(MessageType.LOGIN_RESPONSE, new MessageBody.LoginResponse(h.id, "alice", MessageCodecTest.TOKEN, 5, 16)),
            h.of(MessageType.FILE_OFFER, new MessageBody.FileOffer(h.id, "a".repeat(129), 0)),
            h.of(MessageType.FILE_OFFER, new MessageBody.FileOffer(h.id, "a\\b", 0)),
            h.of(MessageType.FILE_OFFER, new MessageBody.FileOffer(h.id, "a\nb", 0)),
            h.of(MessageType.FILE_COMPLETE, new MessageBody.FileComplete(h.id, -1, "a".repeat(64))),
            h.of(MessageType.FILE_RECEIVED, new MessageBody.FileReceived(h.id, "a".repeat(63))),
            h.of(MessageType.CHAT_SEND, new MessageBody.ChatSend(h.id, "")));
        return java.util.stream.IntStream.range(0, invalid.size()).mapToObj(i ->
            DynamicTest.dynamicTest("invalid value " + i, () -> assertThrows(ProtocolException.class, () -> codec.encode(invalid.get(i)))));
    }

    @Test void validValueEdges() throws Exception {
        UUIDHolder h = new UUIDHolder();
        for (MessageEnvelope m : List.of(
                h.of(MessageType.LOGIN_REQUEST, new MessageBody.LoginRequest("a".repeat(32), "p".repeat(128))),
                h.of(MessageType.FILE_OFFER, new MessageBody.FileOffer(h.id, "a".repeat(128), 10485760)),
                h.of(MessageType.FILE_COMPLETE, new MessageBody.FileComplete(h.id, 640, "a".repeat(64))),
                h.of(MessageType.FILE_STATUS, new MessageBody.FileStatus(h.id, FileStatusState.FAILED, "Safe reason"))))
            assertEquals(m, codec.decode(codec.encode(m)));
    }

    @Test void nestedSnapshotSchemaAndDuplicateBodyKeys() throws Exception {
        var h = new UUIDHolder();
        var message = h.of(MessageType.PRESENCE_SNAPSHOT, new MessageBody.PresenceSnapshot(List.of(
                new MessageBody.PresenceEntry(h.id, "alice", PresenceState.ONLINE))));
        ObjectNode tree = (ObjectNode) JsonSupport.MAPPER.readTree(codec.encode(message));
        ((ObjectNode) tree.get("body").get("clients").get(0)).put("state", "AWAY");
        assertThrows(ProtocolException.class, () -> codec.decode(JsonSupport.MAPPER.writeValueAsBytes(tree)));
        ((ObjectNode) tree.get("body").get("clients").get(0)).put("state", "ONLINE").put("extra", 1);
        assertThrows(ProtocolException.class, () -> codec.decode(JsonSupport.MAPPER.writeValueAsBytes(tree)));
        String duplicate = MessageCodecTest.json("LOGIN_REQUEST", "{\"username\":\"alice\",\"password\":\"dummy\",\"password\":\"other\"}");
        assertEquals(Optional.of(ErrorCode.MALFORMED_MESSAGE),
                assertThrows(ProtocolException.class, () -> codec.decode(MessageCodecTest.bytes(duplicate))).code());
    }

    @Test void nestingSixteenParsesButSeventeenFails() throws Exception {
        String atLimit = "{\"x\":" + "[".repeat(15) + "0" + "]".repeat(15) + "}";
        assertNotNull(JsonSupport.parse(MessageCodecTest.bytes(atLimit), 65536));
        String tooDeep = "{\"x\":" + "[".repeat(16) + "0" + "]".repeat(16) + "}";
        assertThrows(Exception.class, () -> JsonSupport.parse(MessageCodecTest.bytes(tooDeep), 65536));
    }

    private static final class UUIDHolder {
        final java.util.UUID id = MessageCodecTest.ID;
        MessageEnvelope of(MessageType t, MessageBody b) { return new MessageEnvelope(1, t, id, id, b); }
    }
}
