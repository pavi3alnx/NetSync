package com.netsync.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.nio.*;
import java.nio.charset.*;
import java.util.*;

/** Internal strict JSON primitives; mapper is configured once and never exposed. */
final class JsonSupport {
    static final ObjectMapper MAPPER = JsonMapper.builder(JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(16).build())
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
            .defaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS))
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    private JsonSupport() {}
    static JsonNode parse(byte[] data, int max) throws Exception {
        if(data==null || data.length<1 || data.length>max) throw new IllegalArgumentException();
        String text=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(data)).toString();
        JsonNode node=MAPPER.readTree(text);
        if(node==null || !node.isObject()) throw new IllegalArgumentException();
        checkUnicode(node); return node;
    }
    static void checkUnicode(JsonNode node) {
        if(node.isTextual()) utf8(node.textValue());
        if(node.isObject()) node.fieldNames().forEachRemaining(JsonSupport::utf8);
        if(node.isContainerNode()) node.forEach(JsonSupport::checkUnicode);
    }
    static byte[] utf8(String text) {
        try {
            ByteBuffer b=StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(text));
            byte[] bytes=new byte[b.remaining()]; b.get(bytes); return bytes;
        } catch(CharacterCodingException e) { throw new IllegalArgumentException("Invalid Unicode"); }
    }
    static void fields(JsonNode node, String... allowed) {
        if(!node.isObject()) throw new IllegalArgumentException();
        Set<String> names=Set.of(allowed);
        node.fieldNames().forEachRemaining(n->{ if(!names.contains(n)) throw new IllegalArgumentException(); });
    }
    static String text(JsonNode n) {
        if(n==null || !n.isTextual()) throw new IllegalArgumentException(); return n.textValue();
    }
    static UUID uuid(JsonNode n) {
        String s=text(n);
        if(!s.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
            throw new IllegalArgumentException();
        return UUID.fromString(s);
    }
    static long integer(JsonNode n, long max) {
        if(n==null || !n.isIntegralNumber() || !n.canConvertToLong() || n.longValue()<0 || n.longValue()>max)
            throw new IllegalArgumentException();
        return n.longValue();
    }
    static void token(String value) {
        byte[] bytes=Base64.getDecoder().decode(value);
        if(bytes.length!=32 || !Base64.getEncoder().encodeToString(bytes).equals(value)) throw new IllegalArgumentException();
    }
    static void username(String value) {
        if(!value.matches("[A-Za-z0-9_]{1,32}")) throw new IllegalArgumentException();
    }
}
