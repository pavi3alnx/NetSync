package com.netsync.security;

import com.netsync.common.Heartbeat;
import com.netsync.common.HeartbeatCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionTokenGeneratorTest {
    private final SessionTokenGenerator generator = new SessionTokenGenerator();
    private final HeartbeatCodec heartbeatCodec = new HeartbeatCodec();

    @Test
    void generatesCanonicalStandardBase64OfExactly32Bytes() {
        String token = generator.generateToken();
        byte[] decoded = Base64.getDecoder().decode(token);

        assertEquals(44, token.length());
        assertEquals(32, decoded.length);
        assertEquals(token, Base64.getEncoder().encodeToString(decoded));
        assertTrue(generator.isValidToken(token));
    }

    @Test
    void generatesDistinctTokens() {
        Set<String> tokens = new HashSet<>();

        for (int i = 0; i < 1_000; i++) {
            tokens.add(generator.generateToken());
        }

        assertEquals(1_000, tokens.size());
    }

    @Test
    void generatedTokenMatchesHeartbeatCodecValidationRules() {
        String token = generator.generateToken();
        Heartbeat heartbeat = new Heartbeat(1, "HEARTBEAT", UUID.randomUUID(), token, 1);
        byte[] datagram = heartbeatCodec.encode(heartbeat);

        assertTrue(generator.isValidToken(token));
        assertTrue(heartbeatCodec.decode(datagram).isPresent());
    }

    @Test
    void rejectsNullEmptyMalformedAndWrongLengthTokens() {
        assertFalse(generator.isValidToken(null));
        assertFalse(generator.isValidToken(""));
        assertFalse(generator.isValidToken("not-base64"));
        assertFalse(generator.isValidToken(Base64.getEncoder().encodeToString(new byte[31])));
        assertFalse(generator.isValidToken(Base64.getEncoder().encodeToString(new byte[33])));
    }

    @Test
    void rejectsNoncanonicalBase64Forms() {
        String token = generator.generateToken();

        assertTrue(token.endsWith("="));
        assertFalse(generator.isValidToken(token.substring(0, token.length() - 1)));
    }

    @Test
    void rejectsWhitespaceAndUrlSafeBase64() {
        String token = generator.generateToken();
        String withWhitespace = token.substring(0, 4) + "\n" + token.substring(4);
        byte[] bytes = new byte[32];
        Arrays.fill(bytes, (byte) 0xff);
        String urlSafe = Base64.getUrlEncoder().encodeToString(bytes);

        assertFalse(generator.isValidToken(withWhitespace));
        assertFalse(generator.isValidToken(urlSafe));
    }
}
