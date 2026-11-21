package com.netsync.security;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

/** Generates ephemeral protocol-compatible presence tokens. */
public final class SessionTokenGenerator {
    private static final int TOKEN_BYTES = 32;

    private final SecureRandom secureRandom;

    public SessionTokenGenerator() {
        this(new SecureRandom());
    }

    SessionTokenGenerator(SecureRandom secureRandom) {
        this.secureRandom = Objects.requireNonNull(secureRandom);
    }

    /** Generates a fresh, canonical standard-Base64 token from 32 random bytes. */
    public String generateToken() {
        byte[] tokenBytes = new byte[TOKEN_BYTES];
        try {
            secureRandom.nextBytes(tokenBytes);
            return Base64.getEncoder().encodeToString(tokenBytes);
        } finally {
            Arrays.fill(tokenBytes, (byte) 0);
        }
    }

    /**
     * Returns whether a token has the exact canonical Base64 representation accepted
     * by the heartbeat codec. This validates token shape only, not session activity.
     */
    public boolean isValidToken(String token) {
        if (token == null || token.isEmpty()) {
            return false;
        }
        try {
            byte[] decoded = Base64.getDecoder().decode(token);
            return decoded.length == TOKEN_BYTES
                    && Base64.getEncoder().encodeToString(decoded).equals(token);
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }
}
