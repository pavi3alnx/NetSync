package com.netsync.transfer;

import com.netsync.common.ProtocolLimits;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * SHA-256 file checksum calculation and verification utilities.
 * <p>
 * File hashing and streaming digest calculations must be performed on worker threads,
 * never on the NIO Selector thread.
 */
public final class TransferChecksum {
    /**
     * SHA-256 digest of 0 bytes (empty file):
     * {@code e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855}
     */
    public static final String EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private TransferChecksum() {}

    /**
     * Computes the SHA-256 checksum of an in-memory byte array as a 64-character lowercase hex string.
     */
    public static String computeDigestHex(byte[] data) {
        Objects.requireNonNull(data, "Data must not be null");
        MessageDigest md = createDigest();
        md.update(data);
        return HexFormat.of().formatHex(md.digest());
    }

    /**
     * Computes the SHA-256 checksum of a file on disk by streaming it in chunks.
     * <p>
     * MUST be called from a worker thread to prevent blocking the Selector.
     *
     * @param file path to the file
     * @return 64-character lowercase hexadecimal hash
     * @throws IOException if an I/O error occurs
     */
    public static String computeFileDigestHex(Path file) throws IOException {
        Objects.requireNonNull(file, "File must not be null");
        MessageDigest md = createDigest();
        byte[] buffer = new byte[ProtocolLimits.MAX_CHUNK_BYTES];

        try (InputStream in = Files.newInputStream(file)) {
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                if (bytesRead > 0) {
                    md.update(buffer, 0, bytesRead);
                }
            }
        }
        return HexFormat.of().formatHex(md.digest());
    }

    /**
     * Safely compares two SHA-256 hexadecimal digests in a case-insensitive manner.
     */
    public static boolean matches(String expectedHex, String actualHex) {
        if (expectedHex == null || actualHex == null) return false;
        if (expectedHex.length() != 64 || actualHex.length() != 64) return false;
        return expectedHex.equalsIgnoreCase(actualHex);
    }

    /**
     * Creates a new SHA-256 {@link MessageDigest} instance.
     */
    public static MessageDigest createDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }

    /**
     * Stateful streaming checksum calculator for progressive chunk hashing.
     */
    public static final class Incremental {
        private final MessageDigest digest;
        private String finalizedHex;

        public Incremental() {
            this.digest = createDigest();
        }

        public void update(byte[] data) {
            Objects.requireNonNull(data, "Data must not be null");
            update(data, 0, data.length);
        }

        public void update(byte[] data, int offset, int length) {
            Objects.requireNonNull(data, "Data must not be null");
            if (finalizedHex != null) {
                throw new IllegalStateException("Cannot update checksum after digest has been finalized");
            }
            digest.update(data, offset, length);
        }

        public String digestHex() {
            if (finalizedHex == null) {
                finalizedHex = HexFormat.of().formatHex(digest.digest());
            }
            return finalizedHex;
        }
    }
}
