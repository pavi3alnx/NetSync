package com.netsync.transfer;

import com.netsync.common.ProtocolLimits;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TransferSessionTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Initial session state is OFFERED and constructor invariants are enforced")
    void constructorInvariants() {
        UUID sender = UUID.randomUUID();
        UUID recipient = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();
        UUID conn = UUID.randomUUID();
        long now = 1000L;

        TransferSession session = new TransferSession(transferId, sender, recipient, "test.dat", 1000, conn, now);

        assertEquals(transferId, session.transferId());
        assertEquals(sender, session.senderClientId());
        assertEquals(recipient, session.recipientClientId());
        assertEquals("test.dat", session.fileName());
        assertEquals(1000, session.declaredSizeBytes());
        assertEquals(conn, session.senderConnectionId());
        assertEquals(TransferState.OFFERED, session.state());
        assertTrue(session.state().isActive());
        assertFalse(session.state().isTerminal());
        assertEquals(0, session.expectedSequence());
        assertEquals(0L, session.receivedBytes());
        assertEquals(0, session.chunkCount());

        // Self-transfers forbidden
        assertThrows(IllegalArgumentException.class,
                () -> new TransferSession(UUID.randomUUID(), sender, sender, "test.dat", 100, conn, now));

        // Negative size rejected
        assertThrows(IllegalArgumentException.class,
                () -> new TransferSession(UUID.randomUUID(), sender, recipient, "test.dat", -1, conn, now));

        // Exceeding 10 MiB rejected
        assertThrows(IllegalArgumentException.class,
                () -> new TransferSession(UUID.randomUUID(), sender, recipient, "test.dat", ProtocolLimits.MAX_FILE_BYTES + 1, conn, now));
    }

    @Test
    @DisplayName("Valid lifecycle state transitions")
    void validStateTransitions() {
        TransferSession session = new TransferSession(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "test.dat", 1024, UUID.randomUUID(), 1000L);

        // OFFERED -> ACCEPTED
        session.transitionTo(TransferState.ACCEPTED, 1010L);
        assertEquals(TransferState.ACCEPTED, session.state());

        // ACCEPTED -> IN_PROGRESS
        session.transitionTo(TransferState.IN_PROGRESS, 1020L);
        assertEquals(TransferState.IN_PROGRESS, session.state());

        // IN_PROGRESS -> SERVER_VERIFIED
        session.transitionTo(TransferState.SERVER_VERIFIED, 1030L);
        assertEquals(TransferState.SERVER_VERIFIED, session.state());

        // SERVER_VERIFIED -> COMPLETED
        session.transitionTo(TransferState.COMPLETED, 1040L);
        assertEquals(TransferState.COMPLETED, session.state());
        assertTrue(session.state().isTerminal());
        assertFalse(session.state().isActive());
    }

    @Test
    @DisplayName("Invalid state transitions are rejected")
    void invalidStateTransitions() {
        TransferSession session = new TransferSession(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "test.dat", 1024, UUID.randomUUID(), 1000L);

        // Cannot skip directly to COMPLETED from OFFERED
        assertThrows(IllegalStateException.class, () -> session.transitionTo(TransferState.COMPLETED, 1010L));

        // Cancel the session
        session.transitionTo(TransferState.CANCELLED, 1020L);
        assertTrue(session.state().isTerminal());

        // Terminal state cannot transition anywhere
        assertThrows(IllegalStateException.class, () -> session.transitionTo(TransferState.ACCEPTED, 1030L));
        assertThrows(IllegalStateException.class, () -> session.transitionTo(TransferState.COMPLETED, 1040L));
        assertThrows(IllegalStateException.class, () -> session.transitionTo(TransferState.FAILED, 1050L));
    }

    @Test
    @DisplayName("Stop-and-wait chunk sequence validation, intermediate chunk enforcement and byte accumulation")
    void chunkSequenceValidation() {
        TransferSession session = new TransferSession(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "test.dat", 20_000, UUID.randomUUID(), 1000L);

        // Cannot receive chunk while in OFFERED state
        assertThrows(IllegalStateException.class, () -> session.validateAndRecordChunk(0, ProtocolLimits.MAX_CHUNK_BYTES, 1010L));

        session.transitionTo(TransferState.ACCEPTED, 1010L);

        // Out of order chunk (sequence 1 instead of 0) rejected
        assertThrows(IllegalStateException.class, () -> session.validateAndRecordChunk(1, ProtocolLimits.MAX_CHUNK_BYTES, 1020L));

        // Short intermediate chunk (10,000 bytes when 20,000 declared) must be rejected
        assertThrows(IllegalStateException.class, () -> session.validateAndRecordChunk(0, 10_000, 1020L));

        // Valid sequence 0 (must be full 16,384 bytes as non-final chunk)
        session.validateAndRecordChunk(0, ProtocolLimits.MAX_CHUNK_BYTES, 1020L);
        assertEquals(TransferState.IN_PROGRESS, session.state());
        assertEquals(1, session.expectedSequence());
        assertEquals(16_384, session.receivedBytes());
        assertEquals(1, session.chunkCount());

        // Duplicate sequence 0 rejected
        assertThrows(IllegalStateException.class, () -> session.validateAndRecordChunk(0, 3616, 1030L));

        // Oversized chunk (> 16 KiB) rejected
        assertThrows(IllegalArgumentException.class, () -> session.validateAndRecordChunk(1, 16385, 1030L));

        // Valid final chunk (sequence 1, remaining 3616 bytes)
        session.validateAndRecordChunk(1, 3616, 1030L);
        assertEquals(2, session.expectedSequence());
        assertEquals(20_000, session.receivedBytes());
        assertEquals(2, session.chunkCount());

        // Exceeding declared size (20,000 bytes) rejected
        assertThrows(IllegalStateException.class, () -> session.validateAndRecordChunk(2, 1, 1040L));

        // Valid single-chunk small file (e.g. 500 bytes file, final chunk can be short)
        TransferSession smallSession = new TransferSession(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "small.txt", 500, UUID.randomUUID(), 1000L);
        smallSession.transitionTo(TransferState.ACCEPTED, 1010L);
        smallSession.validateAndRecordChunk(0, 500, 1020L);
        assertEquals(500, smallSession.receivedBytes());
        assertEquals(1, smallSession.chunkCount());
    }

    @Test
    @DisplayName("Empty file (0 bytes) lifecycle transitions without chunks")
    void emptyFileLifecycle() {
        TransferSession empty = new TransferSession(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "empty.txt", 0, UUID.randomUUID(), 1000L);
        assertEquals(0, empty.declaredSizeBytes());

        empty.transitionTo(TransferState.ACCEPTED, 1010L);

        // Empty file cannot receive any chunks
        assertThrows(IllegalStateException.class, () -> empty.validateAndRecordChunk(0, 1, 1020L));

        // Direct transition from ACCEPTED to SERVER_VERIFIED
        empty.transitionTo(TransferState.SERVER_VERIFIED, 1030L);
        assertEquals(TransferState.SERVER_VERIFIED, empty.state());

        empty.setVerifiedSha256(TransferChecksum.EMPTY_SHA256);
        assertEquals(TransferChecksum.EMPTY_SHA256, empty.verifiedSha256());

        empty.transitionTo(TransferState.COMPLETED, 1040L);
        assertEquals(TransferState.COMPLETED, empty.state());
        assertTrue(empty.state().isTerminal());
    }

    @Test
    @DisplayName("Invalid SHA-256 string formats are rejected")
    void invalidSha256Formats() {
        TransferSession session = new TransferSession(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "file.bin", 100, UUID.randomUUID(), 1000L);

        assertThrows(IllegalArgumentException.class, () -> session.setVerifiedSha256(""));
        assertThrows(IllegalArgumentException.class, () -> session.setVerifiedSha256("abc"));
        assertThrows(IllegalArgumentException.class, () -> session.setVerifiedSha256("0".repeat(63)));
        assertThrows(IllegalArgumentException.class, () -> session.setVerifiedSha256("0".repeat(65)));
        assertThrows(IllegalArgumentException.class, () -> session.setVerifiedSha256("A".repeat(64))); // uppercase rejected by codec regex
        assertThrows(IllegalArgumentException.class, () -> session.setVerifiedSha256("g".repeat(64))); // non-hex rejected

        session.setVerifiedSha256(null);
        assertNull(session.verifiedSha256());

        session.setVerifiedSha256("a".repeat(64));
        assertEquals("a".repeat(64), session.verifiedSha256());
    }

    @Test
    @DisplayName("Checksum computation, repeated finalization and streaming verification")
    void checksumVerification() throws Exception {
        byte[] data = "NetSync Concurrent File Transfer".getBytes();
        String expectedHash = TransferChecksum.computeDigestHex(data);

        assertEquals(64, expectedHash.length());
        assertTrue(expectedHash.matches("[0-9a-f]{64}"));
        assertTrue(TransferChecksum.matches(expectedHash, expectedHash.toUpperCase()));

        // Streaming incremental digest
        TransferChecksum.Incremental incremental = new TransferChecksum.Incremental();
        incremental.update(data, 0, 7);
        incremental.update(data, 7, data.length - 7);

        // First finalization
        assertEquals(expectedHash, incremental.digestHex());
        // Repeated finalization returns identical digest
        assertEquals(expectedHash, incremental.digestHex());

        // Update after finalization is rejected
        assertThrows(IllegalStateException.class, () -> incremental.update(new byte[]{1}));

        // File-based worker checksum
        Path file = tempDir.resolve("chunked.dat");
        Files.write(file, data);
        assertEquals(expectedHash, TransferChecksum.computeFileDigestHex(file));

        // Empty file checksum matches specification
        assertEquals(TransferChecksum.EMPTY_SHA256, TransferChecksum.computeDigestHex(new byte[0]));
    }
}
