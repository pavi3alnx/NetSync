package com.netsync.transfer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SafeFileStorageTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Creates storage root directory and resolves safe .part paths")
    void safePartPathResolution() {
        SafeFileStorage storage = new SafeFileStorage(tempDir.resolve("server"));
        UUID transferId = UUID.randomUUID();

        Path resolved = storage.resolvePartFile(transferId);
        assertEquals(transferId + ".part", resolved.getFileName().toString());
        assertTrue(resolved.startsWith(storage.rootDir()));
        assertFalse(storage.exists(transferId));
    }

    @Test
    @DisplayName("Appends binary chunks sequentially to .part file")
    void sequentialChunkWriting() throws Exception {
        SafeFileStorage storage = new SafeFileStorage(tempDir.resolve("server"));
        UUID transferId = UUID.randomUUID();

        byte[] chunk1 = new byte[]{1, 2, 3, 4};
        byte[] chunk2 = new byte[]{5, 6, 7};

        storage.appendChunk(transferId, chunk1);
        assertTrue(storage.exists(transferId));
        assertEquals(4, storage.getFileSize(transferId));

        storage.appendChunk(transferId, chunk2);
        assertEquals(7, storage.getFileSize(transferId));

        byte[] allBytes = Files.readAllBytes(storage.resolvePartFile(transferId));
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7}, allBytes);
    }

    @Test
    @DisplayName("Idempotent partial file deletion and cleanup")
    void partialFileCleanup() throws Exception {
        SafeFileStorage storage = new SafeFileStorage(tempDir.resolve("server"));
        UUID transferId = UUID.randomUUID();

        assertFalse(storage.deletePartialFile(null), "Null transfer ID deletion returns false");
        assertFalse(storage.deletePartialFile(transferId), "Non-existent file deletion returns false");

        storage.appendChunk(transferId, new byte[]{42});
        assertTrue(storage.exists(transferId));

        assertTrue(storage.deletePartialFile(transferId), "Existent file deletion returns true");
        assertFalse(storage.exists(transferId));
        assertEquals(0L, storage.getFileSize(transferId));

        assertFalse(storage.deletePartialFile(transferId), "Repeated deletion is safe and returns false");
    }

    @Test
    @DisplayName("Invalid slice bounds and null arguments in appendChunk and resolvePartFile")
    void sliceBoundsAndNullGuards() {
        SafeFileStorage storage = new SafeFileStorage(tempDir.resolve("server"));
        UUID transferId = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3};

        assertThrows(NullPointerException.class, () -> storage.resolvePartFile(null));
        assertThrows(NullPointerException.class, () -> storage.appendChunk(transferId, null));
        assertThrows(IndexOutOfBoundsException.class, () -> storage.appendChunk(transferId, data, -1, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> storage.appendChunk(transferId, data, 0, -1));
        assertThrows(IndexOutOfBoundsException.class, () -> storage.appendChunk(transferId, data, 2, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> storage.appendChunk(transferId, data, 0, 4));
    }

    @Test
    @DisplayName("Path traversal attacks and illegal filenames are strictly rejected")
    void pathTraversalPrevention() {
        assertThrows(SecurityException.class, () -> SafeFileStorage.validateSafeFileName("../escape.bin"));
        assertThrows(SecurityException.class, () -> SafeFileStorage.validateSafeFileName("dir/file.txt"));
        assertThrows(SecurityException.class, () -> SafeFileStorage.validateSafeFileName("dir\\file.txt"));
        assertThrows(SecurityException.class, () -> SafeFileStorage.validateSafeFileName("null\0byte"));
        assertThrows(SecurityException.class, () -> SafeFileStorage.validateSafeFileName("control\nchar"));
        assertThrows(SecurityException.class, () -> SafeFileStorage.validateSafeFileName("control\rchar"));
        assertThrows(IllegalArgumentException.class, () -> SafeFileStorage.validateSafeFileName(null));
        assertThrows(IllegalArgumentException.class, () -> SafeFileStorage.validateSafeFileName(""));
        assertThrows(IllegalArgumentException.class, () -> SafeFileStorage.validateSafeFileName("a".repeat(129)));
    }

    @Test
    @DisplayName("Symbolic links as storage root or target are rejected")
    void symlinkProtection() throws Exception {
        Path targetDir = tempDir.resolve("real_storage");
        Files.createDirectories(targetDir);
        Path linkDir = tempDir.resolve("symlink_storage");

        try {
            Files.createSymbolicLink(linkDir, targetDir);
            assertThrows(SecurityException.class, () -> new SafeFileStorage(linkDir));
        } catch (UnsupportedOperationException | FileSystemException e) {
            // Symlink creation may not be permitted without elevated permissions on Windows
            // In that case, the environment already prevents symbolic link creation
        }
    }
}
