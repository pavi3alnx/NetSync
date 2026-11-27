package com.netsync.gui;

import com.netsync.transfer.TransferChecksum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Deterministic headless tests for {@link SafeFileExportService}. */
class SafeFileExportServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void exportsVerifiedFileWithOriginalFilenameAndPreservesOriginalBin() throws Exception {
        Path storageDir = tempDir.resolve("storage");
        Path exportDir = tempDir.resolve("exports");
        Files.createDirectories(storageDir);
        Files.createDirectories(exportDir);

        byte[] content = "NetSync verified PNG payload bytes".getBytes(StandardCharsets.UTF_8);
        Path sourceBin = storageDir.resolve(UUID.randomUUID() + ".bin");
        Files.write(sourceBin, content);
        String expectedSha = TransferChecksum.computeFileDigestHex(sourceBin);

        String suggested = SafeFileExportService.safeSuggestedFileName("Screenshot (1).png", UUID.randomUUID());
        assertEquals("Screenshot (1).png", suggested);

        Path destination = exportDir.resolve(suggested);
        SafeFileExportService.ExportResult result =
                SafeFileExportService.exportVerifiedFile(sourceBin, expectedSha, destination);

        assertEquals(destination.toAbsolutePath().normalize(), result.destinationFile());
        assertEquals(content.length, result.bytesCopied());
        assertEquals(expectedSha, result.sha256());
        assertArrayEquals(content, Files.readAllBytes(destination));

        // Original safe .bin file must remain untouched.
        assertTrue(Files.exists(sourceBin));
        assertArrayEquals(content, Files.readAllBytes(sourceBin));
    }

    @Test
    void refusesToOverwriteExistingDestinationFile() throws Exception {
        Path sourceBin = tempDir.resolve("source.bin");
        byte[] sourceBytes = "new verified content".getBytes(StandardCharsets.UTF_8);
        Files.write(sourceBin, sourceBytes);
        String expectedSha = TransferChecksum.computeFileDigestHex(sourceBin);

        Path existingDest = tempDir.resolve("Screenshot (1).png");
        byte[] originalExisting = "precious existing file".getBytes(StandardCharsets.UTF_8);
        Files.write(existingDest, originalExisting);

        assertThrows(FileAlreadyExistsException.class,
                () -> SafeFileExportService.exportVerifiedFile(sourceBin, expectedSha, existingDest));
        assertArrayEquals(originalExisting, Files.readAllBytes(existingDest),
                "Existing destination file must never be overwritten");
    }

    @Test
    void rejectsUnsafeRemoteFilenamesAndUsesNeutralDialogFallback() {
        UUID tid = UUID.fromString("12345678-1234-1234-1234-1234567890ab");
        String[] unsafeNames = {
                "../escape.png",
                "..\\escape.png",
                "dir/file.png",
                "dir\\file.png",
                "C:evil.png",
                "CON.png",
                "NUL",
                "trailing-space.png ",
                "trailing-dot.png.",
                "bad<name>.png",
                "bad\0null.png",
                "bad\nctrl.png"
        };
        for (String unsafe : unsafeNames) {
            assertThrows(RuntimeException.class,
                    () -> SafeFileExportService.validateExportFileName(unsafe),
                    "Expected unsafe filename to be rejected: " + unsafe);
            String fallback = SafeFileExportService.safeSuggestedFileName(unsafe, tid);
            assertEquals("received-12345678.bin", fallback);
        }

        assertThrows(IllegalArgumentException.class, () -> SafeFileExportService.validateExportFileName(""));
        assertThrows(IllegalArgumentException.class, () -> SafeFileExportService.validateExportFileName("   "));
        assertThrows(IllegalArgumentException.class, () -> SafeFileExportService.validateExportFileName(null));
    }

    @Test
    void failsCleanlyWhenSourceMissingOrCorruptedWithoutLeavingDestinationFile() throws Exception {
        Path missing = tempDir.resolve("missing.bin");
        Path dest = tempDir.resolve("output.png");
        String validHex = TransferChecksum.EMPTY_SHA256;

        assertThrows(IOException.class, () -> SafeFileExportService.exportVerifiedFile(missing, validHex, dest));
        assertFalse(Files.exists(dest));

        Path tamperedSource = tempDir.resolve("tampered.bin");
        Files.writeString(tamperedSource, "tampered bytes", StandardCharsets.UTF_8);
        assertThrows(IOException.class, () -> SafeFileExportService.exportVerifiedFile(tamperedSource, validHex, dest));
        assertFalse(Files.exists(dest));
    }

    @Test
    void rejectsExportToSamePathAsSource() throws Exception {
        Path sourceBin = tempDir.resolve("same.bin");
        Files.writeString(sourceBin, "data", StandardCharsets.UTF_8);
        String sha = TransferChecksum.computeFileDigestHex(sourceBin);

        assertThrows(IllegalArgumentException.class,
                () -> SafeFileExportService.exportVerifiedFile(sourceBin, sha, sourceBin));
    }
}
