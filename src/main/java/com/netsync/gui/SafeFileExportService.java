package com.netsync.gui;

import com.netsync.common.ProtocolLimits;
import com.netsync.transfer.SafeFileStorage;
import com.netsync.transfer.TransferChecksum;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Safe, non-overwriting file export service for completed and verified incoming transfers.
 * <p>
 * Preserves NetSync's safe storage model: received files remain untouched in their generated
 * {@code <transferId>.bin} storage path, while users may explicitly export a verified copy
 * to a user-chosen destination with a validated filename.
 */
public final class SafeFileExportService {
    private static final Set<String> WINDOWS_RESERVED_NAMES = Set.of(
            "CON", "PRN", "AUX", "NUL",
            "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
    );

    public record ExportResult(Path destinationFile, long bytesCopied, String sha256) {}

    private SafeFileExportService() {}

    /**
     * Validates an untrusted remote-supplied or user-supplied filename for safe local export.
     * Rejects path separators, traversal sequences, control characters, Windows drive/device
     * names, invalid characters, and trailing dots/spaces.
     *
     * @param fileName untrusted filename
     * @return the validated filename verbatim (preserving original extension and spaces)
     * @throws IllegalArgumentException if blank or length invalid
     * @throws SecurityException        if unsafe characters, traversal, or reserved names are detected
     */
    public static String validateExportFileName(String fileName) {
        if (fileName == null || fileName.isBlank()) {
            throw new IllegalArgumentException("File name must not be blank.");
        }
        SafeFileStorage.validateSafeFileName(fileName);

        if (".".equals(fileName) || "..".equals(fileName)) {
            throw new SecurityException("Relative directory name rejected: " + fileName);
        }
        if (fileName.endsWith(".") || fileName.endsWith(" ")) {
            throw new SecurityException("Trailing dot or space in file name is disallowed: " + fileName);
        }
        for (int i = 0; i < fileName.length(); i++) {
            char c = fileName.charAt(i);
            if (c == '<' || c == '>' || c == ':' || c == '"' || c == '|' || c == '?' || c == '*') {
                throw new SecurityException("Disallowed filesystem character in file name: " + fileName);
            }
        }
        int dotIdx = fileName.indexOf('.');
        String baseName = (dotIdx >= 0 ? fileName.substring(0, dotIdx) : fileName).toUpperCase(Locale.ROOT);
        if (WINDOWS_RESERVED_NAMES.contains(baseName)) {
            throw new SecurityException("Reserved device name rejected: " + fileName);
        }
        return fileName;
    }

    /**
     * Returns a safe initial filename suggestion for a Save As dialog.
     * If the remote filename passes {@link #validateExportFileName(String)}, it is returned verbatim;
     * otherwise a neutral generated fallback name is returned so untrusted input never acts as a path.
     */
    public static String safeSuggestedFileName(String remoteFileName, UUID transferId) {
        try {
            return validateExportFileName(remoteFileName);
        } catch (RuntimeException unsafe) {
            String suffix = transferId != null ? transferId.toString().substring(0, 8) : "file";
            return "received-" + suffix + ".bin";
        }
    }

    /**
     * Copies a verified received {@code .bin} file to a new user-chosen destination without
     * overwriting existing files, without following symlinks/junctions, and with full SHA-256
     * verification of both source and exported output.
     * <p>
     * Must be called off the JavaFX application thread.
     */
    public static ExportResult exportVerifiedFile(
            Path verifiedSourceFile,
            String expectedSha256,
            Path destinationFile) throws IOException {
        Objects.requireNonNull(verifiedSourceFile, "Verified source file must not be null");
        Objects.requireNonNull(destinationFile, "Destination file must not be null");
        if (expectedSha256 == null || expectedSha256.length() != 64) {
            throw new IllegalArgumentException("A verified 64-character SHA-256 digest is required for export.");
        }

        Path sourceAbs = verifiedSourceFile.toAbsolutePath().normalize();
        validateSafeRegularSource(sourceAbs);

        Path destAbs = destinationFile.toAbsolutePath().normalize();
        Path destName = destAbs.getFileName();
        Path destParent = destAbs.getParent();
        if (destName == null || destParent == null) {
            throw new IllegalArgumentException("Destination path must include a directory and file name.");
        }
        validateExportFileName(destName.toString());

        if (destAbs.equals(sourceAbs)) {
            throw new IllegalArgumentException("Destination must be different from the stored received file.");
        }

        validateSafeDestinationParent(destParent);
        if (Files.exists(destAbs, LinkOption.NOFOLLOW_LINKS)) {
            throw new FileAlreadyExistsException("Destination file already exists; refusing to overwrite: " + destAbs);
        }

        String actualSourceSha = TransferChecksum.computeFileDigestHex(sourceAbs);
        if (!TransferChecksum.matches(expectedSha256, actualSourceSha)) {
            throw new IOException("Stored received file failed SHA-256 integrity check before export.");
        }

        long expectedSize = Files.size(sourceAbs);
        boolean created = false;
        long bytesCopied = 0L;
        TransferChecksum.Incremental copyDigest = new TransferChecksum.Incremental();

        try (InputStream in = Files.newInputStream(sourceAbs, LinkOption.NOFOLLOW_LINKS);
             OutputStream out = Files.newOutputStream(destAbs, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            created = true;
            byte[] buffer = new byte[ProtocolLimits.MAX_CHUNK_BYTES];
            int read;
            while ((read = in.read(buffer)) != -1) {
                if (read > 0) {
                    out.write(buffer, 0, read);
                    copyDigest.update(buffer, 0, read);
                    bytesCopied += read;
                }
            }
            out.flush();
        } catch (IOException | RuntimeException failure) {
            if (created) {
                try {
                    Files.deleteIfExists(destAbs);
                } catch (IOException ignored) {
                }
            }
            throw failure;
        }

        String exportedSha = copyDigest.digestHex();
        if (bytesCopied != expectedSize || !TransferChecksum.matches(expectedSha256, exportedSha)) {
            try {
                Files.deleteIfExists(destAbs);
            } catch (IOException ignored) {
            }
            throw new IOException("Exported file failed SHA-256 or size verification; incomplete copy removed.");
        }

        // Independent read-back verification of the written destination file on disk.
        String diskSha = TransferChecksum.computeFileDigestHex(destAbs);
        if (!TransferChecksum.matches(expectedSha256, diskSha)) {
            try {
                Files.deleteIfExists(destAbs);
            } catch (IOException ignored) {
            }
            throw new IOException("Exported file on disk did not match verified SHA-256 digest; copy removed.");
        }

        return new ExportResult(destAbs, bytesCopied, diskSha);
    }

    private static void validateSafeRegularSource(Path sourceAbs) throws IOException {
        if (!Files.exists(sourceAbs, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Stored received file no longer exists: " + sourceAbs);
        }
        BasicFileAttributes attrs = Files.readAttributes(sourceAbs, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (attrs.isSymbolicLink() || attrs.isOther() || !attrs.isRegularFile()) {
            throw new SecurityException("Stored received source is not a safe regular file: " + sourceAbs);
        }
    }

    private static void validateSafeDestinationParent(Path destParent) throws IOException {
        Path root = destParent.getRoot();
        if (root == null) {
            throw new SecurityException("Destination directory must be absolute.");
        }
        if (!Files.exists(destParent, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Destination directory does not exist: " + destParent);
        }
        Path current = root;
        for (Path component : destParent) {
            current = current.resolve(component);
            if (!Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Destination path component does not exist: " + current);
            }
            BasicFileAttributes attrs = Files.readAttributes(current, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (attrs.isSymbolicLink() || attrs.isOther()) {
                throw new SecurityException("Destination path traverses a symbolic link or reparse point: " + current);
            }
            if (!attrs.isDirectory()) {
                throw new SecurityException("Destination parent component is not a directory: " + current);
            }
        }
    }
}
