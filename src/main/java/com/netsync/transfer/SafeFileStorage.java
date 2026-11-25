package com.netsync.transfer;

import com.netsync.common.ProtocolLimits;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.*;
import java.util.Objects;
import java.util.UUID;

/**
 * Manages safe server-side file storage for relay file transfers.
 * <p>
 * Temporary relay files are written under {@code data/uploads/server/} using safe,
 * generated names in the format {@code <transferId>.part}. User-supplied filenames
 * are never resolved against the filesystem to prevent directory traversal and link attacks.
 */
public class SafeFileStorage {
    private final Path rootDir;

    public SafeFileStorage() {
        this(Path.of(ProtocolLimits.STORAGE_ROOT, "server"));
    }

    public SafeFileStorage(Path rootDir) {
        this.rootDir = Objects.requireNonNull(rootDir, "Root directory must not be null").normalize().toAbsolutePath();
        ensureRootDirectory();
    }

    /**
     * Ensures the root directory exists and is not a symbolic link.
     */
    private void ensureRootDirectory() {
        try {
            Path current = rootDir.getRoot();
            if (current == null) throw new SecurityException("Storage root must be absolute");
            for (Path component : rootDir) {
                current = current.resolve(component);
                if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                    if (Files.isSymbolicLink(current))
                        throw new SecurityException("Storage path contains a symbolic link: " + current);
                    if (!Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS))
                        throw new SecurityException("Storage path component is not a directory: " + current);
                } else {
                    Files.createDirectory(current);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize storage directory: " + rootDir, e);
        }
    }

    public Path rootDir() {
        return rootDir;
    }

    /**
     * Resolves and validates a safe file path for a transfer ID.
     *
     * @param transferId transfer UUID
     * @return safe absolute Path under root directory
     */
    public Path resolvePartFile(UUID transferId) {
        return resolveGeneratedFile(transferId, ".part");
    }

    /** Final generated client file path; an existing destination is never replaced. */
    public Path resolveFinalFile(UUID transferId) {
        return resolveGeneratedFile(transferId, ".bin");
    }

    private Path resolveGeneratedFile(UUID transferId, String suffix) {
        ensureRootDirectory();
        Objects.requireNonNull(transferId, "Transfer ID must not be null");
        String safeName = transferId + suffix;
        validateSafeFileName(safeName);

        Path candidate = rootDir.resolve(safeName).normalize().toAbsolutePath();
        if (!candidate.startsWith(rootDir)) {
            throw new SecurityException("Path traversal attempt detected: " + safeName);
        }
        if (Files.isSymbolicLink(candidate)) {
            throw new SecurityException("Symbolic link target file rejected: " + candidate);
        }
        return candidate;
    }

    /**
     * Appends binary chunk bytes to the temporary transfer file.
     *
     * @param transferId transfer UUID
     * @param data       raw chunk bytes
     * @throws IOException       if an I/O error occurs
     * @throws SecurityException if path traversal or symlink violation is detected
     */
    public void appendChunk(UUID transferId, byte[] data) throws IOException {
        Objects.requireNonNull(data, "Chunk data must not be null");
        appendChunk(transferId, data, 0, data.length);
    }

    /**
     * Appends a slice of binary chunk bytes to the temporary transfer file.
     *
     * @param transferId transfer UUID
     * @param data       byte array containing chunk
     * @param offset     start offset
     * @param length     byte length
     * @throws IOException       if an I/O error occurs
     * @throws SecurityException if path traversal or symlink violation is detected
     */
    public void appendChunk(UUID transferId, byte[] data, int offset, int length) throws IOException {
        Objects.requireNonNull(data, "Chunk data must not be null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IndexOutOfBoundsException("Invalid slice bounds");
        }

        Path partFile = resolvePartFile(transferId);
        ensureRootDirectory();

        try (OutputStream out = Files.newOutputStream(
                partFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        )) {
            out.write(data, offset, length);
            out.flush();
        }
    }

    /**
     * Returns whether the partial file for the given transfer ID currently exists on disk.
     */
    public boolean exists(UUID transferId) {
        Path partFile = resolvePartFile(transferId);
        return Files.exists(partFile) && !Files.isDirectory(partFile);
    }

    /**
     * Returns the current byte length of the partial file on disk, or 0 if it does not exist.
     */
    public long getFileSize(UUID transferId) throws IOException {
        Path partFile = resolvePartFile(transferId);
        return Files.exists(partFile) ? Files.size(partFile) : 0L;
    }

    /**
     * Deletes the temporary partial file if it exists. Idempotent.
     *
     * @param transferId transfer UUID
     * @return true if file existed and was deleted, false otherwise
     */
    public boolean deletePartialFile(UUID transferId) {
        if (transferId == null) {
            return false;
        }
        try {
            Path partFile = resolvePartFile(transferId);
            for (int attempt = 0; attempt < 10; attempt++) {
                try {
                    return Files.deleteIfExists(partFile);
                } catch (IOException e) {
                    if (attempt == 9) {
                        return false;
                    }
                    try {
                        Thread.sleep(1L);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            }
        } catch (RuntimeException e) {
            return false;
        }
        return false;
    }

    /**
     * Validates that a filename contains no illegal traversal characters or separators.
     *
     * @param name filename to check
     */
    public static void validateSafeFileName(String name) {
        if (name == null || name.isEmpty() || name.length() > 128) {
            throw new IllegalArgumentException("Invalid file name length: " + (name == null ? "null" : name.length()));
        }
        if (name.contains("..") || name.indexOf('/') >= 0 || name.indexOf('\\') >= 0 || name.indexOf('\0') >= 0) {
            throw new SecurityException("Disallowed path characters in file name: " + name);
        }
        if (name.codePoints().anyMatch(Character::isISOControl)) {
            throw new SecurityException("ISO control characters in file name: " + name);
        }
    }
}
