# File transfer module

The module implements the version 1 consent-based transfer protocol over the
existing TCP connection. The server relays binary `FILE_CHUNK` frames between
participants and stores a generated `<transferId>.part` file under
`data/uploads/server/`. A recipient stores a partial under its authenticated
client-ID directory and publishes it as `<transferId>.bin` only after byte count
and SHA-256 validation and successful no-replace finalization.

## State flow

`OFFERED -> ACCEPTED -> IN_PROGRESS -> SERVER_VERIFIED -> COMPLETED` is the
successful path. The recipient must accept before upload. A zero-byte file moves
from `ACCEPTED` to `SERVER_VERIFIED` with zero chunks and SHA-256 of the empty
byte sequence. Rejection, cancellation, peer disconnect, timeout, authorization
failure, storage failure, and checksum failure terminate the transfer and release
the participant slots.

Only the authenticated sender connection can offer, upload chunks, or complete.
Only the designated recipient can accept, reject, or confirm integrity. Only
participants can cancel. Identity comes from the shared TCP session registry;
client identity fields are never accepted as proof.

JSON controls retain the existing `FILE_OFFER`, `FILE_OFFERED`, `FILE_ACCEPT`,
`FILE_REJECT`, `FILE_COMPLETE`, `FILE_RECEIVED`, `FILE_CANCEL`, `FILE_STATUS`,
`ACK`, and `ERROR` message types. Binary kind `0x02` frames carry a transfer ID,
sequence, and up to 16,384 data bytes. Files are limited to 10 MiB; at most one
transfer per client and ten server-wide are active. Offers and stalled transfers
expire after 30 seconds. Chunks are sent in sequence with stop-and-wait ACKs.

The server uses a bounded two-thread worker pool with a 32-task queue. Client
receiver writes, hashing, finalization, and cleanup use bounded transfer workers;
the serial client dispatcher performs validation and queues ordered chunks, with
at most 32 pending chunk payloads retained per receiver. The Selector owns server
state transitions and frame enqueueing; disk operations run on workers. Accepted
client connections are bound to a local connection generation for outbound
transfer frames and transfer output.

Partial-file cleanup runs on bounded transfer workers synchronized with the
transfer's storage lock so cleanup cannot finish before the final accepted write.
Transient deletion failures retry up to 50 times (10 ms backoff) on the worker;
if cleanup task submission is rejected during worker queue saturation, the
transfer ID is retained in a bounded deferred-cleanup set (at most 64 entries)
and drained as workers become available or on periodic timeout checks. If
external filesystem failures persist past retry exhaustion, the transfer ID is
recorded in a bounded failed-cleanup set (at most 64 entries) without retaining
the terminal session or blocking the Selector.

## Storage threat model and limitations

Storage filenames are generated from transfer IDs (`<transferId>.part` and
`<transferId>.bin`); remote filenames are display metadata only. Finalization
omits `REPLACE_EXISTING`, so a pre-existing destination is preserved. Storage
roots and all existing path ancestors are checked with `LinkOption.NOFOLLOW_LINKS`
to reject symbolic links and non-directory components, and generated names are
confined to the configured root.

Because standard portable Java NIO (`java.nio.file.Files`) does not expose POSIX
`openat(..., O_NOFOLLOW | O_DIRECTORY)` directory-descriptor pinning across
Windows and POSIX platforms:

- **Residual race (TOCTOU)**: An attacker with write/replace permissions on the
  local storage root or one of its ancestor directories could replace a directory
  component or target path with a symbolic link or directory junction in the
  window between `ensureRootDirectory()` / `Files.isSymbolicLink(candidate)` and
  `Files.newOutputStream(...)`, `Files.move(...)`, `Files.createFile(...)`, or
  `Files.deleteIfExists(...)`.
- **Trusted storage-root and permission assumptions**: The configured storage
  root (`data/uploads/` or injected root) and every ancestor directory up to the
  filesystem root must be owned by and writable only by the trusted local OS
  user running the NetSync process (e.g., mode `0700` on POSIX or an owner-only
  ACL on Windows, with no untrusted local users or processes granted write,
  delete, or symlink/junction creation privileges in that tree).
- **Platform guarantees**: Rejects untrusted network filename traversal (`..`,
  `/`, `\`, NUL, control characters) and pre-existing symlinks in the storage
  path; does not protect against a concurrent privileged or same-UID local
  process mutating the directory hierarchy during active I/O.
- **Threat-model status**: The existing project documentation (`app/README.md`
  and `client/README.md`) specifies a trusted local machine and terminal for the
  loopback demo, but does not formally establish an adversarial multi-user local
  filesystem threat model. Accepting this residual local-filesystem TOCTOU risk
  outside a trusted single-user directory tree requires explicit project-owner
  approval.

SHA-256 detects accidental corruption; it is not transport authentication. No
resume, TLS, or remote deployment is provided.

See [the production demo](../../../../../../docs/file-transfer-demo.md) for the CLI
workflow and verification commands.
