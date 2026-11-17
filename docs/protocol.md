# NetSync protocol (version 1)

Status: finalized version 1 wire contract, not implemented. See [architecture.md](architecture.md) for thread ownership and [member-tasks.md](member-tasks.md) for module acceptance scenarios. Defaults below are selected for implementation; subsequent wire changes require shared review.

## Transport and limits

Use TCP port 5050 and UDP port 5051 by default, bound to `127.0.0.1`, with configurable ports and a manually entered server address. The initial feature set is direct chat, ephemeral sessions and consent-based file relay. Jackson `com.fasterxml.jackson.core:jackson-databind:2.21.8` handles JSON controls; core and annotations are transitive dependencies. Configure strict schema/duplicate-key/depth checks explicitly; never enable polymorphic default typing for network input.

Each TCP frame is `[4-byte big-endian length N][1-byte kind][N-1 body bytes]`. N counts kind plus body, excluding the four-byte prefix. Validate N before allocating: valid global range is 2 through 65,537. Zero, negative/high-bit values, one, or larger values close the connection without resynchronization. All multibyte binary fields are big-endian. No line delimiters are used.

| Kind | Body and exact limits |
| --- | --- |
| `0x01` JSON control | 1 through 65,536 bytes (64 KiB) of strict UTF-8 JSON. N is 2 through 65,537. |
| `0x02` binary file chunk | 16-byte message UUID, 16-byte transfer UUID, 4-byte nonnegative sequence, then 1 through 16,384 raw file bytes. N is 38 through 16,421. UUIDs encode their most-significant 64 bits then least-significant 64 bits, each big-endian. |

Unknown kinds or invalid kind-specific lengths close the connection. A binary chunk carries operation `FILE_CHUNK` implicitly; it is never JSON-decoded and has no JSON envelope/version field (its layout belongs to protocol version 1 established by login). Its message UUID follows the JSON uniqueness/recent-duplicate rules; ACK/ERROR `replyTo` uses its canonical UUID string. Only authenticated, accepted transfers permit chunks. Format is determined solely from kind, never by inspecting content or switching to an unframed streaming mode. JSON-looking bytes inside a binary chunk remain file data. Chat and chunks may interleave as complete independent frames.

This replaces the earlier all-JSON/Base64 proposal. The JSON body limit remains exactly 64 KiB; the maximum prefix value increases by one byte to include kind. A full 16 KiB chunk has N = 16,421 (`00 00 40 25`), followed by kind `02`, the 36-byte binary header and 16,384 file bytes.

Decode UTF-8 strictly. Invalid encoding, invalid JSON, duplicate object keys, excessive nesting (over 16 levels) or an invalid envelope produces a best-effort `ERROR` with code `MALFORMED_MESSAGE`, followed by closure. If the message ID cannot be parsed safely, omit `replyTo`. Unsupported versions produce `UNSUPPORTED_VERSION` and closure. A valid envelope with an unknown type produces `UNKNOWN_TYPE`; keep the connection open. Invalid body fields produce `INVALID_REQUEST`; keep the connection open. Never echo secrets or raw malformed payloads in an error.

Default per-connection limits: 256 queued output frames and 1 MiB queued encoded bytes, whichever is reached first. If enqueueing would exceed either limit, disconnect the slow consumer and clean up its session/transfers; error delivery is best effort. Limit unauthenticated connections to 10 seconds to login and close incomplete frames after 15 seconds without frame progress. Bound active connections to 100 for the demo. Workers use two threads and a queue of 32 tasks shared by file I/O and expensive credential checks; reject new work as `BUSY` when saturated. Only one credential task per connection may be pending. The Selector task queue is bounded at 256 tasks; reserve completion capacity before dispatching a worker task so results cannot be silently lost. Reject new submissions when capacity cannot be reserved. Presence transitions may be coalesced per client to the latest state; never silently drop transfer/credential completions. These are configurable safeguards, not throughput guarantees.

### Partial reads and writes

Maintain independent per-connection prefix and payload buffers. Accumulate all four prefix bytes, flip/decode and validate, then accumulate exactly N bytes (kind plus body). Validate kind-specific bounds and dispatch only a complete frame to the JSON or binary chunk handler; retain partial state across Selector iterations. Process coalesced frames in order, with a per-turn work budget so one client cannot monopolize the loop. EOF in a partial frame discards it and closes the connection.

Encode a complete outbound frame and queue it in order. A SocketChannel write may consume only part or none of a buffer: retain its position and resume on `OP_WRITE`. Remove only fully written frames; disable `OP_WRITE` when the queue becomes empty. Use bounded buffers/queues and never spin waiting for progress. Only the server event loop reads/writes TCP channels. The client follows the same framing rules. If TLS is selected, apply this framing to decrypted application bytes, using `SSLEngine` transport state beneath it.

## Envelope and identifiers

Every JSON control message is an object with `version` (integer `1`), `type` (listed below), `messageId` (UUID string), optional `replyTo` (request UUID), and `body` (object). Reject unrecognized envelope/body fields in version 1 to catch integration mistakes. Responses, events and errors have their own fresh message ID. A response references its triggering request through `replyTo`; unsolicited events omit it.

Clients generate request UUIDs; the server generates response/event UUIDs. Security assigns an unpredictable `clientId` UUID at successful login, valid only for that session, and an internal session/connection mapping. Usernames are unique among active sessions. Reconnection requires login and yields a new client ID. The connection determines the sender; requests never supply an authoritative sender client ID. `transferId` is a server-generated UUID scoped to an accepted file offer and bound to its two participant sessions.

Requests are processed in TCP order per connection. An asynchronous handler must mark its operation pending and reject incompatible requests as `INVALID_STATE` until completion; it must not block the Selector. No durable replay protection or exactly-once guarantee is promised. Do not automatically resend mutating requests after timeout/reconnection; a lost response leaves its outcome uncertain. A duplicate request ID seen on the same connection is rejected as `DUPLICATE_MESSAGE`; retain at most 1,024 IDs per connection, so this is only a recent-duplicate guard. UUIDs identify messages and clients; they are not authentication secrets.

Forwarded binary chunks get fresh message UUIDs while retaining transfer UUID, sequence and raw bytes. Forwarded JSON completion is a server event with a fresh message ID and no `replyTo`; retain its body fields. Clients do not acknowledge those events directly: `FILE_RECEIVED` or `FILE_CANCEL` is a new request. Usernames contain 1–32 ASCII letters, digits or underscores and are case-sensitive; passwords are nonempty strings of at most 128 characters. Never truncate credentials silently. Integer body fields must be exact nonnegative integers within their stated limits.

## Message types

| Type and direction | Required body / meaning |
| --- | --- |
| `LOGIN_REQUEST` client → server | `username`, `password`; only over TLS or the restricted demonstration mode below. |
| `LOGIN_RESPONSE` server → client | `clientId`, `username`, secret `presenceToken`, `heartbeatIntervalSeconds` = 5, `presenceTimeoutSeconds` = 15. Failure uses `ERROR`, not this response. |
| `LOGOUT_REQUEST` client → server | Empty body; server replies `ACK`, then drains/closes and revokes session. |
| `CHAT_SEND` client → server | `recipientClientId`, `text`; nonempty text up to 4,096 UTF-8 bytes. |
| `CHAT_DELIVER` server → recipient | `senderClientId`, `text`, `sourceMessageId` from `CHAT_SEND`. |
| `PRESENCE_QUERY` client → server | Empty body; requests a current snapshot. |
| `PRESENCE_SNAPSHOT` server → client | `clients`: array of `{clientId, username, state}` for active sessions. State is `ONLINE` or `OFFLINE`; heartbeat-offline sessions remain listed while TCP is active. |
| `PRESENCE_UPDATE` server → clients | `clientId`, `username`, `state`: `ONLINE` or `OFFLINE`. |
| `FILE_OFFER` sender → server | `recipientClientId`, `fileName`, `sizeBytes`; server validates, assigns transfer ID and acknowledges. |
| `FILE_OFFERED` server → recipient | `transferId`, `senderClientId`, `fileName`, `sizeBytes`; recipient must consent before any chunks. |
| `FILE_ACCEPT` / `FILE_REJECT` recipient → server | `transferId`; acknowledge decision, and notify sender with `FILE_STATUS`. |
| `FILE_CHUNK` sender → server → recipient | Binary kind `0x02`, not JSON: message UUID, transfer UUID, sequence and at most 16,384 raw bytes. Server forwards only authorized chunks. JSON `FILE_CHUNK` is rejected as `INVALID_REQUEST`. |
| `FILE_COMPLETE` sender → server → recipient | `transferId`, `chunkCount`, `sha256` (64 lowercase hex characters); forwarded after preceding chunks have been processed. |
| `FILE_RECEIVED` recipient → server | `transferId`, `sha256`; sent only after local byte count/hash validation and successful finalization; server acknowledges and sends sender `FILE_STATUS` with `COMPLETED`. |
| `FILE_CANCEL` either participant → server | `transferId`; acknowledge, cancel and notify both participants via `FILE_STATUS`. |
| `FILE_STATUS` server → participant(s) | `transferId`, `state`: `ACCEPTED`, `REJECTED`, `CANCELLED`, `FAILED` or `COMPLETED`; optional safe `reason`. |
| `ACK` server → client | `operation` (request type), `status` (`QUEUED`, `ACCEPTED` or `STORED`); `transferId` required for file operations. Uses `replyTo`. |
| `ERROR` server → client | `code`, safe human-readable `message`; `transferId` when applicable. Uses `replyTo` when a request is known. |

All types except login require an authenticated session. A second login on an authenticated connection is `INVALID_STATE`. Failed login returns generic `AUTH_FAILED` and closes the connection; duplicate active usernames are rejected without disclosing credential details. Other error codes include `UNAUTHENTICATED`, `FORBIDDEN`, `RECIPIENT_UNAVAILABLE`, `INVALID_STATE`, `INVALID_SEQUENCE`, `SIZE_LIMIT`, `BUSY`, `TRANSFER_FAILED` and `DUPLICATE_MESSAGE`.

`CHAT_SEND` gets `ACK/QUEUED` after the recipient's output queue accepts delivery; it does not prove the recipient read or received it. A missing recipient produces `RECIPIENT_UNAVAILABLE`. `FILE_OFFER` gets `ACK/ACCEPTED` with transfer ID after offer creation and notification enqueue. Accept/reject/cancel/logout get `ACK/ACCEPTED`. A chunk gets `ACK/STORED` only after server temporary-file write and recipient enqueue succeed; sequence the worker completion before dispatching the next chunk. Complete gets `ACK/ACCEPTED` only after server byte count/hash validation and completion enqueue. Received gets `ACK/ACCEPTED` after recipient confirmation is recorded. Queries and login use their named responses, not an additional ACK. Events and ACK/ERROR messages are never acknowledged.

### Examples (JSON body only; add length N and kind `0x01`)

```json
{"version":1,"type":"LOGIN_REQUEST","messageId":"550e8400-e29b-41d4-a716-446655440001","body":{"username":"alice","password":"demo-only-secret"}}
```

The example login is permitted only in the security modes defined below.

```json
{"version":1,"type":"LOGIN_RESPONSE","messageId":"550e8400-e29b-41d4-a716-446655440002","replyTo":"550e8400-e29b-41d4-a716-446655440001","body":{"clientId":"550e8400-e29b-41d4-a716-446655440003","username":"alice","presenceToken":"<random-secret-redacted>","heartbeatIntervalSeconds":5,"presenceTimeoutSeconds":15}}
{"version":1,"type":"CHAT_SEND","messageId":"550e8400-e29b-41d4-a716-446655440004","body":{"recipientClientId":"550e8400-e29b-41d4-a716-446655440005","text":"Hello Bob"}}
{"version":1,"type":"ACK","messageId":"550e8400-e29b-41d4-a716-446655440006","replyTo":"550e8400-e29b-41d4-a716-446655440004","body":{"operation":"CHAT_SEND","status":"QUEUED"}}
{"version":1,"type":"CHAT_DELIVER","messageId":"550e8400-e29b-41d4-a716-446655440007","body":{"senderClientId":"550e8400-e29b-41d4-a716-446655440003","text":"Hello Bob","sourceMessageId":"550e8400-e29b-41d4-a716-446655440004"}}
{"version":1,"type":"ERROR","messageId":"550e8400-e29b-41d4-a716-446655440008","replyTo":"550e8400-e29b-41d4-a716-446655440004","body":{"code":"RECIPIENT_UNAVAILABLE","message":"Recipient is not connected"}}
```

Each JSON line is a separate frame: encode UTF-8, set N to byte count plus one, and prepend the four-byte length and kind `01`. ACK and ERROR above are alternative outcomes for the same chat request.

## UDP heartbeats and presence

One datagram contains one UTF-8 JSON object, without a TCP length prefix, at most 1,024 bytes. The heartbeat schema is separate from the TCP envelope:

```json
{"version":1,"type":"HEARTBEAT","clientId":"550e8400-e29b-41d4-a716-446655440003","presenceToken":"<random-secret-redacted>","sequence":1}
```

Security issues a cryptographically random 32-byte token encoded as Base64 at login. The client sends a heartbeat immediately and every 5 seconds. Sequence is an increasing nonnegative 64-bit integer per session; only a strictly greater sequence with a valid token refreshes server monotonic last-seen time. Drop malformed, oversized, stale, unknown-session or invalid-token datagrams silently. Receive with a 1,025-byte buffer and reject anything above 1,024 bytes so truncation cannot be accepted as a valid packet. No UDP acknowledgements or retransmission are needed.

An authenticated session starts `ONLINE` with login time as last-seen. Check expiry once per second; at 15 seconds without a valid heartbeat it becomes eligible for `OFFLINE`, published by the next check (within one second). A subsequent valid heartbeat returns it to `ONLINE`. This replaces the former `AWAY` label: heartbeat-offline does not revoke a healthy TCP session or cancel transfers, and the client remains in snapshots as `OFFLINE`. TCP logout/disconnect revokes the token, publishes `OFFLINE` only if state changed, and removes the client from snapshots. Token validation must recheck session activity so racing UDP packets cannot revive a logged-out session. Publish transitions, not every heartbeat, over TCP to authenticated clients. UDP status is advisory and never grants permission. LAN discovery is deferred.

## File-transfer strategy

Keep file data on the existing TCP connection: JSON kind `0x01` carries offer/consent/completion/ACK/error controls; binary kind `0x02` carries chunk headers and raw bytes. Both use the same length-prefixed decoder and outgoing queue. Avoid extra file listeners, blocking stream copies on the Selector thread or unframed binary streams. Do not use `transferTo` directly: it bypasses the selected framed chunk queue.

Limit files to 10 MiB (10,485,760 bytes) and one active transfer per client (sender or recipient), with at most 10 active transfers server-wide. A pending offer expires after 30 seconds; an accepted transfer expires after 30 seconds without progress. A file name is display metadata only: reject path separators/control characters and names over 128 characters, never resolve it as a supplied path. Store server relay files under `data/uploads/server/` and client files under `data/uploads/clients/<clientId>/`, relative to the application working directory. The entire `/data/uploads/` tree is Git-ignored runtime data. Create directories at runtime, generate `<transferId>.part` names and finalize client files to `<transferId>.bin` without overwriting existing files. Validate normalized paths remain under the storage root and reject symbolic-link escapes. Delete partial files on failure, timeout, cancellation or disconnect.

The recipient accepts before upload. Sender sends chunks in increasing sequence with exactly one chunk awaiting ACK at a time; stop and cancel on any chunk error. Server validates participants, sequence and declared total size, then uses bounded workers for temporary-file I/O and incremental SHA-256. Post completions to the Selector thread before advancing transfer state. Recipient likewise uses bounded disk work and a temporary file; finalize only after exact byte count, chunk count and SHA-256 match `FILE_COMPLETE`. Empty files use zero chunks and the SHA-256 of empty bytes. Sender waits for the complete ACK and then `FILE_STATUS/COMPLETED`; only recipient confirmation means successful end-to-end receipt. Hash mismatch, disk failure or queue overflow aborts the transfer and informs remaining participants on a best-effort basis. SHA-256 detects corruption; it is not authentication.

Stop-and-wait limits sender throughput, but recipient disk/output queues must still be bounded. A slow recipient can cause cancellation rather than unbounded buffering. No resume, parallel chunks or automatic retry is in version 1.

Nonempty files use nonempty chunks; only the last chunk may contain fewer than 16,384 raw bytes. Reject self-transfers. Recipient confirmation must match the server-validated hash before reporting `COMPLETED`. On success, delete the server relay temporary file and release both clients' transfer slots; the recipient keeps its finalized file under a generated safe name. Rejection/expiry also releases reserved slots. A recipient-side validation or disk failure sends `FILE_CANCEL`; the sender must treat cancellation as unsuccessful receipt.

## Authentication and authorization boundary

Network deployment requires TLS for TCP, including certificate validation by clients, before any password is sent. This requires an agreed NIO `SSLEngine` implementation; it is not delivered by this design. Do not send real passwords over an unencrypted connection or treat hashing a password on the wire as a TLS substitute.

The selected baseline is an explicitly enabled loopback-only authentication demonstration: bind TCP and UDP to loopback, use disposable dummy accounts/passwords only, show an insecure-demo warning, and forbid real credentials, LAN/public binding and deployment. This mode provides no transport confidentiality. Credential validation and session boundaries are still exercised, without claiming production security. Keep credentials out of source control/logs and store password verifiers with a salted password KDF when credential storage is implemented.

The UDP bearer token is visible on unencrypted UDP and can be replayed or stolen by an observer; the sequence guard is not encryption. Version 1 UDP is therefore limited to the same loopback demonstration even if TCP TLS is implemented. A network deployment must first approve protected UDP (for example DTLS) or move heartbeat transport to TLS TCP; do not expose the current UDP token protocol on a LAN/public network.

Security owns identity validation and sessions. Chat and transfer check the authenticated connection and permissions through security APIs on every request. Only the intended recipient can accept/reject/confirm a transfer; only its participants can send allowed control messages, and only its sender can upload chunks/complete. Never authorize from a claimed sender, client ID, transfer ID or UDP source address alone. Revoke all associated state on logout/disconnect. Persistent users, account registration and roles beyond authenticated peer access are deferred.
