# NetSync member tasks and integration plan

Status: finalized module boundaries and defaults for five members. Class/interface names below are suggestions, not implemented features. Assign actual member names before coding; changes to shared contracts require team review. [architecture.md](architecture.md) defines boundaries; [protocol.md](protocol.md) is the shared wire contract.

## Member 1 — Java NIO Server

- **Responsibilities:** own the sole TCP listener, Selector loop, connection state, ByteBuffers, partial frame reads/writes, dispatcher, bounded outgoing queues and lifecycle cleanup. Coordinate the shared CLI shell and application wiring.
- **Suggested types:** `server.NioServer`, `ConnectionContext`, `FrameDecoder`, `ConnectionState`, `MessageDispatcher`, `OutboundSink`; `client.NetSyncClient`. Shared `common.MessageEnvelope`, `MessageCodec`, `FrameCodec` and `FileChunk` need review by all handlers' owners.
- **Dependencies:** security supplies session cleanup; other owners register handlers and use the outbound sink. JSON controls use Jackson 2.21.8 via the shared codec; binary chunks bypass JSON. No handler opens another TCP server.
- **Deliverables:** transport and dispatcher, configuration/lifecycle entry point, injectable handler/sink fixtures, CLI connection/framing support and transport tests.
- **Acceptance tests:** split a prefix and payload across multiple reads; decode two frames in one read; resume partial writes without lost bytes; reject zero/negative/oversized and invalid kind-specific lengths; interleave JSON and binary frames without corrupting file bytes; disconnect a slow consumer at the output limit; bind failure/shutdown releases resources. Use a controllable channel/transport fixture to force partial writes.
- **Two-minute demo:** start one server and two CLI clients, exchange framed requests through registered handlers, show an invalid-length connection rejected while the healthy client continues, then stop/restart the server on the same ports.

## Member 2 — TCP Chat & Messaging

- **Responsibilities:** handle direct `CHAT_SEND`, validate text, resolve recipient sessions, emit `CHAT_DELIVER`, ACK only after enqueue acceptance, and return safe routing errors. No transport or socket reads/writes.
- **Suggested types:** `chat.ChatHandler`, `ChatRouter`; implement the shared `MessageHandler` contract.
- **Dependencies:** `ConnectionContext`, `OutboundSink`, `SessionRegistry`, `AuthorizationService`, shared envelopes/types and CLI commands. Presence `OFFLINE` is advisory; an active TCP recipient remains routable.
- **Deliverables:** direct-message routing, ACK/error semantics, CLI chat command and routing tests using a fake outbound sink.
- **Acceptance tests:** authenticated A sends to B; B receives server-derived sender identity and text; A receives `ACK/QUEUED`; unknown recipient returns `RECIPIENT_UNAVAILABLE`; unauthenticated or oversized chat is rejected; a rejected output enqueue never produces a success ACK.
- **Two-minute demo:** login Alice and Bob, query IDs, exchange direct messages, disconnect Bob and show Alice's next request receives a safe error. Explain that queued ACK does not prove delivery/read.

## Member 3 — UDP Presence & Heartbeats

- **Responsibilities:** separate UDP service, strict datagram validation, monotonic heartbeat expiry, `ONLINE`/`OFFLINE` transitions, TCP snapshot/update handlers, and client heartbeat scheduling. Manual server address is the baseline; discovery is deferred pending approval.
- **Suggested types:** `presence.UdpPresenceService`, `HeartbeatTracker`, `PresenceHandler`, `PresenceListener`; `client.HeartbeatSender`.
- **Dependencies:** security validates session/token; the session registry provides active identities; events go through the Selector task queue/outbound sink. Coordinate login/disconnect notifications with members 1 and 4.
- **Deliverables:** UDP heartbeat sender/receiver, expiry scheduler, presence query/update integration, tests with an injectable monotonic clock and loopback UDP smoke test.
- **Acceptance tests:** valid increasing sequence refreshes presence; invalid token, duplicate sequence and oversized packet do not; 15 seconds of silence marks `OFFLINE`; a valid later heartbeat restores `ONLINE`; TCP disconnect marks `OFFLINE` and old tokens cannot revive it. Ensure one update per transition.
- **Two-minute demo:** login two loopback clients, observe presence snapshot, pause one client's UDP heartbeats for at least 15 seconds while keeping TCP connected, observe `OFFLINE`, resume to show `ONLINE`, then logout to show `OFFLINE`.

## Member 4 — Authentication & Security

- **Responsibilities:** credential verification, login/logout handlers, unique active usernames, ephemeral client IDs, session registry, random presence tokens, authorization and revocation. Define and enforce the selected transport security mode; never claim TLS exists until verified.
- **Suggested types:** `security.AuthenticationService`, `LoginHandler`, `SessionRegistry`, `Session`, `AuthorizationService`, `CredentialStore`, `PresenceTokenValidator`.
- **Dependencies:** server connection IDs/lifecycle and bounded worker execution for expensive password derivation; other modules call security contracts. TLS, if selected, needs joint integration with member 1 beneath framing. Demo identities remain local and disposable.
- **Deliverables:** credential/session contracts, login/logout and permission checks, restricted demonstration configuration guards, secret-safe diagnostics and security tests. TLS is a separate approved scope decision, not an assumed deliverable.
- **Acceptance tests:** valid login returns client ID/token; invalid login returns generic `AUTH_FAILED`; duplicate active username is refused; pre-login chat/file/presence query fails; second login is invalid; logout/disconnect invalidates token and transfer permissions; demo mode refuses non-loopback binds. Test that secrets are absent from logs.
- **Two-minute demo:** show insecure-demo warning and loopback configuration, reject an invalid dummy login, accept a valid one, reject an unauthenticated operation, logout and show the former session/token no longer authorizes access.

## Member 5 — Multithreading & File Sharing

- **Responsibilities:** bounded worker pool (initially two threads/32 queued tasks), consent and transfer state, chunk ordering, safe temporary storage, size/hash checks, cancellation and cleanup. Keep blocking disk work off the Selector; expose bounded execution to security for credential work; overload fails with `BUSY` rather than blocking.
- **Suggested types:** `transfer.TransferCoordinator`, `FileTransferHandler`, `TransferState`, `FileStorage`, `WorkerPool`; client-side `FileSender` and `FileReceiver` follow the same limits.
- **Dependencies:** authenticated participants from security; framed envelopes and outbound sink from server; worker completions posted to the Selector. Coordinate worker lifecycle with member 1. No extra TCP listener, raw stream mode or direct channel writes.
- **Deliverables:** offer/accept/reject/cancel flow, binary chunks over the existing TCP channel, temporary-file handling under `data/uploads/`, SHA-256 validation, CLI file commands and worker/transfer tests.
- **Acceptance tests:** consenting peers transfer a file and verify exact bytes/hash; empty file works; reject files over 10 MiB, invalid sequence, unsafe name and unauthorized accept; injected hash/disk failure aborts; disconnect removes partial files; queue saturation returns `BUSY`; chat remains responsive during file I/O. Test 30-second offer/progress expiry using a fake clock. Verify binary chunk size and UUID/sequence headers, 10 MiB total size and storage confinement under `data/uploads/`.
- **Two-minute demo:** Alice offers a small file, Bob accepts, transfer reaches recipient-confirmed `COMPLETED` with matching hash; send chat during a second transfer, cancel it and show temporary-file cleanup.

## Shared integration contracts and ownership

| Contract | Primary owner / required reviewers |
| --- | --- |
| TCP framing, `ConnectionContext`, `MessageHandler`, `OutboundSink`, Selector task queue | Member 1 / members 2–5 |
| Envelope schema, typed frame layouts, `FileChunk`, errors, `MessageCodec` / `FrameCodec` | Member 1 coordinates / all five approve wire changes |
| `SessionRegistry`, `AuthorizationService`, presence-token validation | Member 4 / members 1–3 and 5 |
| `PresenceListener`, snapshot and transition semantics | Member 3 / members 1, 2 and 4 |
| Transfer state and worker result contract | Member 5 / members 1 and 4 |
| CLI commands and startup configuration | Member 1 coordinates / each feature owner contributes |

Handlers and transfer coordination run on the Selector thread. Presence owns heartbeat timing; the security-owned session registry is thread-safe. Disk/expensive credential work runs on bounded workers, with immutable completions posted to the Selector. Security work sharing the pool must fail safely under overload and never block the event loop. Outbound enqueue acceptance does not guarantee delivery. Disconnect/revocation must invalidate pending callbacks before they mutate session/transfer state.

Agree interfaces and JSON fixtures first. Integrate framing plus a test fixture, then login, direct chat, UDP presence, and finally files. Each owner supplies tests for their contract and runs the joint loopback scenario; keep mocks aligned with the protocol fixtures. No feature is considered complete merely because these documents describe it. Baseline verification is `mvn clean test`; a successful build with no tests is foundation validation only, not evidence of implemented features.

## Final technical defaults

- Jackson databind 2.21.8 is pinned in the POM; implement strict JSON controls through the shared codec, with no polymorphic default typing.
- One TCP server at `127.0.0.1:5050`, separate UDP at `127.0.0.1:5051`; manual addressing. JSON controls and binary chunks share four-byte big-endian length plus kind framing. JSON bodies are at most 65,536 bytes; binary data is at most 16,384 bytes per chunk. Exact layouts and limits are normative in protocol.md.
- Heartbeats every 5 seconds; 15-second silence marks `OFFLINE` at the next one-second check. This replaces `AWAY` without revoking a healthy TCP session. Member 3 must test recovery and ensure logout does not publish a duplicate offline transition.
- Store generated relay/client files under `data/uploads/`, excluded from Git; use generated names and never overwrite user files. One transfer per client, 10 server-wide, 10 MiB per file, stop-and-wait binary chunks and 30-second offer/progress expiry.
- Localhost-only authentication demo with disposable credentials is the baseline; warn that unencrypted authentication is insecure. Network TCP authentication requires implemented TLS, and current UDP tokens stay loopback-only until a protected design is approved.
- Keep protocol.md queue/connection limits. Member 5 owns the shared bounded executor (two threads, 32 queued tasks); member 4 submits credential work through it. Reject saturation with `BUSY`; one connection may have only one credential task pending. Member 1 owns lifecycle and bounded Selector task submission.
- Member 1 coordinates CLI and common contracts; all five review wire/schema changes. Member 4 provisions local dummy identities. Assign actual member names before work starts; no selected default awaits approval.
- GUI, persistence, rooms, offline delivery, resume and LAN discovery remain deferred. Adding them or changing wire contracts requires an explicit team scope decision.
