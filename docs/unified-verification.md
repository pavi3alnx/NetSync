# NetSync — Phase 6 Unified Application Verification & Demonstration Guide

## 1. Verified Project Baseline

- **Branch**: `feature/unified-application-verification`
- **Baseline HEAD Commit**: `b7a442a883ab7a19e76160a1ffb66b5d8f6cf2de`
- **Java Version**: Java 21 (`21.0.4` LTS)
- **Build Tool**: Apache Maven (`3.9.11`)
- **Initial Baseline Test Count**: 818 test executions (`0` failures, `0` errors, `0` skipped)
- **Final Phase 6 Test Count**: 864 test executions (`0` failures, `0` errors, `0` skipped)
- **Source Inventory**: 74 production Java files (`src/main/java`) and 61 test Java files (`src/test/java`)
- **Production Entry Points**:
  - Server: `com.netsync.app.NetSyncServerApplication`
  - Client CLI: `com.netsync.client.NetSyncClient`
  - Transport-only server main (non-application): `com.netsync.server.NioServer`

> **Note on Packaging**: `target/netsync-1.0.0-SNAPSHOT.jar` is a standard compiled-classes JAR without a `Main-Class` manifest attribute or shaded Jackson dependencies. Production execution uses `mvn -B compile dependency:build-classpath` as documented below.

---

## 2. Architecture Summary & Production Integration Inventory

`NetSyncServerApplication` composes all five member modules over one loopback NIO TCP server (`127.0.0.1`) and one loopback UDP presence listener (`127.0.0.1`):

1. **Core TCP Transport (`com.netsync.server`, `com.netsync.common`)**:
   - `NioServer` runs a single nonblocking `Selector` event loop (`netsync-tcp-selector`) enforcing per-pass read budgets (`32` frames, `256 KiB`), write budgets (`256 KiB`), selector task budgets (`32` tasks), a `1,024`-entry per-connection FIFO duplicate JSON message-ID guard, a `10`-second unauthenticated login deadline, and a `15`-second partial-frame / closing-drain deadline.
   - `SelectorTaskQueue` (`256` slots) shares capacity between ordinary background tasks and pre-reserved worker completion slots (`SelectorTasks.Reservation`).
2. **Authentication & Security (`com.netsync.security`)**:
   - `CredentialStore` hashes disposable passwords in memory with `PBKDF2WithHmacSHA256` (`120,000` iterations, `16`-byte random salt, `256`-bit key) and uses a dummy verifier for constant-structure unknown-username verification.
   - `SessionRegistryImpl` is the single authoritative registry shared by `AuthService`, `AuthorizationServiceImpl`, `NioServer`, `PresenceHandler`, `HeartbeatTracker`, `PresenceEventPublisher`, `ChatHandler`, and `FileTransferService`.
   - `LoginHandler` offloads PBKDF2 verification to the shared bounded `TransferWorkerPool` after reserving both an `AuthService` pending slot and a `SelectorTasks.Reservation`.
3. **UDP Presence (`com.netsync.presence`)**:
   - `UdpPresenceService` binds a loopback `DatagramChannel` before TCP starts accepting, decodes `Heartbeat` packets (`<= 1,024` bytes), and runs a 1-second expiry check (`15`-second timeout) that also triggers `PresenceEventPublisher.retryPending()`.
   - `PresenceEventPublisher` coalesces per-client latest `PresenceState` transitions in a bounded mailbox (`200` entries) and drains `PRESENCE_UPDATE` broadcasts on the Selector thread.
   - UDP presence expiry is advisory and never revokes TCP authentication or active chat/file-transfer sessions.
4. **Private TCP Chat (`com.netsync.chat`)**:
   - `ChatHandler`, `ChatService`, and `MessageRouter` execute nonblockingly on the Selector thread, deriving sender identity strictly from the authenticated `SessionView`, routing `CHAT_DELIVER` frames by recipient `clientId`, and returning correlated `ACK(CHAT_SEND, QUEUED)` or `ERROR(RECIPIENT_UNAVAILABLE | BUSY | SIZE_LIMIT)`.
5. **Consent-Based File Transfer (`com.netsync.transfer`)**:
   - `FileTransferCoordinator` and `FileTransferService` validate participant ownership, state transitions (`OFFERED -> ACCEPTED -> IN_PROGRESS -> SERVER_VERIFIED -> COMPLETED`), stop-and-wait sequencing, chunk bounds (`16,384` bytes), file bounds (`10 MiB`), and concurrency limits (`1` per client, `10` server-wide) on the Selector thread while offloading `.part` disk writes, SHA-256 hashing, and bounded partial-file cleanup to `TransferWorkerPool`.
6. **Shared Client CLI (`com.netsync.client`)**:
   - `NetSyncClient` wires `TcpClient`, `CommandRegistry`, `ClientOutputQueue`, `ClientAuthenticationAdapter`, `ClientPresenceAdapter`, `ChatClientAdapter`, and `FileTransferClientAdapter` with generation-bound outbound sends and nonblocking terminal output.

---

## 3. Requirements Traceability Matrix

> **Academic Compliance Disclaimer**: The original university assignment specification and marking rubric are not present in the repository. Consequently, full formal compliance with external university grading criteria cannot be certified; traceability below is established against all repository protocol specifications, module READMEs, and architectural contracts.

| Requirement Domain | Responsible Module | Production Implementation | Production Integration Location | Primary Test Evidence | Status | Remaining Limitations |
|---|---|---|---|---|---|---|
| Single NIO Selector TCP server, partial/coalesced frames, read/write budgets, partial kernel writes | `com.netsync.server` | `NioServer.runLoop`, `read`, `write`, `ConnectionState` | `NetSyncServerApplication.start` | `NioServerTest` (`partialKernelWritesUnderSmallSocketBuffersDrainCompleteFramesInOrder`, `coalescedFramesBeyondReadBudgetAreEventuallyDispatched`), `ProtocolGuardsTest` | Verified | `NioServer.close()` joins the selector thread without a timeout (bounded by nonblocking callbacks). |
| Selector task queue & completion reservations | `com.netsync.server` | `SelectorTaskQueue.submit`, `tryReserveCompletion`, `finishShutdown` | `NetSyncServerApplication.start` (`nio.selectorTasks()`) | `SelectorTaskQueueTest`, `CoreIntegrationTest` | Verified | None. |
| PBKDF2 salted password hashing, generic `AUTH_FAILED`, hidden console input, session/token authority | `com.netsync.security`, `com.netsync.app` | `CredentialStore.verify`, `AuthService.authenticateReserved`, `LoginHandler`, `LogoutHandler`, `DemoAccounts` | `NetSyncServerApplication.start`, `NetSyncClient` (`ClientAuthenticationAdapter`) | `CredentialStoreTest`, `AuthServiceTest`, `LoginHandlerTest`, `ClientAuthenticationIntegrationTest`, `UnifiedApplicationProductionIntegrationTest` | Verified | Loopback demo is unencrypted (`--insecure-demo`); no TLS. |
| UDP heartbeat authentication, 5s interval, 15s expiry, recovery, bounded publication | `com.netsync.presence` | `UdpPresenceService`, `HeartbeatTracker`, `PresenceEventPublisher`, `HeartbeatSender`, `PresenceHandler` | `NetSyncServerApplication.start`, `NetSyncClient` (`ClientPresenceAdapter`) | `HeartbeatTrackerTest`, `PresenceEventPublisherTest`, `PresenceProductionIntegrationTest`, `UnifiedApplicationProductionIntegrationTest` | Verified | None. |
| Authenticated private TCP chat, `ACK/QUEUED` correlation, 128-request pending bound, 30s timeout | `com.netsync.chat` | `ChatHandler`, `ChatService`, `MessageRouter`, `ChatClientAdapter` | `NetSyncServerApplication.start`, `NetSyncClient` (`ChatClientAdapter`) | `ChatHandlerTest`, `ChatClientAdapterTest`, `ChatProductionIntegrationTest`, `UnifiedApplicationProductionIntegrationTest` | Verified | `ACK/QUEUED` confirms recipient TCP queue acceptance, not read receipt. |
| Consent-based file transfer, stop-and-wait chunks, SHA-256 verification, `FILE_RECEIVED` confirmation | `com.netsync.transfer` | `FileTransferService`, `FileSender`, `FileReceiver`, `FileTransferClientService`, `FileTransferClientAdapter` | `NetSyncServerApplication.start`, `NetSyncClient` (`FileTransferClientAdapter`) | `FileTransferProductionIntegrationTest`, `FullClientServerFileTransferIntegrationTest`, `UnifiedApplicationProductionIntegrationTest` | Verified | Single active transfer per client, max 10 server-wide, max 10 MiB file size. |
| Safe generated storage paths, non-replacing finalization, bounded worker cleanup & deferred cleanup capacity | `com.netsync.transfer` | `SafeFileStorage`, `FileTransferService.scheduleCleanup`, `isDeferredCleanupExhausted`, `FileReceiver.cleanupPartialAsync` | `NetSyncServerApplication.start`, `NetSyncClient` | `SafeFileStorageTest`, `FileTransferConcurrencyAndFailureTest` (`deferredCleanupCapacityExhaustionRejectsNewOffersWithBusyWithoutEvictingPendingCleanups`), `FileTransferProductionIntegrationTest` | Verified | Portable Java NIO cannot prevent local-filesystem TOCTOU symlink swaps by a same-UID attacker; requires trusted local storage directory permissions. |
| Shared client transport, generation isolation, bounded output queue, command registry | `com.netsync.client` | `TcpClient`, `ClientDispatcher`, `ClientOutputQueue`, `CommandRegistry`, `NetSyncClient` | `NetSyncClient` | `TcpClientTest`, `ClientOutputQueueTest`, `ChatClientOutputPathTest`, `FileTransferClientAdapterTest` | Verified | Java cannot forcibly preempt a custom output sink or dispatch callback that ignores thread interruption. |

---

## 4. Unified Test Scenarios (Phase 6B)

Implemented in `com.netsync.app.UnifiedApplicationProductionIntegrationTest`:
1. **`unifiedThreeClientEndToEndScenarioWithOverlappingTransferChatAndPresence`**:
   - Starts production TCP + UDP server and connects three production `NetSyncClient` instances (`alice`, `bob`, `carol`).
   - Verifies `/login`, `/presence` snapshots, and private `/chat` between Alice and Bob with zero leakage to Carol.
   - Offers a multi-chunk file (`33,285` bytes) from Alice to Bob, verifies zero `.part`/`.bin` creation prior to `/accept`, holds chunk 0 inside `SafeFileStorage.appendChunk`, exchanges overlapping `/chat` messages (`alice -> bob` and `carol -> alice`), verifies live UDP heartbeats and `/presence` responsiveness, releases the held write, and verifies byte-for-byte + SHA-256 equality, server `.part` cleanup, and zero duplicate generic shell output.
2. **`unifiedAuthenticationAndSessionTransitionsCleanStateAndIsolateGenerations`**:
   - Exercises invalid login (`AUTH_FAILED`), valid login, duplicate active username rejection, pending transfer cleanup and token revocation on `/logout`, rejection of protected commands after logout, `/disconnect`, `/connect`, re-login with fresh `clientId`/`connectionId`/token, and subsequent valid chat and file transfer.
3. **`unifiedPresenceExpiryDuringActiveTcpOperationsAndRecoveryWhenHeartbeatsResume`**:
   - Expires Alice's UDP presence (`PresenceState.OFFLINE`) via injectable clock while keeping TCP open, verifies private `/chat` and `/sendfile` + `/accept` succeed while `OFFLINE`, resumes UDP heartbeats with higher sequence numbers, and verifies `PresenceState.ONLINE` recovery broadcast.
4. **`unifiedFailureIsolationProtectsHealthyClientsFromMisbehavingPeer`**:
   - Verifies that an unauthenticated request (`UNAUTHENTICATED`), duplicate message ID (`DUPLICATE_MESSAGE`), third-party transfer cancellation (`FORBIDDEN`), and fatal malformed JSON frame (`MALFORMED_MESSAGE` + connection close) from a third peer do not disrupt an active file transfer and chat between Alice and Bob.
5. **`unifiedShutdownWithPendingUnansweredFileOfferAndSustainedPresenceTaskRejectionTerminatesCleanly` (`@RepeatedTest(20)`)**:
   - Initiates a file offer that remains unanswered (`OFFERED`), saturates the `SelectorTaskQueue` while triggering a presence state change, shuts down `NetSyncServerApplication`, and verifies clean termination of workers, timeout scheduler, sockets, and transfer/session registries without false completion or resource leaks.

---

## 5. Resource-Bounds Table

| Resource Name | Owning Component | Capacity / Limit | Overflow / Rejection Behavior | Cleanup Behavior | Shutdown Behavior | Regression Evidence |
|---|---|---|---|---|---|---|
| Active TCP Connections (`connections`) | `NioServer` | `100` (`ProtocolLimits.MAX_CONNECTIONS`) | Immediately closes newly accepted `SocketChannel` beyond 100 | Removed in `disconnect(ConnectionState)` | All channels closed in `cleanup()` | `NioServerTest`, `NetSyncServerApplicationTest` |
| Per-Connection Outbound Queue (`output` / `queuedBytes`) | `ConnectionState` (`NioServer`) | `256` frames / `1,048,576` bytes | Disconnects slow-consumer connection and returns `EnqueueResult.REJECTED` | Cleared on `disconnect()` | Cleared on `cleanup()` | `NioServerTest.outputQueueOverflowDisconnectsOnlyThatClient`, `outputByteLimitRejectsBeforeFrameCountLimitAndClearsQueue` |
| Per-Connection Duplicate JSON ID Window (`recentJsonIds`) | `ConnectionState` (`NioServer`) | `1,024` IDs (`ProtocolLimits.MAX_RECENT_MESSAGE_IDS`) | FIFO eviction of oldest ID; duplicate returns `ERROR(DUPLICATE_MESSAGE)` | Cleared on `disconnect()` | Cleared on `cleanup()` | `ProtocolGuardsTest.fullHistoryEvictsOldestOnWire`, `fifoWindowIsBoundedAndDuplicatesDoNotRefreshRetention` |
| Selector Task & Reservation Mailbox (`SelectorTaskQueue`) | `NioServer` | `256` total (`ProtocolLimits.SELECTOR_QUEUE_SIZE`) | `submit()` returns `false`; `tryReserveCompletion()` returns `Optional.empty()` | Tasks polled in batches of `32`; closed reservations release slot | `beginShutdown()` converts unused reservations to cancellations; `finishShutdown()` drains up to `256` tasks | `SelectorTaskQueueTest`, `CoreIntegrationTest` |
| Pending Authentication Reservations (`pendingConnections`) | `AuthService` / `LoginHandler` | `100` (`ProtocolLimits.MAX_CONNECTIONS`) | Returns `ERROR(BUSY)` (or `INVALID_STATE` if already pending on same connection) | Removed on completion, cancellation, or `onDisconnected` | Cancelled via `onDisconnected` and `authentication.disconnect` | `AuthServiceTest`, `LoginHandlerTest`, `NetSyncServerApplicationTest` |
| Active Sessions (`byConnectionId`, `byClientId`, `byUsername`) | `SessionRegistryImpl` | `100` (bounded by `MAX_CONNECTIONS` and demo account limit `1..100`) | Rejects conflicting connection or duplicate active username | Removed on `logout` or `disconnect` | Revoked in `NetSyncServerApplication.close()` | `SessionRegistryImplTest`, `NetSyncServerApplicationTest` |
| Presence Tracker Sessions (`sessions`) | `HeartbeatTracker` | `100` (`ProtocolLimits.MAX_CONNECTIONS`) | `registerSession` returns `false` | Removed on `revokeSession(clientId)` | Cleared when sessions are revoked on shutdown | `HeartbeatTrackerTest` |
| Presence Event Mailbox (`entries`) | `PresenceEventPublisher` | `200` (`2 * ProtocolLimits.MAX_CONNECTIONS`) | `registerSession` returns `false` (login handler aborts session admission) | Coalesces per-client state; removes revoked entries once final `OFFLINE` drains | `close()` sets `closed = true` and clears `entries` | `PresenceEventPublisherTest` |
| Server Shared Worker Pool (`TransferWorkerPool`) | `NetSyncServerApplication` (shared by Auth & Transfer) | `2` threads, `32` queued tasks | `AbortPolicy` throws `RejectedExecutionException`; callers release reservation and send `ERROR(BUSY)` | Workers drain queue and deferred cleanups | `close()` waits up to `5s`, then `shutdownNow()`, plus `1s` app verification | `TransferWorkerPoolTest`, `FileTransferConcurrencyAndFailureTest` |
| Active Transfer Registry (`activeTransfers`, `clientToTransfer`) | `TransferRegistry` | `10` global (`MAX_TRANSFERS`), `1` per client (`MAX_CLIENT_TRANSFERS`) | Throws `IllegalStateException`; `FileTransferService` sends `ERROR(BUSY)` | Removed on completion, rejection, cancellation, timeout, or disconnect | Cleared via disconnect listeners on server close | `TransferRegistryTest`, `FileTransferProductionIntegrationTest` |
| Deferred & Failed Partial-File Cleanups (`deferredCleanups`, `failedCleanups`) | `FileTransferService` | `64` deferred (`MAX_DEFERRED_CLEANUPS`), `64` failed (`MAX_FAILED_CLEANUPS`) | When `deferredCleanups.size() + registry.activeCount() >= 64`, new `FILE_OFFER` is rejected with `ERROR(BUSY)` without evicting uncleaned files | Drained by workers in `finally` and on `checkTimeouts()`; `failedCleanups` bounds unrecoverable OS failures | Drained by active workers before pool termination | `FileTransferConcurrencyAndFailureTest.deferredCleanupCapacityExhaustionRejectsNewOffersWithBusyWithoutEvictingPendingCleanups` |
| Client Outbound Queue (`output` / `bytes`) | `TcpClient` | `256` frames / `1,048,576` bytes | `send()` returns `EnqueueResult.REJECTED`; `sendOrThrow()` throws `RejectedExecutionException` | Drained by `netsync-client-write`; cleared on `stop()` | Cleared on `disconnect()` / `close()` | `TcpClientTest.queueCountLimitIsDeterministicAndFifo`, `byteLimitAndTransferAdapterReportRejection` |
| Client Inbound Dispatch Queue (`dispatch`) | `TcpClient` | `1` thread, `256` queued callbacks | Stops connection with `Failure.DISPATCH_FULL` | Cleared on explicit disconnect, error, or close; drained on orderly `REMOTE_CLOSED` | `shutdownNow()` and `awaitTermination(2s)` | `TcpClientTest.slowHandlerDoesNotBlockReadsAndDispatchOverflowCloses` |
| Client Pending Chat Map & Tombstones (`pendingChats`, `recentlyCompleted`) | `ChatClientAdapter` | `128` pending (`MAX_PENDING_REQUESTS`), `128` tombstones | Rejects `/chat` with `InputException` when 128 requests are unanswered | Removed on correlated `ACK`/`ERROR` or 30s expiry; cleared on logout/disconnect | Cleared and 100ms timer shut down in `close()` | `ChatClientAdapterTest`, `ChatClientLifecycleTest` |
| Client Receiver Pending Chunk Queue (`queuedChunks`) | `FileReceiver` | `32` chunks (`MAX_QUEUED_CHUNKS`), max `512 KiB` | Fails transfer (`"Receiver pending chunk limit reached"`), sends `FILE_CANCEL`, deletes `.part` | Drained sequentially by receiver worker; cleared on cancel/fail | Cleared and cancelled in `close()` | `FileTransferClientServiceTest.clientWorkerSubmissionRejectionAndReceiverChunkQueueLimitEnforceBoundedBackpressure` |
| Client Terminal Output Queue (`pending`) | `ClientOutputQueue` | `64` items (`DEFAULT_CAPACITY`) | Coalesces `PRESENCE` items; evicts oldest `PRESENCE` for `IMPORTANT` or increments `omitted` and prints visible notice | Drained by `netsync-client-output` daemon thread | `close()` joins up to `100ms`, clears `pending`, and interrupts worker | `ClientOutputQueueTest`, `ChatClientOutputPathTest` |

---

## 6. Security Threat Model, Startup/Shutdown, and Known Limitations

1. **Loopback Insecure Demo Scope**:
   - `NetSyncServerApplication` requires `--insecure-demo` and `--users <u1,u2,...>` (1–100 unique ASCII usernames), binds strictly to `127.0.0.1`, reads disposable passwords via `System.console().readPassword(...)` with echo disabled, and zeroes password `char[]` buffers immediately after PBKDF2 verifier creation.
   - Transport is unencrypted loopback TCP/UDP; TLS and remote network deployment are out of scope.
2. **Local Filesystem TOCTOU Symlink/Junction Limitation**:
   - `SafeFileStorage` never resolves user-supplied filenames on disk (using `<transferId>.part` and `<transferId>.bin`), omits `StandardCopyOption.REPLACE_EXISTING` on finalization, and checks every ancestor directory and target path with `LinkOption.NOFOLLOW_LINKS`.
   - Because portable Java NIO (`java.nio.file.Files`) lacks POSIX `openat(O_NOFOLLOW | O_DIRECTORY)` directory-descriptor pinning, a local process with write permissions to the storage directory tree could swap a directory component with a symlink or junction between path validation and file open/move/delete. The demo requires an owner-only local storage directory tree (`data/uploads/`).
3. **Operating-System & JVM Thread Interruption Boundaries**:
   - Java cannot forcibly terminate an uninterruptible kernel disk write, delete a file permanently locked by an external privileged OS process, or preempt a custom output stream / callback that ignores `Thread.interrupt()`. Shutdown waits and deletion retries are strictly bounded (`50` deletion retries; `5s + 1s` worker pool termination wait; `100ms` output queue join; `2s` client dispatch join).

---

## 7. Verified Four-Terminal Windows PowerShell Demonstration Procedure (Phase 6C)

> **Execution Status**: `PENDING USER EXECUTION` (Production login requires interactive `System.console()` hidden password entry across four live Windows PowerShell terminals).
> **Quoted Paths & Spaces**: `CommandRegistry.parse` supports single-quoted (`'...'`) and double-quoted (`"..."`) arguments containing spaces, as well as backslash escaping (`\`). Always wrap file paths containing spaces in double quotes.
> **Storage Safety Note**: Never recursively delete the repository `data/` directory. Clean up only the explicit sample file in `$env:TEMP` and the specific `<transfer-id>.bin` artifact created during the demonstration.

### Step 1 — Build & Construct Dependency Classpath (Run once in Terminal 1)

```powershell
Set-Location "C:\Users\hp\OneDrive\Desktop\NetSync"
mvn -B compile dependency:build-classpath "-Dmdep.outputFile=target/netsync-classpath.txt"
```

### Step 2 — Start the Production Server (Terminal 1)

```powershell
Set-Location "C:\Users\hp\OneDrive\Desktop\NetSync"
$netsyncClasspath = "target/classes;" + (Get-Content -Raw "target/netsync-classpath.txt").Trim()
java -cp $netsyncClasspath com.netsync.app.NetSyncServerApplication --insecure-demo --users alice,bob,carol --port 5050 --udp-port 5051
```
- **Action**: At the three interactive hidden console prompts (`Disposable password for alice: `, `Disposable password for bob: `, `Disposable password for carol: `), enter disposable passwords (for example: `demo-pass-1`, `demo-pass-2`, `demo-pass-3`).
- **Expected Output**:
  - `INSECURE DEMO: loopback only; use disposable credentials. No transport confidentiality; unsuitable for untrusted networks or deployment.`
  - `NetSync server listening on 127.0.0.1 TCP 5050, UDP 5051`

### Step 3 — Start Three Production Clients (Terminals 2, 3, and 4)

In **Terminal 2 (Alice)**, **Terminal 3 (Bob)**, and **Terminal 4 (Carol)**, run:

```powershell
Set-Location "C:\Users\hp\OneDrive\Desktop\NetSync"
$netsyncClasspath = "target/classes;" + (Get-Content -Raw "target/netsync-classpath.txt").Trim()
java -cp $netsyncClasspath com.netsync.client.NetSyncClient
```
- **Expected Output in each client terminal**:
  - `Welcome to NetSync. Use /help to list commands.`
  - `Unencrypted loopback demo: use only disposable local credentials. Log in within 10 seconds of connecting.`

### Step 4 — Connect and Authenticate Alice, Bob, and Carol (Within 10 Seconds of Connecting Each)

- **Terminal 2 (Alice)**:
  ```text
  /connect 127.0.0.1 5050 5051
  /login alice
  ```
  *(Enter Alice's disposable password at the hidden `Disposable password: ` prompt)*
- **Terminal 3 (Bob)**:
  ```text
  /connect 127.0.0.1 5050 5051
  /login bob
  ```
  *(Enter Bob's disposable password at the hidden `Disposable password: ` prompt)*
- **Terminal 4 (Carol)**:
  ```text
  /connect 127.0.0.1 5050 5051
  /login carol
  ```
  *(Enter Carol's disposable password at the hidden `Disposable password: ` prompt)*
- **Expected Output**:
  - Each client prints `Connected.`, `Login requested.`, and `Logged in as <username> (client ID: <uuid>).`
  - Record `<alice-uuid>`, `<bob-uuid>`, and `<carol-uuid>` from the login output.
  - Already-logged-in clients print `Presence: <username> [<uuid>] is ONLINE.`

### Step 5 — Query Presence (`/presence`)

- In **Terminal 2 (Alice)** and **Terminal 4 (Carol)**, run:
  ```text
  /presence
  ```
- **Expected Output**:
  - `Presence snapshot:`
  - `alice [<alice-uuid>]: ONLINE`
  - `bob [<bob-uuid>]: ONLINE`
  - `carol [<carol-uuid>]: ONLINE`

### Step 6 — Exchange Private Chat & Verify Third-Party Isolation

- In **Terminal 2 (Alice)**, send a private message to Bob:
  ```text
  /chat <bob-uuid> Hello Bob from Alice!
  ```
- In **Terminal 3 (Bob)**, reply to Alice:
  ```text
  /chat <alice-uuid> Hi Alice, received loud and clear!
  ```
- **Expected Output**:
  - Alice sees:
    - `[HH:mm:ss] [Me -> <bob-uuid>]: Hello Bob from Alice! (sending...)`
    - `[Chat] Queued for recipient <bob-uuid>; this does not confirm that it was read.`
    - `[HH:mm:ss] [From <bob-uuid>]: Hi Alice, received loud and clear!`
  - Bob sees:
    - `[HH:mm:ss] [From <alice-uuid>]: Hello Bob from Alice!`
    - `[HH:mm:ss] [Me -> <alice-uuid>]: Hi Alice, received loud and clear! (sending...)`
    - `[Chat] Queued for recipient <alice-uuid>; this does not confirm that it was read.`
  - **Terminal 4 (Carol)** receives **neither** private chat message.

### Step 7 — File Offer & Rejection (`/sendfile` + `/reject`)

- Create a temporary sample file in `$env:TEMP` (run in a separate PowerShell prompt or before launching Alice's client; forward slashes or escaped backslashes work inside quotes):
  ```powershell
  $samplePath = Join-Path $env:TEMP "netsync demo sample.bin"
  [IO.File]::WriteAllBytes($samplePath, [byte[]](0..255 + 0..255))
  Write-Host "Sample file path for /sendfile:" ($samplePath -replace '\\','/')
  ```
- In **Terminal 2 (Alice)**, offer the file (using the printed path wrapped in double quotes, which also verifies paths containing spaces):
  ```text
  /sendfile <bob-uuid> "<printed-sample-path-with-forward-slashes>"
  ```
- **Expected Output**:
  - Alice sees: `[Transfer] Offering file netsync demo sample.bin (512 B) to <bob-uuid>...`
  - Bob sees: `[Transfer] Incoming file offer from <alice-uuid>: netsync demo sample.bin (512 B). Transfer ID: <reject-transfer-id>. Type /accept <reject-transfer-id> or /reject <reject-transfer-id>`
- In **Terminal 3 (Bob)**, reject the offer:
  ```text
  /reject <reject-transfer-id>
  ```
- **Expected Output**:
  - Bob sees: `[Transfer] Rejected offer for transfer <reject-transfer-id>.` and `[Transfer] Rejected [<reject-transfer-id>]: Rejected by user`
  - Alice sees: `[Transfer] Rejected [<reject-transfer-id>]: Recipient rejected transfer offer`

### Step 8 — File Offer & Acceptance (`/sendfile` + `/accept`)

- In **Terminal 2 (Alice)**, offer the file again:
  ```text
  /sendfile <bob-uuid> "<printed-sample-path-with-forward-slashes>"
  ```
- In **Terminal 3 (Bob)**, accept the new `<accept-transfer-id>`:
  ```text
  /accept <accept-transfer-id>
  ```
- **Expected Output**:
  - Bob sees:
    - `[Transfer] Accepted offer for transfer <accept-transfer-id>. Starting download...`
    - `[Transfer] Completed [<accept-transfer-id>]: Saved to C:\Users\hp\OneDrive\Desktop\NetSync\data\uploads\clients\<bob-uuid>\<accept-transfer-id>.bin (512 bytes)`
  - Alice sees:
    - `[Transfer] Completed [<accept-transfer-id>]: 512 bytes sent successfully`

### Step 9 — Exact Received-File Path & SHA-256 Comparison

- Once clients exit (or in a separate PowerShell window), compare the SHA-256 digests of the source file in `$env:TEMP` and Bob's finalized destination file (`C:\Users\hp\OneDrive\Desktop\NetSync\data\uploads\clients\<bob-uuid>\<accept-transfer-id>.bin`):
  ```powershell
  $samplePath = Join-Path $env:TEMP "netsync demo sample.bin"
  $receivedPath = "C:\Users\hp\OneDrive\Desktop\NetSync\data\uploads\clients\<bob-uuid>\<accept-transfer-id>.bin"
  Get-FileHash -Path $samplePath -Algorithm SHA256
  Get-FileHash -Path $receivedPath -Algorithm SHA256
  ```
- **Expected Output**: Both `Get-FileHash` commands output the exact same 64-character SHA-256 hex digest (`7873A32818D9F6D2E767871F9C80E5E4F337A97F430C5A2C000A38B7A6701A94`).

### Step 10 — Transfer Cancellation (`/cancel` on an Unanswered Offer)

- In **Terminal 2 (Alice)**, send a new file offer to Bob:
  ```text
  /sendfile <bob-uuid> "<printed-sample-path-with-forward-slashes>"
  ```
- Before Bob accepts or rejects, cancel the active offer in **Terminal 2 (Alice)**:
  ```text
  /cancel
  ```
- **Expected Output**:
  - Alice sees: `[Transfer] Cancelled [<cancel-transfer-id>]: Cancelled by sender` and `[Transfer] Cancelled active transfer <cancel-transfer-id>.`
  - Bob sees: `[Transfer] Cancelled [<cancel-transfer-id>]: Transfer cancelled by peer`

### Step 11 — Logout & Presence Revocation (`/logout`)

- In **Terminal 4 (Carol)**, log out:
  ```text
  /logout
  ```
- **Expected Output**:
  - Carol sees: `Logout requested.` and `Logged out.`
  - Alice and Bob see: `Presence: carol [<carol-uuid>] is OFFLINE.`

### Step 12 — Explicit Disconnect & Reconnect (`/disconnect` + `/connect`)

- In **Terminal 3 (Bob)**, disconnect and reconnect:
  ```text
  /disconnect
  /connect 127.0.0.1 5050 5051
  ```
- **Expected Output**:
  - Bob sees: `Disconnected.` followed by `Connected.`
  - Alice sees: `Presence: bob [<bob-uuid>] is OFFLINE.`

### Step 13 — Reauthentication & Fresh Operation

- In **Terminal 3 (Bob)**, log in again within 10 seconds of connecting and send a fresh chat message to Alice:
  ```text
  /login bob
  ```
  *(Enter Bob's disposable password at `Disposable password: `)*
  ```text
  /presence
  /chat <alice-uuid> Reconnected and authenticated with a new clientId!
  ```
- **Expected Output**:
  - Bob receives a fresh `<new-bob-uuid>` (`Logged in as bob (client ID: <new-bob-uuid>).`).
  - Alice sees `Presence: bob [<new-bob-uuid>] is ONLINE.` and `[HH:mm:ss] [From <new-bob-uuid>]: Reconnected and authenticated with a new clientId!`.

### Step 14 — Client Shutdown (`/quit`)

- In **Terminals 2, 3, and 4**, run:
  ```text
  /quit
  ```
- **Expected Output**: Each client process terminates cleanly with exit code `0`.

### Step 15 — Server Shutdown & Targeted Artifact Cleanup

- In **Terminal 1**, press **Ctrl+C** to trigger the `netsync-app-shutdown` hook and stop `NetSyncServerApplication`.
- Clean up only the explicit demonstration files created during the session (do **not** recursively delete `data/`):
  ```powershell
  Remove-Item -LiteralPath (Join-Path $env:TEMP "netsync demo sample.bin") -Force -ErrorAction SilentlyContinue
  Remove-Item -LiteralPath "C:\Users\hp\OneDrive\Desktop\NetSync\data\uploads\clients\<bob-uuid>\<accept-transfer-id>.bin" -Force -ErrorAction SilentlyContinue
  ```

### Step 16 — Server Restart on the Same TCP and UDP Ports (`5050` / `5051`)

- In **Terminal 1**, immediately re-run the exact server startup command from Step 2:
  ```powershell
  java -cp $netsyncClasspath com.netsync.app.NetSyncServerApplication --insecure-demo --users alice,bob,carol --port 5050 --udp-port 5051
  ```
- Enter the disposable passwords at the prompts and confirm the server prints `NetSync server listening on 127.0.0.1 TCP 5050, UDP 5051`, proving that both TCP port `5050` and UDP port `5051` were cleanly released and rebound. Press **Ctrl+C** to exit.
