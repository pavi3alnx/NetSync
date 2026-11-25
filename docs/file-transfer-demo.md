# Production file-transfer demo

This demonstrates the real `NetSyncServerApplication` and `NetSyncClient` on the
loopback-only, explicitly enabled insecure demo. Use disposable credentials only.
The automated end-to-end test uses two production clients and a multi-chunk file;
the interactive workflow below is documented but was not manually performed in
this review.

## Build and classpath (PowerShell)

Run from the repository root with Java 21 and Maven:

```powershell
mvn -B clean verify
mvn -B compile dependency:build-classpath "-Dmdep.outputFile=target/netsync-classpath.txt"
$netsyncClasspath = "target/classes;" + (Get-Content -Raw target/netsync-classpath.txt).Trim()
```

The classpath command uses the dependency plugin invocation already used by the
application and client READMEs. Keep the server and client terminals in the
repository root so the documented `data/uploads/` paths are predictable.

## Start server and clients

In a real interactive PowerShell terminal, enter:

```powershell
java -cp $netsyncClasspath com.netsync.app.NetSyncServerApplication --insecure-demo --users alice,bob --port 5050 --udp-port 5051
```

Enter fresh disposable passwords at the hidden prompts. In two other terminals,
construct `$netsyncClasspath` as above, then start one client in each terminal:

```powershell
java -cp $netsyncClasspath com.netsync.client.NetSyncClient
```

Use these CLI commands in each client:

```text
/connect 127.0.0.1 5050 5051
/login alice
```

and in the second client:

```text
/connect 127.0.0.1 5050 5051
/login bob
```

Use `/presence` in either client to display authenticated client IDs. The
presence snapshot also shows their current online state.

## Send and verify a file

Create a sample file in the sender's PowerShell window:

```powershell
$sample = Join-Path $PWD 'sample file.bin'
[IO.File]::WriteAllBytes($sample, [byte[]](0..255))
```

In Alice's client, offer it to Bob using Bob's client ID:

```text
/sendfile <bob-client-id> "sample file.bin"
```

The transfer offer prints a transfer ID. In Bob's client, accept it:

```text
/accept <transfer-id>
```

The recipient's generated final file is located at
`data/uploads/clients/<bob-client-id>/<transfer-id>.bin`. Verify it from
PowerShell using the same client ID and transfer ID:

```powershell
$received = Join-Path $PWD 'data/uploads/clients/<bob-client-id>/<transfer-id>.bin'
Get-FileHash $sample -Algorithm SHA256
Get-FileHash $received -Algorithm SHA256
```

The hashes should match. The CLI reports successful receipt only after recipient
length and SHA-256 validation, finalization, and the server's `FILE_RECEIVED`
confirmation path.

To reject a pending offer, use `/reject <transfer-id>` in Bob's client. To cancel
an active transfer, use `/cancel` or `/cancel <transfer-id>` in either participant
client. To exercise cross-module activity during a transfer, run `/chat
<recipient-client-id> hello` and `/presence` from a client while the offer or
transfer is active; both use the same TCP connection and the UDP presence socket.

Use `/logout` for logout and `/quit` or Ctrl+C to stop each client. Stop the server
with Ctrl+C. Runtime partial and completed files are under the ignored
`data/uploads/` tree; remove only the disposable demo data when finished.

The automated suite verifies production transfers (zero-byte, exact 16 KiB chunk
boundary, and multi-chunk files) with recipient consent, exact byte and SHA-256
comparison, rejection, sender/recipient cancellation, disconnect, logout,
unanswered-offer and stalled-transfer timeouts, authorization and integrity
rejections, backpressure, startup rollback, port-reuse restart, and concurrent
chat/presence responsiveness. The interactive two-terminal CLI workflow above
remains pending manual execution. The demo remains loopback-only and unencrypted;
TLS and remote use are not available.
