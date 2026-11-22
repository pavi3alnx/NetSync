# Phase 5A authentication server

`com.netsync.app.NetSyncServerApplication` is the application entry point for the
existing authentication services. The older `server.NioServer` main remains a
transport-only entry point; use the application main below for login/logout.

## Start from PowerShell

Run in a real interactive terminal, from the repository root, using Java 21:

```powershell
mvn -B compile dependency:build-classpath "-Dmdep.outputFile=target/server-classpath.txt"
$serverClasspath = "target/classes;" + (Get-Content -Raw target/server-classpath.txt).Trim()
java -cp $serverClasspath com.netsync.app.NetSyncServerApplication --insecure-demo --users alice,bob --port 5050
```

The names above are examples of disposable local identities. Choose your own
demo usernames (1-32 ASCII letters, digits or underscores, unique and
case-sensitive). The application prompts for each password with console echo
disabled. Use fresh disposable passwords, never a real account password.
Passwords are nonempty and limited by the existing credential store to 128 UTF-16
code units. The application takes ownership of the password arrays, builds salted
PBKDF2 verifiers using `CredentialStore`, and clears the provisioning arrays even
when provisioning fails. Accounts and sessions are in memory only.

Do not put passwords in arguments, source files, environment variables or files.
No account file is needed or supported. Redirected input and IDE consoles without
`System.console()` are refused. Use Windows Terminal or another real terminal.
Keep the terminal and machine trusted; this does not protect against local malware.

`--insecure-demo` and `--users` are required. `--port` defaults to 5050; zero selects
an ephemeral port (primarily for tests). Invalid, unknown and repeated options are
rejected before provisioning. There is no host/bind option: the existing core binds
only to `127.0.0.1`. A warning identifies the mode as insecure and unsuitable for
untrusted networks/deployment. TCP is unencrypted; there is no TLS.

## Current behavior

- Real `LOGIN_REQUEST` / `LOGIN_RESPONSE` and `LOGOUT_REQUEST` / `ACK` handlers.
- One security-owned session registry shared by authentication, authorization and
  the transport's live connection lookup.
- Two bounded credential workers with 32 queued tasks, reusing
  `TransferWorkerPool` as an executor only. No transfer service/storage starts.
- Disconnect cancels pending authentication and revokes the session/token.
- Failed login returns generic `AUTH_FAILED`, then closes after output drains.
- Logout returns `ACK/ACCEPTED`, revokes the session, then closes after draining.
- Existing 10-second login deadline and 1,024-entry JSON duplicate-ID guard remain
  active. A duplicate returns `DUPLICATE_MESSAGE` before handler dispatch.
- Other decoded operations require authentication and are then rejected as
  unsupported. Binary frames receive a bounded error response; no no-op receiver
  or placeholder feature handler is installed.

The standalone client CLI still has **no login/logout commands**. This phase is
verified by loopback TCP integration tests using the real wire codec. No chat,
UDP presence, heartbeat sender, file transfer or client changes are included.
Login responses retain the existing presence-token/interval fields, but no UDP
service is running in this phase. Do not present this as the complete NetSync demo.

## Stop and lifecycle

Press **Ctrl+C**. The shutdown hook closes TCP, invokes existing disconnect hooks,
then stops the worker pool and checks termination. Startup bind/construction
failure rolls back the resources already created. Close is idempotent, including
before start and after startup failure. Each owner is single-use; create a new
owner to restart. The main also detects a stopped/failed transport and cleans up.

Workers have the existing five-second graceful wait, interruption fallback, and
an additional one-second termination check. Java cannot forcibly stop a task that
ignores interruption. `NioServer.close()` retains its existing Selector join
without a timeout; this phase installs only bounded security/composition callbacks
and does not change that core API. Application shutdown does not promise response
draining to peers; normal logout/failed-login close-after-drain is preserved.

The existing session API has no authentication timestamp: a live session visible
at the core deadline check wins its boundary race. Closed connections cannot be
reopened by late completions; disconnect cleanup cancels pending authentication.

## Verify

```powershell
mvn -B "-Dtest=com.netsync.app.*Test" test
mvn -B clean verify
```

Application tests exercise real TCP and security services. A test-only bridge in
the server test package exposes the existing monotonic clock seam for deterministic
deadline tests; production uses the normal `System.nanoTime()` transport.
