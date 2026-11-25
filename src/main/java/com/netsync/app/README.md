# Production loopback demo application

`com.netsync.app.NetSyncServerApplication` composes authentication, UDP presence,
private TCP chat, and consent-based file transfer over the existing NIO server. The older `server.NioServer`
main remains a transport-only entry point; use the application main below.

## Start from PowerShell

Run in a real interactive terminal, from the repository root, using Java 21:

```powershell
mvn -B compile dependency:build-classpath "-Dmdep.outputFile=target/server-classpath.txt"
$serverClasspath = "target/classes;" + (Get-Content -Raw target/server-classpath.txt).Trim()
java -cp $serverClasspath com.netsync.app.NetSyncServerApplication --insecure-demo --users alice,bob --port 5050 --udp-port 5051
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

- Real `LOGIN_REQUEST` / `LOGIN_RESPONSE`, `LOGOUT_REQUEST` / `ACK`, presence and `CHAT_SEND` handlers.
- One security-owned session registry shared by authentication, authorization and
  the transport's live connection lookup.
- One bounded two-thread worker pool with 32 queued tasks shared by authentication
  and transfer disk/hash work.
- Disconnect cancels pending authentication and revokes the session/token.
- Failed login returns generic `AUTH_FAILED`, then closes after output drains.
- Logout returns `ACK/ACCEPTED`, revokes the session, then closes after draining.
- Existing 10-second login deadline and 1,024-entry JSON duplicate-ID guard remain
  active. A duplicate returns `DUPLICATE_MESSAGE` before handler dispatch.
- Authenticated chat derives sender identity from the authoritative TCP session,
  routes by recipient client ID through the shared session registry, and uses the
  same nonblocking outbound sink. A queued ACK confirms only recipient output-queue
  acceptance, not receipt or reading. UDP presence expiry does not revoke TCP chat
  authorization.
- File controls and binary chunks use the same authenticated NIO connection and
  session registry. The application owns transfer state, `data/uploads/server/`
  relay storage, worker operations, disconnect cleanup, and a one-second timeout
  scheduler for the documented 30-second transfer lifetime.

`NetSyncClient` supplies production `/login`, `/logout`, `/presence`, `/chat`,
`/sendfile`, `/accept`, `/reject`, and `/cancel` commands. Client files are stored
under `data/uploads/clients/<authenticated-clientId>/` with generated transfer-ID
names. The UDP port defaults to 5051 and
can be selected with `--udp-port`; both listeners bind only to loopback. Clients
send heartbeats every five seconds and presence expires after fifteen seconds.
See `docs/chat-demo.md` and `docs/file-transfer-demo.md` for demonstrations. This
is not a TLS or remote deployment.

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
