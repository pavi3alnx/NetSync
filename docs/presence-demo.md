# Production presence demo

This is an explicitly insecure, loopback-only demonstration. Use disposable account passwords. The TCP server and UDP presence service bind to `127.0.0.1`; the default ports are TCP `5050` and UDP `5051`.

## Start the server

From the repository root in PowerShell:

```powershell
mvn -B compile dependency:build-classpath "-Dmdep.outputFile=target/server-classpath.txt"
$serverClasspath = "target/classes;" + (Get-Content -Raw target/server-classpath.txt).Trim()
java -cp $serverClasspath com.netsync.app.NetSyncServerApplication --insecure-demo --users alice,bob --port 5050 --udp-port 5051
```

The options match `DemoServerConfig.parse`: `--insecure-demo` is required, `--users` supplies comma-separated usernames, `--port` selects TCP, and `--udp-port` selects UDP. The application prompts for each password using hidden console input. Keep this server terminal open.

## Start two clients

In each of two separate PowerShell terminals, from the repository root, run:

```powershell
mvn -B compile dependency:build-classpath "-Dmdep.outputFile=target/server-classpath.txt"
$clientClasspath = "target/classes;" + (Get-Content -Raw target/server-classpath.txt).Trim()
java -cp $clientClasspath com.netsync.client.NetSyncClient
```

Each client uses the production `NetSyncClient` entry point and hidden password input.

In both client terminals:

1. Run `/connect 127.0.0.1 5050 5051`.
2. Complete `/login alice` in one client and `/login bob` in the other, entering the disposable passwords at the hidden prompt.
3. Log in within **10 seconds of connecting**. Unauthenticated TCP connections are closed at the login deadline.
4. Observe `Presence` ONLINE updates as each account logs in. Run `/presence` in either client to view the correlated snapshot.
5. Run `/logout` in one client and observe the OFFLINE update in the other. Run `/disconnect` in the remaining client, then `/quit` to close it.
6. Stop the server with Ctrl+C.

Authenticated clients send a heartbeat every five seconds. Therefore an idle client with its automatic sender running remains ONLINE. To verify heartbeat expiry and recovery without waiting on manual terminal behavior, run the deterministic production-composition integration tests:

```powershell
mvn "-Dtest=PresenceProductionIntegrationTest" test
```

Those tests use the real application composition and UDP datagrams with an injected monotonic clock. A silent heartbeat stream becomes OFFLINE after the 15-second timeout while TCP authentication remains valid; a subsequent valid heartbeat restores ONLINE. `UdpPresenceDemoTest` separately covers the UDP service in isolation.

The transport has no confidentiality and is unsuitable for untrusted networks or real credentials.
