# Production TCP chat demonstration

This walkthrough uses the actual `NetSyncServerApplication` and `NetSyncClient`
composition. It is an explicitly insecure loopback demo. Use only disposable
credentials. TCP is unencrypted; there is no TLS, remote deployment, or integrated
file transfer. Automated integration tests exercise the real production socket
composition. The interactive steps below are instructions, not a manual run claim.

## Build and start the server

From the repository root in Windows PowerShell with Java 21:

```powershell
mvn -B compile dependency:build-classpath "-Dmdep.outputFile=target/server-classpath.txt"
$serverClasspath = "target/classes;" + (Get-Content -Raw target/server-classpath.txt).Trim()
java -cp $serverClasspath com.netsync.app.NetSyncServerApplication --insecure-demo --users alice,bob,carol --port 5050 --udp-port 5051
```

Enter fresh disposable passwords at the hidden prompts and leave this terminal
running. The application binds TCP to `127.0.0.1:5050` and UDP presence to
`127.0.0.1:5051`. Both ports are configurable; `--udp-port` defaults to 5051.

## Start Alice and Bob clients

In two separate PowerShell terminals at the repository root:

```powershell
mvn -B compile dependency:build-classpath "-Dmdep.outputFile=target/client-classpath.txt"
$clientClasspath = "target/classes;" + (Get-Content -Raw target/client-classpath.txt).Trim()
java -cp $clientClasspath com.netsync.client.NetSyncClient
```

In the first client, run:

```text
/connect 127.0.0.1 5050 5051
/login alice
```

In the second client, run `/connect 127.0.0.1 5050 5051`, then `/login bob`.
Enter each account's disposable password at its hidden prompt within ten seconds
of connecting.

Run `/presence` in Alice's client. The snapshot lists online users and their UUID
client IDs. Copy Bob's ID, then send a private message:

```text
/chat <bob-client-uuid> Hello Bob, this message contains spaces.
```

Bob sees the message and Alice sees a correlated `Queued for recipient ...` notice.
`QUEUED` only confirms that the recipient's TCP output queue accepted the message;
it does not confirm network receipt or reading. A third authenticated client can
be used to confirm that private messages are not broadcast.

For unavailable-recipient behavior, use a valid UUID that is not in the current
presence snapshot:

```text
/chat aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa This recipient is unavailable.
```

The sender receives a safe `RECIPIENT_UNAVAILABLE` response. Logged-in clients
send UDP heartbeats every five seconds. If heartbeat presence expires after fifteen
seconds, TCP authentication remains valid and chat still works; expiry is advisory.
`/logout` revokes the session and closes that TCP connection. Use `/connect` and
`/login` again for a fresh session. Use `/quit` or Ctrl+D to close each client and
Ctrl+C to stop the server.

The project tests verify this production composition with loopback sockets,
including private delivery, correlated acknowledgements/errors, unauthenticated
rejection, presence expiry and reconnect behavior. A human terminal demonstration
has not been performed as part of automated verification.
