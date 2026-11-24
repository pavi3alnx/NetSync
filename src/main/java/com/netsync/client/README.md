# NetSync client integration

The client contains transport, dispatch, session access, an extensible CLI and
production authentication, presence and private-chat adapters. It reuses the
existing wire models and codecs. File transfer is not composed into this CLI.

## Start the CLI (PowerShell, from the repository root)

```powershell
mvn -B compile dependency:build-classpath "-Dmdep.outputFile=target/client-classpath.txt"
$clientClasspath = "target/classes;" + (Get-Content -Raw target/client-classpath.txt).Trim()
java -cp $clientClasspath com.netsync.client.NetSyncClient
```

Example commands:

```text
/help
/connect
/status
/disconnect
/connect 127.0.0.1 5050 5051
/login alice
/presence
/chat <recipientClientId> Hello there
/logout
/quit
```

Default: `127.0.0.1:5050` TCP and UDP `5051`. The server must be started separately. EOF and `/quit`
close the client. Input/output streams remain caller-owned. CLI input executes on
the caller thread. Generic incoming messages display only type and ID; the chat
adapter renders chat text and sender client IDs. Passwords and presence tokens are
never rendered. Binary frames require an actual file-module receiver.

## Production authentication (Phase 1)

In a real PowerShell terminal at the repository root, start the existing demo server:

```powershell
mvn -B compile dependency:build-classpath "-Dmdep.outputFile=target/client-classpath.txt"
$netsyncClasspath = "target/classes;" + (Get-Content -Raw target/client-classpath.txt).Trim()
java -cp $netsyncClasspath com.netsync.app.NetSyncServerApplication --insecure-demo --users alice,bob --port 5050 --udp-port 5051
```

The names are example disposable identities. Enter fresh disposable passwords at
the server's hidden prompts, never real account passwords. This is an unencrypted,
loopback-only demo, unsuitable for untrusted networks or deployment.

In another PowerShell terminal at the repository root:

```powershell
$netsyncClasspath = "target/classes;" + (Get-Content -Raw target/client-classpath.txt).Trim()
java -cp $netsyncClasspath com.netsync.client.NetSyncClient
```

Enter `/connect 127.0.0.1 5050`, then `/login alice`, and enter the matching password
at the hidden prompt. Never put passwords on the command line. Wait for `Logged in
as ...`. `/logout` asynchronously sends the logout request, processes its correlated
ACK and closes that connection. Use `/connect` and `/login` again for a fresh session.
All existing core commands remain available.

The server's ten-second login deadline starts at TCP acceptance, so finish password
entry promptly after connecting. The adapter also bounds one pending login/logout
to ten seconds, including password entry for login. Neither extends the server
deadline. Timeout closes the original connection because the outcome may be
uncertain; there is no automatic retry. Local input cancellation/validation can be
retried while the connection is alive and its original server deadline has not expired.

The real terminal path uses the same `java.io.Console` for command lines and hidden
passwords. IDE/redirected input without `System.console()` cannot securely log in
and receives a safe error; there is no echoed-password fallback. An already waiting
console prompt cannot be forcibly cancelled by socket closure: finish/cancel it;
the returned buffer is wiped and an expired request is not sent. Password collection
is refused on the client dispatch thread. Response handling never waits for replies.

Password arrays are cleared on success and failure. The existing immutable
`LoginRequest`/String and transient encoded bytes cannot be explicitly erased; the
adapter does not retain them. Input is limited to 1-128 UTF-16 code units to match
the current demo credential store. Passwords, tokens and raw server/provider error
text are never printed by the adapter. Authentication checks the actual connected
peer is loopback before collecting or sending a password.

Every `NetSyncClient` owns one `ClientAuthenticationAdapter`, available through
`cli.authentication()`. A future shell may construct the adapter directly with a
transport, command registry, bounded output consumer and `PasswordInput`. The CLI
constructor overload accepting `PasswordInput` supports trusted embedding/tests;
return a fresh owned `char[]` from a secure input provider. Register one authentication
owner per command registry. Close removes its commands/subscriptions, stops its
single periodic timeout task and disconnects an authentication connection it owns.

Replies are correlated to a UUID and local connection generation. Success supplies
the server-returned identity to `TcpClient.supplySession`. Logout keeps identity
until accepted ACK, loss of authentication, disconnect or timeout; a denied logout
does not discard a live identity. Orderly EOF can precede dispatch of a final
ERROR/ACK: only bounded request metadata is retained for that response; credentials
are cleared immediately by the internal nonblocking transport lifecycle hook.

`presenceCredentials()` returns an optional current snapshot containing the session,
token, heartbeat interval and presence timeout. Its `toString()` redacts the token.
Future asynchronous composition must recheck `isCurrent(snapshot)` before using a
captured snapshot and never log `presenceToken()`. Disconnect/reconnect/close
invalidates access; a copy already retained by a caller cannot be erased remotely.
The generation-specific `TcpClient.disconnect(UUID)` prevents late timeouts or
responses from closing a new connection.

After login, the production CLI starts UDP heartbeats every five seconds. `/presence`
returns client IDs for use with `/chat <recipientClientId> <message>`. Chat text may
contain spaces and is limited to 4,096 UTF-8 bytes. A correlated `ACK/QUEUED` means
the recipient's TCP output queue accepted the message; it is not a receipt or read
confirmation. At most 128 chat requests remain pending, each for up to 30 seconds.
A timeout reports an unknown outcome and never resends. Logout, disconnect,
reconnect and close clear session-bound chat state. Chat shares the bounded output
worker with authentication and presence; saturation uses its visible omission
notice. Presence expiry is advisory and does not revoke a live TCP chat session.
File-transfer integration, TLS and remote deployment are not provided.

```powershell
mvn -B "-Dtest=ClientAuthenticationAdapterTest,ClientAuthenticationIntegrationTest" test
mvn -B clean verify
```

## Transport and outbound API

```java
new TcpClient(); // 256 outgoing frames, 1 MiB outgoing bytes, 256 dispatch tasks
new TcpClient(int outputFrames, int outputBytes, int dispatchCapacity);
void connect() throws IOException;
void connect(String host, int port) throws IOException;
void disconnect();
void close();
boolean isConnected();
TcpClient.State state();
TcpClient.Failure failure();
Optional<UUID> connectionId();
EnqueueResult send(OutboundFrame frame) throws ProtocolException;
EnqueueResult sendJson(MessageEnvelope message) throws ProtocolException;
EnqueueResult sendChunk(FileChunk chunk) throws ProtocolException;
void sendOrThrow(OutboundFrame frame);
long protocolRejections();
ClientDispatcher dispatcher();
```

`ClientOutbound` exposes `send`, `sendJson`, `sendChunk`, `sendOrThrow` and
`isConnected`. ACCEPTED means enqueued, not delivered. Invalid outbound models
throw the shared ProtocolException. Disconnected clients and full output queues
return REJECTED. `sendOrThrow` adapts to existing transfer module Consumers:
rejection throws RejectedExecutionException, invalid models IllegalArgumentException.
There is no automatic retry or replay.

Concurrent sends are serialized in enqueue order. Both output limits include the
frame currently being written. Blocking OutputStream.write completes all requested
bytes or throws: partial kernel writes are handled by the Java socket stream.
The reader separately accumulates prefix, kind and body, validates sizes before
allocation and kind bounds immediately, and decodes through FrameCodec. It handles
fragmented and coalesced frames, discards incomplete EOF tails, and times out a
partial frame after 15 seconds without progress. Idle connections have no read
expiry. Recoverable shared-codec errors skip that frame and increment
`protocolRejections`; fatal malformed input closes with INVALID_FRAME.

Lifecycle is DISCONNECTED -> CONNECTING -> CONNECTED -> DISCONNECTED; CLOSED is
terminal. Connect is synchronous with a five-second socket-connect timeout (host
name resolution uses the platform resolver). Concurrent/duplicate attempts are
rejected. Disconnect/close can cancel a connection attempt by closing its socket.
Reconnect creates a fresh local connection UUID and clears the old session.
Failures are safe enum values, with no raw payload or exception diagnostics.

## Dispatch registration

```java
ClientDispatcher.Registration register(MessageType type, Consumer<MessageEnvelope> handler);
ClientDispatcher.Registration registerBinary(Consumer<FileChunk> handler);
long unhandledMessages();
long handlerFailures();
```

These methods are on `client.dispatcher()`. Registration.close() is idempotent.
Different handlers for a type run in registration order. Registering the identical
Consumer object twice for the same type/binary route is rejected. FILE_CHUNK cannot
be registered on the JSON route. A registration closed before invocation is skipped;
an invocation already starting/running may finish. Dispatcher.close() removes all
registrations and rejects new ones. Handlers may close the client safely.

One serial dispatch worker preserves mixed JSON/binary receive order. Handler
RuntimeExceptions increment `handlerFailures` and do not prevent other handlers
from running. Unhandled JSON increments `unhandledMessages`. Binary frames without
an active receiver close with NO_BINARY_HANDLER. Both counters are cumulative.

The dispatch queue holds at most the configured pending task count plus one running
callback task. It shares capacity across JSON and binary to preserve protocol order.
Slow callbacks never block socket reads; saturation closes with DISPATCH_FULL rather
than dropping an arbitrary frame and continuing a corrupted transfer. The current
callback may finish. Module callbacks should be bounded, cooperate with interruption,
and offload expensive disk work to their own bounded worker infrastructure.

There are at most three client threads: one reader, one writer and one dispatch
worker. Disconnect closes the socket, clears queued output/session state and joins
I/O threads with bounded waits. Reconnect refuses while old I/O threads are alive.
Orderly remote EOF retains already complete queued inbound frames. Explicit local
disconnect, reconnect, fatal errors and overload cancel pending old inbound work.
An already running callback may finish; asynchronous module work must retain and
recheck its connection UUID before sending or publishing results. Disconnect and
close do not promise output draining or delivery. Close interrupts dispatch work,
waits up to two seconds except when called by its own worker, and is idempotent.
All client threads are daemon threads; Java cannot forcibly terminate a module
callback that ignores interruption. Modules own cleanup of their workers/transfers.

## Session access (security-owned authentication)

```java
Optional<SessionView> session();
void supplySession(UUID connectionId, SessionView session);
void clearSession();
```

Use the existing shared immutable SessionView. Security supplies the identity only
after its own successful authentication. Both the supplied UUID and the snapshot's
connectionId must equal the current local connection UUID. This is a local
client-generation ID, not the server's internal connection ID. Session access is
synchronized and empty after disconnect/close. Old generations cannot repopulate it.
The transport itself makes no authentication decisions and stores no password or
presence token. `ClientAuthenticationAdapter` validates/correlates authentication
responses and owns the in-memory presence credentials separately from `SessionView`.

## CLI extension API

```java
NetSyncClient cli = new NetSyncClient(client, outputWriter);
cli.commands().register("/msg", "/msg recipient text", "Send a chat message", args -> {
    // Chat owner implements validation and sending here.
});
cli.run(inputReader);
```

`CommandRegistry.register(String name, String usage, String summary, Command command)`
rejects duplicate names. Command.execute(List<String> arguments) may throw Exception.
`CommandRegistry.parse(String)` supports whitespace, single/double quotes, empty
quoted arguments, and backslash escaping. Unclosed quotes/escapes are rejected.
Command names are case-sensitive slash-prefixed lowercase names. Descriptions are
immutable snapshots. Unknown/malformed commands do not terminate the interactive
loop. Module exceptions are rendered generically so arbitrary error text does not
leak credentials. InputException is an opt-in safe user-facing error; never include
secrets in it. Backslashes escape the following character, so Windows paths need
escaped backslashes (or forward slashes).

## Module wiring examples

```java
// Sachin: independent inbound subscription and outbound controls.
var chatRegistration = client.dispatcher().register(MessageType.CHAT_DELIVER, chatHandler);
ClientOutbound outbound = client;

// Authentication is already owned by the CLI adapter; do not register a second owner.
var authentication = cli.authentication();

// Gayangi: register presence handlers and inspect isConnected/session/connectionId.
var presenceRegistration = client.dispatcher().register(MessageType.PRESENCE_UPDATE, presenceHandler);

// Pajas: existing file services accept a Consumer<OutboundFrame>.
// new FileSender(client::sendOrThrow);
// new FileReceiver(client::sendOrThrow, ...);
var chunkRegistration = client.dispatcher().registerBinary(receiver::handleChunk);
var fileRegistration = client.dispatcher().register(MessageType.FILE_OFFERED, receiver::handleControl);
// Register other required transfer control types in the same way.
```

Register handlers before connecting/sending. Retain and close registration handles
when a module shuts down. The production CLI registers `/login`, `/logout`,
`/presence` and `/chat` once. It warns about unencrypted demo transport. Use
disposable local credentials only; the chat adapter renders safe message text and
sender IDs.
