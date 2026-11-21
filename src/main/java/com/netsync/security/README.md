# NetSync security module

## Status

Security-owned implementation is present for in-memory sessions, disposable demo credential
verification, presence-token generation and validation, authentication, authorization, and
login/logout message handling. The handlers are unit-tested with bounded-worker and Selector
mailbox test doubles, and the real NIO login/logout flow is covered by TCP integration tests.
`NioServer.HandlerFactory` provides startup access to the server's outbound sink, Selector
task queue, session-registry hook, disconnect-listener registration, and close-after-drain
control. The application still supplies its own account configuration and service lifecycle.

The `com.netsync.chat` package is empty in the inspected worktree. Chat routing and CLI login
commands are not implemented here.

## Authentication architecture

`AuthService` coordinates:

- `CredentialStore` for local account verification.
- `SessionTokenGenerator` for temporary 32-byte cryptographic tokens.
- `SessionRegistryImpl` for atomic connection/client/username indexes and the private
  token-to-session association.

The registry returns only immutable `SessionView` values. It never includes a presence token
in a public session snapshot. Authentication results contain the token only because the login
response must deliver it to the client; their `toString()` redacts the token.

`AuthorizationServiceImpl` resolves the connection identity from the registry and confirms
the connection context reports that same session. Its chat/transfer helper methods require
an active sender and recipient; callers must use the returned sender rather than any client
claimed sender identity.

## Credentials

`CredentialStore` accepts caller-provided disposable account specifications. No demo
passwords are embedded in production source. It stores salted PBKDF2-HMAC-SHA256 verifiers
(120,000 iterations, 256-bit output), compares derived bytes using
`MessageDigest.isEqual`, and performs a dummy derivation for unknown or malformed
credentials. Input char arrays are copied and cleared by the store after initialization;
authentication clears its working password copy after verification.

This store is in-memory and is intended only for the loopback demonstration. It is not a
production account store. Supply disposable test credentials from an approved local
configuration that is not committed.

## Session lifecycle

`SessionRegistryImpl` synchronizes all indexes as one unit. Active usernames and connection
IDs are unique; client IDs are generated server-side. Logout and disconnect remove the
connection, client, username, and token association together. Removal is idempotent.

The protocol does not specify a general absolute session TTL. Sessions therefore do not
expire by default. A positive duration and `Clock` can be injected for environments or tests
that explicitly need expiration. Expiration is applied during registry operations and by
`expireExpiredSessions()`; there is no background expiration scheduler.

UDP heartbeat timeout is separate from authentication: `OFFLINE` presence does not revoke a
healthy TCP session. `AuthService.isPresenceTokenValid(clientId, token)` rechecks active
session ownership and is suitable as the heartbeat tracker token-validator callback.

## Token generation

`SessionTokenGenerator` uses `SecureRandom` to generate exactly 32 bytes and encodes them
with the standard padded Base64 encoder. `isValidToken` applies the protocol's canonical
decode-length-re-encode rule. It validates format only; active ownership and revocation are
checked by `AuthService` against the registry.

## Login and logout handlers

`LoginHandler` expects an injected bounded `Executor`, `SelectorTasks`, `OutboundSink`, and
`ConnectionCloser`.
It reserves Selector completion capacity before submitting PBKDF2 work, rejects queue
saturation as `BUSY`, and only queues the response through the reserved Selector completion.
The handler rejects duplicate/pending logins and prevents a failed connection from retrying
until its disconnect callback clears the per-connection marker. `onDisconnected` must be
registered with the server and called on its Selector thread.

`LogoutHandler` sends the existing `ACK/ACCEPTED` response and revokes the session/token.
It queues the existing `ACK/ACCEPTED` response, revokes the session/token, requests connection
closure after the ACK drains, and accepts a callback for presence cleanup. It does not invent a
wire type or attempt to write directly to a socket.

### Server composition

`NioServer.HandlerFactory` makes the current transport resources available while handlers are
created, and `NioServer.closeAfterDrain(...)` closes only after queued output has been written.
An application composes these security handlers through the existing
`MessageDispatcher.Builder`:

1. Creates one `SessionRegistryImpl`, `AuthService`, `AuthorizationServiceImpl`, bounded
   worker pool, and presence tracker.
2. Registers `LoginHandler` for `LOGIN_REQUEST` and `LogoutHandler` for `LOGOUT_REQUEST`.
3. Supplies the server's outbound sink, Selector completion queue, and `ConnectionCloser`.
4. Wires the login-success callback to `HeartbeatTracker.registerSession(clientId, token)`.
5. Wires logout/disconnect cleanup to `HeartbeatTracker.revokeSession(clientId)` and
   `AuthService.disconnect(connectionId)`.

The TCP integration tests demonstrate this composition, including generic failed-login
response-before-close, logout ACK-before-close, and presence/session cleanup.

A runnable application entry point and credential provisioning remain application-owned.

## Presence, chat, and file transfer

- **Presence:** inject `authService::isPresenceTokenValid` as
  `HeartbeatTracker.TokenValidator`; call `registerSession` after successful login and
  `revokeSession` on logout/disconnect. The tracker continues to own heartbeat ordering and
  presence timeouts.
- **Chat:** derive sender identity from `AuthorizationServiceImpl.authenticatedSession`.
  `authorizedChatSender(context, recipientClientId)` returns an active server-owned sender
  only if the recipient is active. The chat package is absent, so no chat integration is
  claimed.
- **File transfer:** existing `FileTransferService` consumes the shared
  `AuthorizationService` and `SessionRegistry` contracts and checks active initiator and
  recipient sessions. Supply this module's `AuthorizationServiceImpl` and
  `SessionRegistryImpl` when composing the server. No transfer classes were changed.

## Concurrency and security limitations

- PBKDF2 must run on a bounded worker, never the Selector thread.
- Login completion capacity must be reserved before worker submission; accepted credentials
  must not be silently lost on Selector saturation.
- Registry/session operations are synchronized and bounded by the protocol's 100-connection
  demonstration limit when used with `NioServer`.
- Per-connection login failure tracking is bounded by the connection limit and requires the
  disconnect callback to release entries.
- The project is loopback-only and has no TLS/transport confidentiality. Never send real
  credentials or expose the UDP bearer-token protocol to a LAN/public network.
- There is no IP-based login rate limiter because the current connection context exposes no
  peer address. One failed login is rejected for the lifetime of that connection; the server
  must close failed-login connections as specified by the protocol.
- Session expiration is optional and there is no automatic expiration callback into presence.
  The selected default has no absolute session TTL.

## Tests

Run the security tests:

```powershell
mvn -Dtest=SessionRegistryImplTest,CredentialStoreTest,SessionTokenGeneratorTest,AuthServiceTest,AuthorizationServiceImplTest,LoginHandlerTest,LogoutHandlerTest,SecurityIntegrationTest,NioServerSecurityIntegrationTest test
```

Run the complete project test suite:

```powershell
mvn clean test
mvn -B clean verify
```

`SecurityIntegrationTest` exercises the security services with the heartbeat tracker.
`NioServerSecurityIntegrationTest` launches the actual NIO server with the security handlers
and verifies TCP login, failed-login error-before-close, logout ACK-before-close, and
disconnect cleanup. These tests do not provide a production application entry point or claim
production network security.

## Demo checklist

1. Configure disposable accounts locally; do not commit real secrets.
2. Bind both transports only to loopback and display an insecure-demo warning.
3. Start the server only after the shared startup wiring is complete.
4. Reject an invalid login with the generic `AUTH_FAILED` response.
5. Log in with a disposable account and verify the returned client ID/token.
6. Confirm the valid token is accepted by the presence tracker.
7. Verify unauthenticated operations and inactive recipients are rejected by authorization.
8. Log out, verify token revocation, and confirm offline presence cleanup.
9. Run the security tests and the whole Maven test suite.

The TCP integration test demonstrates handler wiring, login/logout, and cleanup. The
application still needs an entry point that provisions disposable credentials and coordinates
TCP, UDP presence, chat, file transfer, and worker-pool lifecycle.
