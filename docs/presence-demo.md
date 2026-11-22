# Member 3: UDP presence demo

This loopback-only automated demo exercises the real `UdpPresenceService` with actual UDP datagrams. It uses a test-only authenticated session and fixed test token; it does not start the application server, perform real authentication, or change production security behavior.

The test demonstrates:

1. The registered session appears as `ONLINE` in a `PRESENCE_SNAPSHOT`.
2. Advancing the injected monotonic clock by 15 seconds, with no heartbeat, causes the real service's expiry scheduler to publish `OFFLINE`.
3. A valid heartbeat sent over UDP to the running service restores `ONLINE`, and a later snapshot reflects the recovery.

## Run from PowerShell

```powershell
Set-Location C:\Users\ASUS\Desktop\NetSync

# Run the local UDP demonstration and print its state transitions.
mvn "-Dtest=UdpPresenceDemoTest" "-Dsurefire.useFile=false" test

# Run all focused presence tests.
mvn "-Dtest=HeartbeatTrackerTest,UdpPresenceServiceTest,PresenceHandlerTest,UdpPresenceDemoTest" test

# Run the complete project test suite.
mvn clean test
```

The demonstration binds UDP to an ephemeral port on `127.0.0.1`, so it does not require the default application port or another user. Its successful output includes transitions and snapshots similar to:

```text
[Presence demo] transition demo_user=ONLINE
[Presence demo] PRESENCE_SNAPSHOT demo_user=ONLINE
[Presence demo] transition demo_user=OFFLINE
[Presence demo] PRESENCE_SNAPSHOT demo_user=OFFLINE
[Presence demo] transition demo_user=ONLINE
[Presence demo] PRESENCE_SNAPSHOT demo_user=ONLINE
```

The test clock advances by the protocol's 15-second timeout instead of waiting 15 wall-clock seconds. The UDP packet itself is sent through a real `DatagramSocket` to the real `UdpPresenceService`.

This is a module demonstration, not the two-client application demo: production startup, login/logout callbacks, client heartbeat scheduling, and sending `PRESENCE_UPDATE` events to TCP clients still require application integration.
