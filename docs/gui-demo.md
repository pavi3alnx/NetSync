# NetSync — JavaFX Desktop GUI Demonstration Guide

This guide covers building, launching, and demonstrating the NetSync JavaFX desktop frontend connected to the real production NetSync TCP/UDP backend.

---

## 1. Prerequisites

- **Java**: JDK 21 (`java -version` shows 21.x).
- **Maven**: Apache Maven 3.9+ (`mvn -version`) for the one-time build.
- **JavaFX**: Managed automatically by Maven (`org.openjfx:javafx-controls:21.0.5` with platform-specific native libraries copied into `target/lib/`).
- **Display**: A graphical desktop session is required to display JavaFX windows. Automated Maven tests run headlessly without a display.

---

## 2. Clone, Update, and One-Time Preparation

Team members can obtain or update the repository and prepare the GUI runtime once:

```bash
git clone https://github.com/pavi3alnx/NetSync.git
cd NetSync
```

Run the one-time build before the demonstration (either by double-clicking or from a terminal at the repository root):

- **Windows (double-click)**: `Build-NetSync.bat`
- **Command line (Windows / Linux / macOS)**:
  ```bash
  mvn -B clean package -DskipTests
  ```
  *(To also run the complete automated test suite: `mvn -B clean verify`.)*

This builds `target/netsync-1.0.0-SNAPSHOT.jar` and populates `target/lib/` with Jackson and JavaFX runtime JARs. The standard JAR is **not** a self-contained fat JAR; both `target/netsync-1.0.0-SNAPSHOT.jar` and `target/lib/*` are required at runtime.

---

## 3. Launching the GUI

### Windows Double-Click Launchers (Recommended for Demo)

1. Double-click **`Start-NetSync-Server.bat`** to open the Server Administration Dashboard.
2. Double-click **`Start-NetSync-Client.bat`** once per user (e.g., three times for Alice, Bob, and Carol). Multiple independent client windows can be opened simultaneously without triggering Maven builds.

### Direct Command-Line Launch (Portable)

- **Windows (PowerShell / CMD)**:
  ```powershell
  java -cp "target\netsync-1.0.0-SNAPSHOT.jar;target\lib\*" com.netsync.gui.NetSyncServerGuiLauncher
  java -cp "target\netsync-1.0.0-SNAPSHOT.jar;target\lib\*" com.netsync.gui.NetSyncClientGuiLauncher
  ```
- **Linux / macOS**:
  ```bash
  java -cp "target/netsync-1.0.0-SNAPSHOT.jar:target/lib/*" com.netsync.gui.NetSyncServerGuiLauncher
  java -cp "target/netsync-1.0.0-SNAPSHOT.jar:target/lib/*" com.netsync.gui.NetSyncClientGuiLauncher
  ```

The existing CLI entry points (`com.netsync.app.NetSyncServerApplication` and `com.netsync.client.NetSyncClient`) remain fully functional.

---

## 4. Demonstration Walkthrough

### Server Configuration & Lifecycle
1. Open the **NetSync Server** dashboard.
2. Confirm the fixed loopback bind address (`127.0.0.1`), TCP port (`5050`), and UDP presence port (`5051`).
3. Check **Acknowledge local-only unencrypted demo (`--insecure-demo`)**.
4. Enter disposable passwords for `alice`, `bob`, and `carol` (or add/remove account rows). Passwords are never saved to disk.
5. Click **Start Server**. Verify the status badge changes to **RUNNING**, TCP/UDP endpoints show `127.0.0.1:5050` and `127.0.0.1:5051`, and uptime begins ticking.
6. Use **Stop Server** and **Restart** to demonstrate clean socket release and port reuse.

### Client Login & Authentication
1. Open a **NetSync Client** window.
2. Keep defaults (`127.0.0.1`, TCP `5050`, UDP `5051`).
3. Demonstrate invalid credentials first (verify error banner and cleared password field).
4. Enter valid disposable credentials (e.g., `alice`) and click **Connect & Sign In**.
5. Open additional client windows for `bob` and `carol` and sign them in.

### Live Presence (UDP Heartbeats + TCP Snapshots)
1. Observe online peers appearing in the left sidebar with green `ONLINE` badges.
2. Click **Refresh** to issue a live TCP `PRESENCE_QUERY`.
3. Disconnect or log out one client and observe its presence state update, then sign back in and observe a fresh session generation (toggle **Technical IDs** in the sidebar to inspect new client UUIDs if needed).

### Private Messaging (TCP Chat)
1. In Alice's window, select **bob** in the sidebar.
2. Type a message and press **Enter** or click **Send**.
3. Verify the outgoing bubble transitions from `Sending...` to **`Queued`** (indicating server outbound-queue acceptance via `ACK(QUEUED)`, not guaranteed recipient read receipt).
4. Verify Bob receives the message immediately and replies, while Carol does **not** receive Alice and Bob's private messages.

### Consent-Based File Transfer (Stop-and-Wait + SHA-256 + Save As)
1. Select **bob** in Alice's window and click **Send File...** (or **Attach**).
2. Choose a regular file (up to 10 MiB; paths with spaces are supported).
3. In Bob's window, observe the incoming file offer card with sender name, filename, size, and an advisory countdown (`Respond within approximately N seconds`) aligned with the server's 30-second transfer timeout (`ProtocolLimits.TRANSFER_TIMEOUT_SECONDS = 30`). Switching tabs or conversations does not extend the offer lifetime, and the server remains authoritative for timeout decisions.
4. **Timeout & retry flow (optional)**: Waiting past 30 seconds without responding allows the server to time out the offer cleanly as `FAILED`, after which a fresh offer can be sent and accepted.
5. **Reject flow**: Click **Reject** on Bob's card and confirm Alice sees `REJECTED`.
6. **Accept, Verification & Labels flow**: Send another file from Alice, click **Accept** on Bob's card, and observe real progress bar updates. Upon completion:
   - **Sender card** displays `Source file: <local source path>`, a message confirming recipient-reported verification, and **Open Source Folder**.
   - **Receiver card** displays `Saved to: data/uploads/clients/<clientId>/<transferId>.bin`, **SHA-256 Verified: `<digest>`**, **Save As...**, and **Open Folder**.
7. **Safe `Save As...` export (receiver)**: Click **Save As...** on a completed, verified incoming transfer to export a verified copy using the validated original filename (e.g., `Screenshot (1).png`). Existing destination files are never overwritten, unsafe filenames/symlinks are rejected, the original `.bin` file is preserved, and received files are never auto-executed.
8. **Independent SHA-256 verification (Windows PowerShell)**:
   ```powershell
   (Get-FileHash "<path-to-source-file>" -Algorithm SHA256).Hash -eq (Get-FileHash "<path-to-received-or-exported-file>" -Algorithm SHA256).Hash
   ```
9. **Cancel flow**: Start a transfer and click **Cancel Transfer** to verify cancellation and `.part` cleanup.

### Logout, Reconnect, and Shutdown
1. Click **Log Out** in a client window to perform correlated protocol logout and return to the sign-in screen (previous conversation and transfer history are cleared for session isolation).
2. Close client windows and click **Stop Server** (or close the server window) for clean shutdown.

---

## 5. Manual Acceptance Checklist

- [ ] `Build-NetSync.bat` completes without errors.
- [ ] `Start-NetSync-Server.bat` opens the server dashboard cleanly and displays full `Remove` button labels (no `Rem...` truncation) at minimum, normal, and maximized sizes.
- [ ] Starting without `--insecure-demo` checked or with blank/duplicate accounts shows a clear validation error.
- [ ] Server starts on `127.0.0.1` TCP `5050` / UDP `5051`, monitors active sessions/transfers, stops cleanly, and restarts on the same ports.
- [ ] Multiple simultaneous `Start-NetSync-Client.bat` windows launch and authenticate cleanly.
- [ ] Invalid login displays a generic error and clears the password field.
- [ ] Presence list shows `ONLINE`/`OFFLINE` updates and `Refresh` works.
- [ ] Private chat displays `Queued` status and isolates third-party conversations.
- [ ] Incoming file offer displays the advisory 30-second countdown across both Messages and Transfers views without extending on tab switch.
- [ ] File transfer supports Reject, Timeout & Retry, Accept with real progress and SHA-256 verification, and Cancel.
- [ ] Sender completed card shows `Source file:` + `Open Source Folder`; receiver completed card shows `Saved to:` + `Save As...` + `Open Folder`.
- [ ] Receiver `Save As...` exports a verified copy under the original filename without overwriting existing files or removing the stored `.bin` file.
- [ ] Logout clears session history and reconnect succeeds with a new client ID.
- [ ] Resizing windows across minimum, `1366x768`, and maximized sizes causes no clipped or overlapping controls.

---

## 6. Troubleshooting

- **Missing JAR or JavaFX libraries**: Run `Build-NetSync.bat` (or `mvn -B clean package -DskipTests`) once before double-clicking the launchers.
- **Locked `target/netsync-1.0.0-SNAPSHOT.jar` during `mvn clean`**: Close all running NetSync Server and Client GUI windows before rebuilding with Maven on Windows.
- **Port already in use (`5050` / `5051`)**: Ensure no other NetSync server instance (GUI or CLI) is bound to TCP `5050` or UDP `5051`, or choose different ports on both server and client.
- **Login timed out**: The server enforces a 10-second login deadline after TCP connect; the GUI collects credentials before connecting so this normally only occurs if the server is overloaded or unresponsive.
- **Save As notice**: Save As notifications include a **Dismiss** button, auto-clear after a short timeout, and clear automatically when switching views or starting new operations.

---

## 7. Known Security and Operational Limitations

- **Loopback-only unencrypted demo**: Binds exclusively to `127.0.0.1` with no TLS/DTLS encryption and no LAN/remote host support. Unsuitable for untrusted networks or production deployment.
- **JavaFX `PasswordField` memory limitation**: While backend `char[]` password buffers are zeroed in `finally` blocks, JavaFX `PasswordField` stores immutable `String` values internally that cannot be cryptographically erased from JVM heap memory.
- **Presence is advisory**: UDP heartbeat expiry marks a user `OFFLINE` after 15 seconds without revoking an otherwise valid TCP session.
- **Safe generated filenames**: Received files are saved as `<transferId>.bin` under `data/uploads/clients/<clientId>/` to prevent path-traversal attacks. Use **Save As...** to export a verified copy under the validated original filename without overwriting existing files.
