package com.netsync.gui;

import com.netsync.common.PresenceState;
import com.netsync.common.ProtocolLimits;
import com.netsync.common.SessionView;
import com.netsync.transfer.TransferChecksum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full 3-client (Alice, Bob, Carol) end-to-end production integration test exercising the real
 * NetSync TCP/UDP server and GUI-facing {@link ServerDashboardController} + {@link ClientSessionController}
 * without mocks or graphical display requirements.
 */
class GuiClientProductionIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void fullThreeClientGuiBackendWorkflowEndToEnd() throws Exception {
        Path clientsRoot = tempDir.resolve("clients");
        Path sourceDir = tempDir.resolve("source files"); // Intentionally includes a space in path
        Files.createDirectories(sourceDir);

        byte[] payload = new byte[40_000]; // Spans multiple 16 KiB chunks
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) ((i * 31 + 7) & 0xFF);
        }
        Path sampleFile = sourceDir.resolve("demo report.bin");
        Files.write(sampleFile, payload);
        String expectedSha256 = TransferChecksum.computeFileDigestHex(sampleFile);

        try (ServerDashboardController server = new ServerDashboardController(Runnable::run);
             ClientSessionController alice = new ClientSessionController(Runnable::run, clientsRoot);
             ClientSessionController bob = new ClientSessionController(Runnable::run, clientsRoot);
             ClientSessionController carol = new ClientSessionController(Runnable::run, clientsRoot)) {

            // 1. Programmatic server startup
            ServerModels.ServerSnapshot started = server.startServer(true, 0, 0, List.of(
                    new ServerDashboardController.AccountInput("alice", "alice-demo-pw".toCharArray()),
                    new ServerDashboardController.AccountInput("bob", "bob-demo-pw".toCharArray()),
                    new ServerDashboardController.AccountInput("carol", "carol-demo-pw".toCharArray())
            )).get(15, TimeUnit.SECONDS);
            int tcpPort = started.tcpPort();
            int udpPort = started.udpPort();
            assertEquals(ServerModels.ServerState.RUNNING, started.state());

            // Negative auth: invalid credentials must fail and clear caller buffer
            char[] wrongPw = "wrong-password".toCharArray();
            var badLogin = alice.connectAndLogin(ProtocolLimits.BIND_ADDRESS, tcpPort, udpPort, "alice", wrongPw);
            assertThrows(ExecutionException.class, () -> badLogin.get(10, TimeUnit.SECONDS));
            for (char c : wrongPw) {
                assertEquals('\0', c);
            }
            assertEquals(ClientModels.ConnectionPhase.DISCONNECTED, alice.phase());

            // 2. Authenticate all three clients (wait for prior failed login's transport threads to stop)
            SessionView aliceSession = connectWhenReady(
                    alice, ProtocolLimits.BIND_ADDRESS, tcpPort, udpPort, "alice", "alice-demo-pw");
            SessionView bobSession = bob.connectAndLogin(
                    ProtocolLimits.BIND_ADDRESS, tcpPort, udpPort, "bob", "bob-demo-pw".toCharArray())
                    .get(15, TimeUnit.SECONDS);
            SessionView carolSession = carol.connectAndLogin(
                    ProtocolLimits.BIND_ADDRESS, tcpPort, udpPort, "carol", "carol-demo-pw".toCharArray())
                    .get(15, TimeUnit.SECONDS);

            // Duplicate login on same account while active must fail
            try (ClientSessionController dupAlice = new ClientSessionController(Runnable::run, clientsRoot)) {
                var dupAttempt = dupAlice.connectAndLogin(
                        ProtocolLimits.BIND_ADDRESS, tcpPort, udpPort, "alice", "alice-demo-pw".toCharArray());
                assertThrows(ExecutionException.class, () -> dupAttempt.get(10, TimeUnit.SECONDS));
            }

            // 3 & 4. Actual UDP heartbeat lifecycle + presence snapshot & updates
            awaitCondition(() -> alice.isSendingHeartbeats() && bob.isSendingHeartbeats() && carol.isSendingHeartbeats(), 10_000);
            alice.refreshUsers();
            awaitCondition(() -> hasOnlineUser(alice.usersSnapshot(), bobSession.clientId())
                    && hasOnlineUser(alice.usersSnapshot(), carolSession.clientId()), 10_000);

            ServerModels.ServerSnapshot monitored = server.snapshot();
            assertEquals(3, monitored.sessions().size());

            // 5, 6, 7. Private chat Alice -> Bob, Bob reply, Carol isolation, QUEUED semantics
            UUID req1 = alice.sendChatMessage(bobSession.clientId(), "Hello Bob from GUI!");
            awaitCondition(() -> alice.conversationSnapshot(bobSession.clientId()).stream()
                    .anyMatch(m -> req1.equals(m.requestId()) && m.status() == ClientModels.DeliveryStatus.QUEUED), 10_000);
            awaitCondition(() -> bob.conversationSnapshot(aliceSession.clientId()).stream()
                    .anyMatch(m -> !m.outgoing() && "Hello Bob from GUI!".equals(m.text())), 10_000);

            UUID req2 = bob.sendChatMessage(aliceSession.clientId(), "Hi Alice, received!");
            awaitCondition(() -> bob.conversationSnapshot(aliceSession.clientId()).stream()
                    .anyMatch(m -> req2.equals(m.requestId()) && m.status() == ClientModels.DeliveryStatus.QUEUED), 10_000);
            awaitCondition(() -> alice.conversationSnapshot(bobSession.clientId()).stream()
                    .anyMatch(m -> !m.outgoing() && "Hi Alice, received!".equals(m.text())), 10_000);

            // Carol must not see Alice/Bob private messages
            assertTrue(carol.conversationSnapshot(aliceSession.clientId()).isEmpty());
            assertTrue(carol.conversationSnapshot(bobSession.clientId()).isEmpty());

            // 8 & 9. File offer from Alice to Bob -> Recipient rejection
            alice.sendFile(bobSession.clientId(), sampleFile);
            awaitCondition(() -> bob.transfersSnapshot().stream()
                    .anyMatch(t -> t.direction() == ClientModels.TransferDirection.INCOMING
                            && t.status() == ClientModels.TransferStatus.OFFERED), 10_000);
            UUID rejectedId = bob.transfersSnapshot().stream()
                    .filter(t -> t.status() == ClientModels.TransferStatus.OFFERED)
                    .findFirst().orElseThrow().transferId();
            bob.rejectFileOffer(rejectedId);

            awaitCondition(() -> alice.transfersSnapshot().stream()
                    .anyMatch(t -> rejectedId.equals(t.transferId())
                            && t.status() == ClientModels.TransferStatus.REJECTED), 10_000);

            // 10, 11, 12, 13. New accepted transfer, real progress, exact bytes, SHA-256 verification
            alice.sendFile(bobSession.clientId(), sampleFile);
            awaitCondition(() -> bob.transfersSnapshot().stream()
                    .anyMatch(t -> t.direction() == ClientModels.TransferDirection.INCOMING
                            && t.status() == ClientModels.TransferStatus.OFFERED
                            && !rejectedId.equals(t.transferId())), 10_000);
            UUID acceptedId = bob.transfersSnapshot().stream()
                    .filter(t -> t.status() == ClientModels.TransferStatus.OFFERED && !rejectedId.equals(t.transferId()))
                    .findFirst().orElseThrow().transferId();
            bob.acceptFileOffer(acceptedId);

            awaitCondition(() -> bob.transfersSnapshot().stream()
                    .anyMatch(t -> acceptedId.equals(t.transferId())
                            && t.status() == ClientModels.TransferStatus.COMPLETED), 15_000);
            awaitCondition(() -> alice.transfersSnapshot().stream()
                    .anyMatch(t -> acceptedId.equals(t.transferId())
                            && t.status() == ClientModels.TransferStatus.COMPLETED), 15_000);

            ClientModels.TransferItem bobCompleted = bob.transfersSnapshot().stream()
                    .filter(t -> acceptedId.equals(t.transferId())).findFirst().orElseThrow();
            assertNotNull(bobCompleted.localFile());
            assertTrue(Files.exists(bobCompleted.localFile()));
            assertArrayEquals(payload, Files.readAllBytes(bobCompleted.localFile()));
            assertTrue(bobCompleted.sha256Verified());
            assertEquals(expectedSha256, bobCompleted.sha256());
            assertTrue(TransferPresentation.completedFilePathLabel(bobCompleted).orElseThrow().startsWith("Saved to: "));
            assertTrue(TransferPresentation.canSaveAs(bobCompleted));

            // Sender-side completion must show "Source file:" and must not enable Save As.
            ClientModels.TransferItem aliceCompleted = alice.transfersSnapshot().stream()
                    .filter(t -> acceptedId.equals(t.transferId())).findFirst().orElseThrow();
            assertTrue(TransferPresentation.completedFilePathLabel(aliceCompleted).orElseThrow().startsWith("Source file: "));
            assertFalse(TransferPresentation.canSaveAs(aliceCompleted));
            assertTrue(aliceCompleted.statusDetail().contains("Recipient confirmed"));

            // Receiver Save As export with original filename, SHA-256 verification, and non-overwrite guard.
            Path exportDir = tempDir.resolve("bob exports");
            Files.createDirectories(exportDir);
            Path exportedCopy = exportDir.resolve(SafeFileExportService.safeSuggestedFileName(
                    bobCompleted.fileName(), bobCompleted.transferId()));
            SafeFileExportService.ExportResult exportResult =
                    bob.exportReceivedFile(acceptedId, exportedCopy).get(10, TimeUnit.SECONDS);
            assertEquals(expectedSha256, exportResult.sha256());
            assertArrayEquals(payload, Files.readAllBytes(exportedCopy));
            assertTrue(Files.exists(bobCompleted.localFile()), "Original .bin file must remain preserved");
            assertThrows(ExecutionException.class,
                    () -> bob.exportReceivedFile(acceptedId, exportedCopy).get(10, TimeUnit.SECONDS),
                    "Re-exporting to existing file must refuse overwrite");

            // 14. Transfer cancellation
            Path cancelFile = sourceDir.resolve("cancel me.txt");
            Files.writeString(cancelFile, "cancel test content", StandardCharsets.UTF_8);
            alice.sendFile(bobSession.clientId(), cancelFile);
            awaitCondition(() -> bob.transfersSnapshot().stream()
                    .anyMatch(t -> t.status() == ClientModels.TransferStatus.OFFERED
                            && "cancel me.txt".equals(t.fileName())), 10_000);
            UUID cancelId = bob.transfersSnapshot().stream()
                    .filter(t -> t.status() == ClientModels.TransferStatus.OFFERED && "cancel me.txt".equals(t.fileName()))
                    .findFirst().orElseThrow().transferId();
            alice.cancelFileTransfer(cancelId);
            awaitCondition(() -> alice.transfersSnapshot().stream()
                    .anyMatch(t -> cancelId.equals(t.transferId())
                            && t.status() == ClientModels.TransferStatus.CANCELLED), 10_000);

            // 15 & 16. Logout (clears session history) + reconnect with new client ID
            alice.logout().get(10, TimeUnit.SECONDS);
            awaitCondition(() -> alice.phase() == ClientModels.ConnectionPhase.DISCONNECTED, 10_000);
            assertTrue(alice.conversationSnapshot(bobSession.clientId()).isEmpty(),
                    "Private conversation history must be cleared on logout");
            assertTrue(alice.transfersSnapshot().isEmpty(),
                    "Transfer history must be cleared on logout");

            SessionView aliceSession2 = connectWhenReady(
                    alice, ProtocolLimits.BIND_ADDRESS, tcpPort, udpPort, "alice", "alice-demo-pw");
            assertNotEquals(aliceSession.clientId(), aliceSession2.clientId(),
                    "Reconnected session must receive a new client ID");

            // 17. Fresh chat after reconnect
            UUID req3 = alice.sendChatMessage(bobSession.clientId(), "Reconnected cleanly!");
            awaitCondition(() -> alice.conversationSnapshot(bobSession.clientId()).stream()
                    .anyMatch(m -> req3.equals(m.requestId()) && m.status() == ClientModels.DeliveryStatus.QUEUED), 10_000);
            awaitCondition(() -> bob.conversationSnapshot(aliceSession2.clientId()).stream()
                    .anyMatch(m -> !m.outgoing() && "Reconnected cleanly!".equals(m.text())), 10_000);

            // 18 & 19. Clean client & server shutdown
            alice.close();
            bob.close();
            carol.close();

            ServerModels.ServerSnapshot stopped = server.stopServer().get(15, TimeUnit.SECONDS);
            assertEquals(ServerModels.ServerState.STOPPED, stopped.state());

            // 20. Restart on the exact same TCP and UDP ports
            ServerModels.ServerSnapshot restarted = server.startServer(true, tcpPort, udpPort, List.of(
                    new ServerDashboardController.AccountInput("alice", "alice-demo-pw".toCharArray())
            )).get(15, TimeUnit.SECONDS);
            assertEquals(ServerModels.ServerState.RUNNING, restarted.state());
            assertEquals(tcpPort, restarted.tcpPort());
            assertEquals(udpPort, restarted.udpPort());
        }
    }

    @Test
    void incomingOfferCountdownUsesMonotonicClockWithoutResettingOnRefreshAndClearsOnResolution() throws Exception {
        Path clientsRoot = tempDir.resolve("countdown-clients");
        Path sourceFile = tempDir.resolve("countdown-sample.png");
        Files.writeString(sourceFile, "png-bytes", StandardCharsets.UTF_8);

        java.util.concurrent.atomic.AtomicLong bobClock = new java.util.concurrent.atomic.AtomicLong(50_000_000_000L);
        try (ServerDashboardController server = new ServerDashboardController(Runnable::run);
             ClientSessionController alice = new ClientSessionController(Runnable::run, clientsRoot);
             ClientSessionController bob = new ClientSessionController(Runnable::run, clientsRoot,
                     new com.netsync.client.TcpClient(), bobClock::get)) {

            ServerModels.ServerSnapshot started = server.startServer(true, 0, 0, List.of(
                    new ServerDashboardController.AccountInput("alice", "pw-alice".toCharArray()),
                    new ServerDashboardController.AccountInput("bob", "pw-bob".toCharArray())
            )).get(15, TimeUnit.SECONDS);

            alice.connectAndLogin(ProtocolLimits.BIND_ADDRESS, started.tcpPort(), started.udpPort(),
                    "alice", "pw-alice".toCharArray()).get(15, TimeUnit.SECONDS);
            SessionView bobSession = bob.connectAndLogin(ProtocolLimits.BIND_ADDRESS, started.tcpPort(), started.udpPort(),
                    "bob", "pw-bob".toCharArray()).get(15, TimeUnit.SECONDS);

            alice.sendFile(bobSession.clientId(), sourceFile);
            awaitCondition(() -> bob.transfersSnapshot().stream()
                    .anyMatch(t -> t.status() == ClientModels.TransferStatus.OFFERED), 10_000);

            ClientModels.TransferItem offer = bob.transfersSnapshot().stream()
                    .filter(t -> t.status() == ClientModels.TransferStatus.OFFERED).findFirst().orElseThrow();
            assertEquals(java.util.Optional.of(30L),
                    TransferPresentation.estimatedRemainingOfferSeconds(offer, bob.nanoTime()));

            // Advance monotonic clock by 6 seconds and trigger presence refresh: countdown must NOT reset.
            bobClock.addAndGet(TimeUnit.SECONDS.toNanos(6));
            bob.refreshUsers();
            ClientModels.TransferItem sameOffer = bob.transfersSnapshot().stream()
                    .filter(t -> offer.transferId().equals(t.transferId())).findFirst().orElseThrow();
            assertEquals(java.util.Optional.of(24L),
                    TransferPresentation.estimatedRemainingOfferSeconds(sameOffer, bob.nanoTime()));
            assertEquals(java.util.Optional.of("Respond within approximately 24 seconds"),
                    TransferPresentation.countdownLabel(sameOffer, bob.nanoTime()));

            // Rejecting clears OFFERED state and removes countdown.
            bob.rejectFileOffer(offer.transferId());
            awaitCondition(() -> bob.transfersSnapshot().stream()
                    .anyMatch(t -> offer.transferId().equals(t.transferId())
                            && t.status() == ClientModels.TransferStatus.REJECTED), 10_000);
            ClientModels.TransferItem rejected = bob.transfersSnapshot().stream()
                    .filter(t -> offer.transferId().equals(t.transferId())).findFirst().orElseThrow();
            assertTrue(TransferPresentation.countdownLabel(rejected, bob.nanoTime()).isEmpty());
        }
    }

    private static SessionView connectWhenReady(
            ClientSessionController client, String host, int tcpPort, int udpPort, String user, String pass)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true) {
            try {
                return client.connectAndLogin(host, tcpPort, udpPort, user, pass.toCharArray())
                        .get(15, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                boolean stillStopping = e.getCause() != null
                        && e.getCause().getMessage() != null
                        && e.getCause().getMessage().contains("Previous connection is still stopping");
                if (!stillStopping || System.nanoTime() >= deadline) {
                    throw e;
                }
                Thread.sleep(25);
            }
        }
    }

    private static boolean hasOnlineUser(List<ClientModels.UserItem> users, UUID clientId) {
        return users.stream().anyMatch(u -> clientId.equals(u.clientId()) && u.state() == PresenceState.ONLINE);
    }

    private static void awaitCondition(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        assertTrue(condition.getAsBoolean(), "Condition did not become true within " + timeoutMs + " ms");
    }
}
