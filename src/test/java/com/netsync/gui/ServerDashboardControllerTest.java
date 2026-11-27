package com.netsync.gui;

import com.netsync.common.ProtocolLimits;
import org.junit.jupiter.api.Test;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Headless lifecycle and rollback tests for {@link ServerDashboardController}. */
class ServerDashboardControllerTest {

    @Test
    void startsStopsAndRestartsOnSamePortsWithoutConsoleAndClearsPasswords() throws Exception {
        try (ServerDashboardController controller = new ServerDashboardController(Runnable::run)) {
            char[] alicePass = "demo-alice-1".toCharArray();
            char[] bobPass = "demo-bob-2".toCharArray();
            var accounts = List.of(
                    new ServerDashboardController.AccountInput("alice", alicePass),
                    new ServerDashboardController.AccountInput("bob", bobPass));

            ServerModels.ServerSnapshot started = controller.startServer(true, 0, 0, accounts)
                    .get(15, TimeUnit.SECONDS);

            assertEquals(ServerModels.ServerState.RUNNING, started.state());
            assertEquals(ProtocolLimits.BIND_ADDRESS, started.bindAddress());
            assertTrue(started.tcpPort() > 0);
            assertTrue(started.udpPort() > 0);
            assertEquals(2, started.configuredAccountCount());
            assertEquals(0, started.activeTransferCount());

            // Caller password buffers passed to AccountInput are copied/cleared by startServer.
            for (var input : accounts) {
                for (char c : input.password()) {
                    assertEquals('\0', c, "AccountInput password buffer should be zeroed");
                }
            }

            // Activity log must never contain password text.
            for (var entry : controller.activityLogSnapshot()) {
                assertFalse(entry.message().contains("demo-alice-1"));
                assertFalse(entry.message().contains("demo-bob-2"));
            }

            // Overlapping start while running must be rejected.
            var duplicateStart = controller.startServer(true, started.tcpPort(), started.udpPort(),
                    List.of(new ServerDashboardController.AccountInput("alice", "pw".toCharArray())));
            assertThrows(ExecutionException.class, () -> duplicateStart.get(5, TimeUnit.SECONDS));

            int boundTcp = started.tcpPort();
            int boundUdp = started.udpPort();

            ServerModels.ServerSnapshot stopped = controller.stopServer().get(15, TimeUnit.SECONDS);
            assertEquals(ServerModels.ServerState.STOPPED, stopped.state());

            // Restart on the exact same TCP and UDP ports must succeed cleanly.
            ServerModels.ServerSnapshot restarted = controller.startServer(true, boundTcp, boundUdp,
                    List.of(new ServerDashboardController.AccountInput("alice", "new-pass".toCharArray())))
                    .get(15, TimeUnit.SECONDS);
            assertEquals(ServerModels.ServerState.RUNNING, restarted.state());
            assertEquals(boundTcp, restarted.tcpPort());
            assertEquals(boundUdp, restarted.udpPort());
        }
    }

    @Test
    void rejectsInvalidConfigurationsCleanly() {
        try (ServerDashboardController controller = new ServerDashboardController(Runnable::run)) {
            // Missing --insecure-demo acknowledgement
            var noDemo = controller.startServer(false, 0, 0,
                    List.of(new ServerDashboardController.AccountInput("alice", "pass".toCharArray())));
            assertThrows(ExecutionException.class, () -> noDemo.get(10, TimeUnit.SECONDS));
            assertEquals(ServerModels.ServerState.FAILED, controller.state());

            // Empty account list
            var noAccounts = controller.startServer(true, 0, 0, List.of());
            assertThrows(ExecutionException.class, () -> noAccounts.get(10, TimeUnit.SECONDS));
            assertEquals(ServerModels.ServerState.FAILED, controller.state());

            // Duplicate username
            var dupUsers = controller.startServer(true, 0, 0, List.of(
                    new ServerDashboardController.AccountInput("alice", "pass1".toCharArray()),
                    new ServerDashboardController.AccountInput("alice", "pass2".toCharArray())));
            assertThrows(ExecutionException.class, () -> dupUsers.get(10, TimeUnit.SECONDS));
            assertEquals(ServerModels.ServerState.FAILED, controller.state());
        }
    }

    @Test
    void rollsBackCleanlyOnTcpAndUdpBindFailures() throws Exception {
        try (ServerSocket occupiedTcp = new ServerSocket();
             DatagramSocket occupiedUdp = new DatagramSocket(null);
             ServerDashboardController controller = new ServerDashboardController(Runnable::run)) {
            occupiedTcp.bind(new InetSocketAddress(ProtocolLimits.BIND_ADDRESS, 0));
            occupiedUdp.bind(new InetSocketAddress(ProtocolLimits.BIND_ADDRESS, 0));

            var tcpFail = controller.startServer(true, occupiedTcp.getLocalPort(), 0,
                    List.of(new ServerDashboardController.AccountInput("alice", "pass".toCharArray())));
            assertThrows(ExecutionException.class, () -> tcpFail.get(15, TimeUnit.SECONDS));
            assertEquals(ServerModels.ServerState.FAILED, controller.state());

            var udpFail = controller.startServer(true, 0, occupiedUdp.getLocalPort(),
                    List.of(new ServerDashboardController.AccountInput("alice", "pass".toCharArray())));
            assertThrows(ExecutionException.class, () -> udpFail.get(15, TimeUnit.SECONDS));
            assertEquals(ServerModels.ServerState.FAILED, controller.state());

            // After both bind failures, starting on ephemeral free ports must succeed cleanly.
            ServerModels.ServerSnapshot recovered = controller.startServer(true, 0, 0,
                    List.of(new ServerDashboardController.AccountInput("alice", "pass".toCharArray())))
                    .get(15, TimeUnit.SECONDS);
            assertEquals(ServerModels.ServerState.RUNNING, recovered.state());
        }
    }
}
