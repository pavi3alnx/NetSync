package com.netsync.presence;

import com.netsync.common.Heartbeat;
import com.netsync.common.HeartbeatCodec;
import com.netsync.common.ProtocolLimits;
import org.junit.jupiter.api.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeartbeatSenderTest {
    private static final UUID CLIENT_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440003");
    private static final String PRESENCE_TOKEN = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    @Test
    void sendsImmediatelyAtIntervalWithIncreasingSequencesToLoopback() throws Exception {
        try (DatagramSocket receiver = loopbackReceiver()) {
            receiver.setSoTimeout(2_000);
            HeartbeatSender sender = new HeartbeatSender(
                    destination(receiver),
                    CLIENT_ID,
                    PRESENCE_TOKEN,
                    Duration.ofMillis(120));
            long startedAt = System.nanoTime();
            try {
                sender.start();
                sender.start();

                Heartbeat first = receive(receiver);
                long firstReceivedAt = System.nanoTime();
                Heartbeat second = receive(receiver);
                long secondReceivedAt = System.nanoTime();
                Heartbeat third = receive(receiver);

                assertTrue(TimeUnit.NANOSECONDS.toMillis(firstReceivedAt - startedAt) < 1_000);
                assertTrue(TimeUnit.NANOSECONDS.toMillis(secondReceivedAt - firstReceivedAt) >= 70);
                assertEquals(1, first.sequence());
                assertEquals(2, second.sequence());
                assertEquals(3, third.sequence());
                for (Heartbeat heartbeat : new Heartbeat[]{first, second, third}) {
                    assertEquals(ProtocolLimits.VERSION, heartbeat.version());
                    assertEquals("HEARTBEAT", heartbeat.type());
                    assertEquals(CLIENT_ID, heartbeat.clientId());
                    assertEquals(PRESENCE_TOKEN, heartbeat.presenceToken());
                }
            } finally {
                sender.stop();
            }
            assertFalse(sender.isRunning());
        }
    }

    @Test
    void repeatedStopAndRestartAreSafeAndSequenceContinues() throws Exception {
        try (DatagramSocket receiver = loopbackReceiver()) {
            HeartbeatSender sender = new HeartbeatSender(
                    destination(receiver),
                    CLIENT_ID,
                    PRESENCE_TOKEN,
                    Duration.ofMillis(500));
            try {
                receiver.setSoTimeout(2_000);
                sender.start();
                assertEquals(1, receive(receiver).sequence());

                sender.stop();
                sender.stop();
                assertFalse(sender.isRunning());
                receiver.setSoTimeout(150);
                assertThrows(SocketTimeoutException.class, () -> receive(receiver));

                receiver.setSoTimeout(2_000);
                sender.start();
                assertTrue(sender.isRunning());
                assertEquals(2, receive(receiver).sequence());
            } finally {
                sender.close();
                sender.close();
            }
            assertFalse(sender.isRunning());
        }
    }

    @Test
    void rejectsInvalidTokenAndNonLoopbackDestination() throws Exception {
        InetSocketAddress destination = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 5051);
        assertThrows(IllegalArgumentException.class,
                () -> new HeartbeatSender(destination, CLIENT_ID, "not-a-token"));

        InetSocketAddress external = new InetSocketAddress(InetAddress.getByName("192.0.2.1"), 5051);
        assertThrows(IllegalArgumentException.class,
                () -> new HeartbeatSender(external, CLIENT_ID, PRESENCE_TOKEN));
    }

    private static DatagramSocket loopbackReceiver() throws Exception {
        return new DatagramSocket(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
    }

    private static InetSocketAddress destination(DatagramSocket receiver) {
        return new InetSocketAddress(receiver.getLocalAddress(), receiver.getLocalPort());
    }

    private static Heartbeat receive(DatagramSocket receiver) throws Exception {
        byte[] buffer = new byte[ProtocolLimits.MAX_HEARTBEAT_BYTES];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        receiver.receive(packet);
        byte[] payload = Arrays.copyOf(packet.getData(), packet.getLength());
        return new HeartbeatCodec().decode(payload).orElseThrow();
    }
}
