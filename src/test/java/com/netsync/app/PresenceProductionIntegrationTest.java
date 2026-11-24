package com.netsync.app;

import com.netsync.common.*;
import com.netsync.security.CredentialStore;
import com.netsync.server.NioServer;
import com.netsync.transfer.TransferWorkerPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class PresenceProductionIntegrationTest {
    private static final FrameCodec FRAMES = new FrameCodec(new MessageCodec());
    private static final HeartbeatCodec HEARTBEATS = new HeartbeatCodec();
    private static final CredentialStore ACCOUNTS = new CredentialStore(Map.of(
            "alice", "disposable-alice".toCharArray(), "bob", "disposable-bob".toCharArray()));

    @Test
    void productionUdpExpiryAndRecoveryKeepTcpAuthenticationValid() throws Exception {
        AtomicLong clock = new AtomicLong();
        try (var app = new NetSyncServerApplication(config(), ACCOUNTS, NioServer::new,
                TransferWorkerPool::new, clock::get)) {
            app.start();
            try (Socket alice = connect(app); DatagramSocket udp = new DatagramSocket()) {
                MessageBody.LoginResponse identity = login(alice, "alice");
                assertEquals(5, identity.heartbeatIntervalSeconds());
                assertEquals(15, identity.presenceTimeoutSeconds());
                assertTrue(sendHeartbeat(udp, app, identity, 1));
                await(() -> app.decodedHeartbeatsForTest() == 1);

                clock.set(TimeUnit.SECONDS.toNanos(14));
                sendHeartbeat(udp, app, identity, 2);
                await(() -> app.decodedHeartbeatsForTest() == 2);
                clock.set(TimeUnit.SECONDS.toNanos(29));
                await(() -> app.presenceTracker().getPresenceState(identity.clientId())
                        .orElse(PresenceState.ONLINE) == PresenceState.OFFLINE);
                assertTrue(app.sessions().findByClientId(identity.clientId()).isPresent());
                assertTrue(app.authentication().isPresenceTokenValid(identity.clientId(), identity.presenceToken()));
                MessageBody.PresenceSnapshot offline = querySnapshot(alice);
                assertTrue(offline.clients().stream().anyMatch(entry -> entry.clientId().equals(identity.clientId())
                        && entry.state() == PresenceState.OFFLINE));

                sendHeartbeat(udp, app, identity, 3);
                await(() -> app.decodedHeartbeatsForTest() == 3);
                await(() -> app.presenceTracker().getPresenceState(identity.clientId())
                        .orElse(PresenceState.OFFLINE) == PresenceState.ONLINE);
                assertTrue(app.sessions().findByClientId(identity.clientId()).isPresent());
                assertTrue(querySnapshot(alice).clients().stream().anyMatch(entry ->
                        entry.clientId().equals(identity.clientId()) && entry.state() == PresenceState.ONLINE));
            }
        }
    }

    @Test
    void productionLogoutRejectsFormerTokenAndFreshSessionRemainsIndependent() throws Exception {
        try (var app = new NetSyncServerApplication(config(), ACCOUNTS)) {
            app.start();
            try (Socket alice = connect(app); Socket bob = connect(app); DatagramSocket udp = new DatagramSocket()) {
                MessageBody.LoginResponse oldAlice = login(alice, "alice");
                MessageBody.LoginResponse bobIdentity = login(bob, "bob");
                querySnapshot(bob); // Drain both initial ONLINE broadcasts before the logout/replay check.
                sendHeartbeat(udp, app, oldAlice, 1);
                await(() -> app.decodedHeartbeatsForTest() == 1);
                assertTrue(app.presenceTracker().getPresenceState(oldAlice.clientId()).isPresent());

                logout(alice);
                await(() -> app.sessions().findByClientId(oldAlice.clientId()).isEmpty());
                sendHeartbeat(udp, app, oldAlice, 2);
                await(() -> app.decodedHeartbeatsForTest() == 2);
                assertTrue(app.presenceTracker().getPresenceState(oldAlice.clientId()).isEmpty());
                assertFalse(app.authentication().isPresenceTokenValid(oldAlice.clientId(), oldAlice.presenceToken()));
                List<MessageBody.PresenceUpdate> afterLogout = new ArrayList<>();
                assertTrue(querySnapshot(bob, afterLogout).clients().stream().noneMatch(entry ->
                        entry.clientId().equals(oldAlice.clientId())));
                assertFalse(afterLogout.stream().anyMatch(event -> event.clientId().equals(oldAlice.clientId())
                        && event.state() == PresenceState.ONLINE));

                try (Socket freshAlice = connect(app)) {
                    MessageBody.LoginResponse fresh = login(freshAlice, "alice");
                    assertNotEquals(oldAlice.clientId(), fresh.clientId());
                    assertNotEquals(oldAlice.presenceToken(), fresh.presenceToken());
                    sendHeartbeat(udp, app, oldAlice, 3);
                    await(() -> app.decodedHeartbeatsForTest() == 3);
                    assertEquals(Optional.of(PresenceState.ONLINE),
                            app.presenceTracker().getPresenceState(fresh.clientId()));
                    assertTrue(app.authentication().isPresenceTokenValid(fresh.clientId(), fresh.presenceToken()));
                    List<MessageBody.PresenceUpdate> afterReplay = new ArrayList<>();
                    MessageBody.PresenceSnapshot snapshot = querySnapshot(bob, afterReplay);
                    assertTrue(snapshot.clients().stream().anyMatch(entry -> entry.clientId().equals(fresh.clientId())
                            && entry.state() == PresenceState.ONLINE));
                    assertFalse(snapshot.clients().stream().anyMatch(entry -> entry.clientId().equals(oldAlice.clientId())));
                    assertFalse(afterReplay.stream().anyMatch(event -> event.clientId().equals(oldAlice.clientId())
                            && event.state() == PresenceState.ONLINE));
                }
                assertTrue(app.sessions().findByClientId(bobIdentity.clientId()).isPresent());
            }
        }
    }

    private static DemoServerConfig config() { return new DemoServerConfig(true, 0, List.of("alice", "bob"), 0); }
    private static Socket connect(NetSyncServerApplication app) throws Exception {
        Socket socket = new Socket("127.0.0.1", app.port()); socket.setSoTimeout(3000); return socket;
    }
    private static MessageBody.LoginResponse login(Socket socket, String user) throws Exception {
        MessageEnvelope request = new MessageEnvelope(1, MessageType.LOGIN_REQUEST, UUID.randomUUID(), null,
                new MessageBody.LoginRequest(user, "disposable-" + user));
        write(socket, request);
        MessageEnvelope response = readUntil(socket, request.messageId());
        assertEquals(MessageType.LOGIN_RESPONSE, response.type());
        return (MessageBody.LoginResponse) response.body();
    }
    private static void logout(Socket socket) throws Exception {
        MessageEnvelope request = empty(MessageType.LOGOUT_REQUEST); write(socket, request);
        MessageEnvelope ack = readUntil(socket, request.messageId());
        assertEquals(MessageType.ACK, ack.type());
    }
    private static MessageBody.PresenceSnapshot querySnapshot(Socket socket) throws Exception {
        return querySnapshot(socket, new ArrayList<>());
    }
    private static MessageBody.PresenceSnapshot querySnapshot(Socket socket,
                                                               List<MessageBody.PresenceUpdate> events) throws Exception {
        MessageEnvelope request = empty(MessageType.PRESENCE_QUERY); write(socket, request);
        MessageEnvelope response;
        do {
            response = read(socket);
            if (response.type() == MessageType.PRESENCE_UPDATE)
                events.add((MessageBody.PresenceUpdate) response.body());
        } while (!request.messageId().equals(response.replyTo()));
        assertEquals(MessageType.PRESENCE_SNAPSHOT, response.type());
        return (MessageBody.PresenceSnapshot) response.body();
    }
    private static MessageEnvelope empty(MessageType type) {
        return new MessageEnvelope(1, type, UUID.randomUUID(), null, new MessageBody.Empty());
    }
    private static boolean sendHeartbeat(DatagramSocket socket, NetSyncServerApplication app,
                                         MessageBody.LoginResponse identity, long sequence) throws Exception {
        byte[] packet = HEARTBEATS.encode(new Heartbeat(1, "HEARTBEAT", identity.clientId(),
                identity.presenceToken(), sequence));
        socket.send(new DatagramPacket(packet, packet.length, InetAddress.getLoopbackAddress(), app.udpPort()));
        return true;
    }
    private static void write(Socket socket, OutboundFrame frame) throws Exception {
        ByteBuffer encoded = FRAMES.encode(frame); byte[] bytes = new byte[encoded.remaining()]; encoded.get(bytes);
        socket.getOutputStream().write(bytes);
    }
    private static MessageEnvelope readUntil(Socket socket, UUID replyTo) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            MessageEnvelope message = read(socket);
            if (message.type() != MessageType.PRESENCE_UPDATE && replyTo.equals(message.replyTo())) return message;
        }
        throw new AssertionError("Timed out waiting for correlated TCP response");
    }
    private static MessageEnvelope read(Socket socket) throws Exception {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        byte[] prefix = input.readNBytes(4); assertEquals(4, prefix.length);
        int size = FRAMES.decodeLength(ByteBuffer.wrap(prefix));
        byte[] payload = input.readNBytes(size); assertEquals(size, payload.length);
        return (MessageEnvelope) FRAMES.decodePayload(ByteBuffer.wrap(payload));
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.yield();
        assertTrue(condition.getAsBoolean());
    }
}
