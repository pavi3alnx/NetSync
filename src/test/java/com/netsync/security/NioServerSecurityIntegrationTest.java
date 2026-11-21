package com.netsync.security;

import com.netsync.common.*;
import com.netsync.presence.HeartbeatTracker;
import com.netsync.server.MessageDispatcher;
import com.netsync.server.NioServer;
import com.netsync.transfer.TransferWorkerPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class NioServerSecurityIntegrationTest {
    private final FrameCodec codec = new FrameCodec(new MessageCodec());

    @Test
    void loginRegistersPresenceAndLogoutDrainsAcknowledgementBeforeClosing() throws Exception {
        try (Fixture fixture = new Fixture(); Socket client = fixture.connect()) {
            MessageEnvelope login = login("demo-alice-pass");
            client.getOutputStream().write(encode(login));

            MessageEnvelope response = readMessage(client);
            assertEquals(MessageType.LOGIN_RESPONSE, response.type());
            assertEquals(login.messageId(), response.replyTo());
            MessageBody.LoginResponse body = assertInstanceOf(MessageBody.LoginResponse.class, response.body());
            assertEquals(PresenceState.ONLINE, fixture.presence.getPresenceState(body.clientId()).orElseThrow());
            assertTrue(fixture.auth.isPresenceTokenValid(body.clientId(), body.presenceToken()));
            UUID connectionId = fixture.registry.findByClientId(body.clientId())
                    .map(SessionView::connectionId)
                    .orElseThrow();

            MessageEnvelope logout = new MessageEnvelope(ProtocolLimits.VERSION,
                    MessageType.LOGOUT_REQUEST, UUID.randomUUID(), null, new MessageBody.Empty());
            client.getOutputStream().write(encode(logout));

            MessageEnvelope acknowledgement = readMessage(client);
            assertEquals(MessageType.ACK, acknowledgement.type());
            assertEquals(logout.messageId(), acknowledgement.replyTo());
            assertEquals(AckStatus.ACCEPTED, ((MessageBody.Ack) acknowledgement.body()).status());
            assertEquals(-1, client.getInputStream().read());
            assertFalse(fixture.auth.isAuthenticatedConnection(connectionId));
            assertTrue(fixture.registry.findByClientId(body.clientId()).isEmpty());
            assertTrue(fixture.presence.getPresenceState(body.clientId()).isEmpty());
        }
    }

    @Test
    void failedLoginDrainsGenericErrorBeforeClosing() throws Exception {
        try (Fixture fixture = new Fixture(); Socket client = fixture.connect()) {
            MessageEnvelope login = login("incorrect");
            client.getOutputStream().write(encode(login));

            MessageEnvelope response = readMessage(client);
            assertEquals(MessageType.ERROR, response.type());
            assertEquals(login.messageId(), response.replyTo());
            MessageBody.Error error = assertInstanceOf(MessageBody.Error.class, response.body());
            assertEquals(ErrorCode.AUTH_FAILED, error.code());
            assertEquals("Authentication failed", error.message());
            assertEquals(-1, client.getInputStream().read());
            assertTrue(fixture.registry.activeSessions().isEmpty());
        }
    }

    @Test
    void disconnectRevokesSessionAndPresence() throws Exception {
        try (Fixture fixture = new Fixture()) {
            UUID clientId;
            Socket client = fixture.connect();
            try {
                client.getOutputStream().write(encode(login("demo-alice-pass")));
                MessageBody.LoginResponse body = assertInstanceOf(
                        MessageBody.LoginResponse.class, readMessage(client).body());
                clientId = body.clientId();
                assertEquals(PresenceState.ONLINE, fixture.presence.getPresenceState(clientId).orElseThrow());
            } finally {
                client.close();
            }

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (fixture.registry.findByClientId(clientId).isPresent()
                    && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertTrue(fixture.registry.findByClientId(clientId).isEmpty());
            assertTrue(fixture.presence.getPresenceState(clientId).isEmpty());
        }
    }

    private MessageEnvelope login(String password) {
        return new MessageEnvelope(ProtocolLimits.VERSION, MessageType.LOGIN_REQUEST,
                UUID.randomUUID(), null, new MessageBody.LoginRequest("alice", password));
    }

    private byte[] encode(OutboundFrame frame) throws ProtocolException {
        ByteBuffer buffer = codec.encode(frame);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private MessageEnvelope readMessage(Socket socket) throws Exception {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int length = input.readInt();
        byte[] payload = input.readNBytes(length);
        assertEquals(length, payload.length);
        return assertInstanceOf(MessageEnvelope.class, codec.decodePayload(ByteBuffer.wrap(payload)));
    }

    private static final class Fixture implements AutoCloseable {
        private final SessionRegistryImpl registry = new SessionRegistryImpl();
        private final AuthService auth = new AuthService(
                new CredentialStore(List.of(
                        new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray()))),
                registry,
                new SessionTokenGenerator());
        private final AuthorizationServiceImpl authorization = new AuthorizationServiceImpl(registry);
        private final TransferWorkerPool workers = new TransferWorkerPool(2, 32);
        private final HeartbeatTracker presence = new HeartbeatTracker(
                System::nanoTime,
                (clientId, state) -> {},
                registry,
                auth::isPresenceTokenValid);
        private final NioServer server = new NioServer(0, nio -> {
            nio.withSessionRegistry(registry);
            LoginHandler login = new LoginHandler(
                    auth,
                    workers::submit,
                    nio.selectorTasks(),
                    nio,
                    nio,
                    result -> {
                        if (!presence.registerSession(result.session().clientId(), result.presenceToken())) {
                            throw new IllegalStateException("Authenticated presence registration failed");
                        }
                    },
                    session -> presence.revokeSession(session.clientId()));
            LogoutHandler logout = new LogoutHandler(
                    auth,
                    authorization,
                    nio,
                    nio,
                    session -> presence.revokeSession(session.clientId()));
            nio.addDisconnectListener(login::onDisconnected);
            return MessageDispatcher.builder()
                    .register(MessageType.LOGIN_REQUEST, login)
                    .register(MessageType.LOGOUT_REQUEST, logout)
                    .build(nio);
        }, (context, chunk) -> {});

        private Fixture() throws Exception {
            server.start();
        }

        private Socket connect() throws Exception {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
            socket.setSoTimeout(5000);
            return socket;
        }

        @Override
        public void close() {
            server.close();
            workers.close();
        }
    }
}
