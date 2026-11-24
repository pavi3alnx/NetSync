package com.netsync.testsupport;

import com.netsync.common.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;

/** Controlled loopback TCP peer for deterministic client lifecycle tests. */
public final class ControlledTcpPeer implements AutoCloseable {
    private static final UUID CLIENT_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final FrameCodec CODEC = new FrameCodec(new MessageCodec());
    private final ServerSocket listener;
    private final LinkedBlockingQueue<Connection> connections = new LinkedBlockingQueue<>();
    private final CopyOnWriteArrayList<Connection> allConnections = new CopyOnWriteArrayList<>();
    private volatile boolean closed;
    private final Thread acceptor;

    public ControlledTcpPeer() throws IOException {
        listener = new ServerSocket(0, 10, InetAddress.getByName("127.0.0.1"));
        acceptor = new Thread(this::acceptLoop, "test-controlled-tcp-peer");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    public int port() { return listener.getLocalPort(); }

    public Connection nextConnection() throws InterruptedException {
        Connection connection = connections.poll(3, TimeUnit.SECONDS);
        if (connection == null) throw new AssertionError("Client did not connect to controlled peer");
        return connection;
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Connection connection = new Connection(listener.accept());
                allConnections.add(connection);
                connections.add(connection);
                connection.start();
            } catch (IOException failure) {
                if (!closed) throw new IllegalStateException("Controlled peer accept failed", failure);
            }
        }
    }

    @Override public void close() throws IOException {
        closed = true;
        listener.close();
        for (Connection connection : allConnections) connection.close();
    }

    public static final class Connection implements AutoCloseable {
        private final Socket socket;
        private final LinkedBlockingQueue<MessageEnvelope> requests = new LinkedBlockingQueue<>();
        private final Thread reader;
        private volatile boolean closed;

        private Connection(Socket socket) {
            this.socket = socket;
            reader = new Thread(this::readLoop, "test-controlled-tcp-reader");
            reader.setDaemon(true);
        }

        private void start() { reader.start(); }

        public MessageEnvelope nextRequest(MessageType type) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline) {
                MessageEnvelope message = requests.poll(50, TimeUnit.MILLISECONDS);
                if (message == null) continue;
                if (message.type() == type) return message;
                throw new AssertionError("Unexpected client request type: " + message.type());
            }
            throw new AssertionError("Client did not send " + type);
        }

        public synchronized void send(MessageEnvelope message) throws IOException, ProtocolException {
            ByteBuffer bytes = CODEC.encode(message);
            byte[] frame = new byte[bytes.remaining()];
            bytes.get(frame);
            socket.getOutputStream().write(frame);
            socket.getOutputStream().flush();
        }

        public void acknowledge(MessageEnvelope request, MessageType operation, AckStatus status) throws IOException, ProtocolException {
            send(new MessageEnvelope(ProtocolLimits.VERSION, MessageType.ACK, UUID.randomUUID(),
                    request.messageId(), new MessageBody.Ack(operation, status, null)));
        }

        public void deliver(UUID sender, String text) throws IOException, ProtocolException {
            send(new MessageEnvelope(ProtocolLimits.VERSION, MessageType.CHAT_DELIVER, UUID.randomUUID(), null,
                    new MessageBody.ChatDeliver(sender, text, UUID.randomUUID())));
        }

        public void sendInvalidFrame() throws IOException {
            socket.getOutputStream().write(new byte[]{0, 0, 0, 0});
            socket.getOutputStream().flush();
        }

        private void readLoop() {
            try {
                DataInputStream input = new DataInputStream(socket.getInputStream());
                while (!closed) {
                    int length = CODEC.decodeLength(ByteBuffer.wrap(input.readNBytes(4)));
                    byte[] payload = input.readNBytes(length);
                    if (payload.length != length) return;
                    MessageEnvelope message = (MessageEnvelope) CODEC.decodePayload(ByteBuffer.wrap(payload));
                    if (message.type() == MessageType.LOGIN_REQUEST) {
                        send(new MessageEnvelope(ProtocolLimits.VERSION, MessageType.LOGIN_RESPONSE, UUID.randomUUID(),
                                message.messageId(), new MessageBody.LoginResponse(CLIENT_ID,
                                ((MessageBody.LoginRequest) message.body()).username(),
                                Base64.getEncoder().encodeToString(new byte[32]),
                                ProtocolLimits.HEARTBEAT_INTERVAL_SECONDS, ProtocolLimits.PRESENCE_TIMEOUT_SECONDS)));
                    } else requests.add(message);
                }
            } catch (IOException | ProtocolException | RuntimeException ignored) { }
        }

        @Override public void close() throws IOException {
            closed = true;
            socket.close();
        }
    }
}
