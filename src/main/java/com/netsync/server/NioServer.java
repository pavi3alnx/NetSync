package com.netsync.server;

import com.netsync.common.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/** Single-use, loopback TCP transport. Callbacks run on the selector thread and must not block. */
public final class NioServer implements AutoCloseable, OutboundSink {
    private static final int FRAME_BUDGET = 32;
    private static final int BYTE_BUDGET = 256 * 1024;
    private final int port;
    private final MessageHandler messages;
    private final BiConsumer<ConnectionContext, FileChunk> chunks;
    private final FrameCodec codec = new FrameCodec(new MessageCodec());
    private final Map<UUID, ConnectionState> connections = new HashMap<>();
    private Selector selector;
    private ServerSocketChannel listener;
    private Thread thread;
    private volatile boolean running;
    private volatile IOException failure;
    private volatile int localPort = -1;

    /** Starts with no business handlers; valid frames are decoded and discarded. */
    public NioServer() { this(ProtocolLimits.TCP_PORT); }
    public NioServer(int port) { this(port, (context, message) -> {}, (context, chunk) -> {}); }
    public NioServer(int port, MessageHandler messages,
                     BiConsumer<ConnectionContext, FileChunk> chunks) {
        if (port < 0 || port > 65535) throw new IllegalArgumentException("Invalid TCP port");
        this.port = port;
        this.messages = Objects.requireNonNull(messages);
        this.chunks = Objects.requireNonNull(chunks);
    }

    /** Returns only after binding succeeds. Port zero requests an ephemeral test port. */
    public synchronized void start() throws IOException {
        if (thread != null) throw new IllegalStateException("Server is single-use");
        try {
            selector = Selector.open();
            listener = ServerSocketChannel.open();
            listener.configureBlocking(false);
            listener.bind(new InetSocketAddress(ProtocolLimits.BIND_ADDRESS, port));
            listener.register(selector, SelectionKey.OP_ACCEPT);
            localPort = ((InetSocketAddress) listener.getLocalAddress()).getPort();
            running = true;
            thread = new Thread(this::runLoop, "netsync-tcp-selector");
            thread.start();
        } catch (IOException | RuntimeException e) {
            running = false;
            cleanup();
            throw e;
        }
    }

    public int port() { return localPort; }
    public boolean isRunning() { return running; }
    /** Listener/selector failures are observable; individual client failures are isolated. */
    public IOException failure() { return failure; }

    private void runLoop() {
        try {
            while (running) {
                selector.select(1000); // Blocks until readiness, shutdown wakeup, or timeout maintenance.
                var keys = selector.selectedKeys().iterator();
                while (running && keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();
                    if (!key.isValid()) continue;
                    if (key.isAcceptable()) {
                        accept();
                        continue;
                    }
                    ConnectionState state = (ConnectionState) key.attachment();
                    try {
                        if (key.isReadable()) read(state);
                        if (key.isValid() && key.isWritable()) write(state);
                    } catch (IOException | RuntimeException e) {
                        disconnect(state);
                    }
                }
                long now = System.nanoTime();
                for (ConnectionState state : connections.values().toArray(ConnectionState[]::new)) {
                    if ((state.payload != null || state.prefix.position() > 0 || state.closing)
                            && now - state.lastProgress >= TimeUnit.SECONDS.toNanos(ProtocolLimits.FRAME_TIMEOUT_SECONDS)) {
                        disconnect(state);
                    }
                }
            }
        } catch (IOException e) {
            failure = e;
        } finally {
            running = false;
            cleanup();
        }
    }

    private void accept() throws IOException {
        for (int i = 0; i < FRAME_BUDGET; i++) {
            SocketChannel channel = listener.accept();
            if (channel == null) return;
            try {
                if (connections.size() >= ProtocolLimits.MAX_CONNECTIONS) {
                    channel.close();
                    continue;
                }
                channel.configureBlocking(false);
                SelectionKey key = channel.register(selector, SelectionKey.OP_READ);
                ConnectionState state = new ConnectionState(key);
                key.attach(state);
                connections.put(state.connectionId(), state);
            } catch (IOException | RuntimeException e) {
                channel.close();
            }
        }
    }

    private void read(ConnectionState state) throws IOException {
        SocketChannel channel = (SocketChannel) state.key.channel();
        for (int frames = 0, bytes = 0; frames < FRAME_BUDGET && bytes < BYTE_BUDGET;) {
            ByteBuffer target = state.payload == null ? state.prefix : state.payload;
            // Read the kind separately so invalid kind/length pairs fail immediately.
            boolean kind = state.payload != null && target.position() == 0;
            int originalLimit = target.limit();
            target.limit(Math.min(originalLimit, target.position() + (kind ? 1 : BYTE_BUDGET - bytes)));
            int count;
            try { count = channel.read(target); }
            finally { target.limit(originalLimit); }
            if (count < 0) { disconnect(state); return; }
            if (count == 0) return;
            bytes += count;
            state.lastProgress = System.nanoTime();
            try {
                if (kind) codec.validateKindLength(target.get(0), target.capacity());
                if (target.hasRemaining()) continue;
                target.flip();
                if (state.payload == null) {
                    int length = codec.decodeLength(target);
                    state.prefix.clear();
                    state.payload = ByteBuffer.allocate(length);
                } else {
                    // Reset before decoding: KEEP_OPEN errors must allow the next frame.
                    state.payload = null;
                    frames++;
                    OutboundFrame frame = codec.decodePayload(target);
                    if (frame instanceof MessageEnvelope message) messages.handle(state, message);
                    else chunks.accept(state, (FileChunk) frame);
                    if (!state.key.isValid() || state.closing) return;
                }
            } catch (ProtocolException e) {
                protocolFailure(state, e);
                if (!state.key.isValid() || state.closing) return;
            }
        }
    }

    private void protocolFailure(ConnectionState state, ProtocolException error) {
        if (error.action() == ProtocolException.Action.CLOSE) { disconnect(state); return; }
        send(state.connectionId(), new MessageEnvelope(ProtocolLimits.VERSION, MessageType.ERROR,
                UUID.randomUUID(), error.replyTo().orElse(null),
                new MessageBody.Error(error.code().orElseThrow(), "Invalid protocol message", null)));
        if (state.key.isValid() && error.action() == ProtocolException.Action.ERROR_THEN_CLOSE) {
            state.closing = true;
            state.key.interestOps(SelectionKey.OP_WRITE);
        }
    }

    /** Selector-thread only, as required by the shared OutboundSink contract. */
    @Override public EnqueueResult send(UUID connectionId, OutboundFrame frame) {
        if (Thread.currentThread() != thread) throw new IllegalStateException("Selector thread required");
        ConnectionState state = connections.get(connectionId);
        if (state == null || state.closing || !state.key.isValid()) return EnqueueResult.REJECTED;
        ByteBuffer encoded;
        try { encoded = codec.encode(frame); }
        catch (ProtocolException e) { return EnqueueResult.REJECTED; }
        if (state.output.size() >= ProtocolLimits.MAX_OUTPUT_FRAMES
                || encoded.remaining() > ProtocolLimits.MAX_OUTPUT_BYTES - state.queuedBytes) {
            disconnect(state);
            return EnqueueResult.REJECTED;
        }
        state.queuedBytes += encoded.remaining();
        state.output.add(encoded);
        state.key.interestOps(state.key.interestOps() | SelectionKey.OP_WRITE);
        return EnqueueResult.ACCEPTED;
    }

    private void write(ConnectionState state) throws IOException {
        SocketChannel channel = (SocketChannel) state.key.channel();
        int budget = BYTE_BUDGET;
        while (!state.output.isEmpty() && budget > 0) {
            ByteBuffer buffer = state.output.peek();
            int limit = buffer.limit();
            buffer.limit(Math.min(limit, buffer.position() + budget));
            int count;
            try { count = channel.write(buffer); }
            finally { buffer.limit(limit); }
            state.queuedBytes -= count;
            budget -= count;
            if (count == 0) return;
            if (buffer.hasRemaining()) continue;
            state.output.remove();
        }
        if (state.output.isEmpty()) {
            if (state.closing) disconnect(state);
            else state.key.interestOps(SelectionKey.OP_READ);
        }
    }

    private void disconnect(ConnectionState state) {
        connections.remove(state.connectionId());
        state.key.cancel();
        closeQuietly(state.key.channel());
        state.output.clear();
        state.payload = null;
    }

    private void cleanup() {
        for (ConnectionState state : connections.values().toArray(ConnectionState[]::new)) disconnect(state);
        closeQuietly(listener);
        closeQuietly(selector);
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource != null) try { resource.close(); } catch (Exception ignored) { }
    }

    /** Stops accepting and closes all clients; no application work needs draining in this milestone. */
    @Override public void close() {
        Thread loop;
        synchronized (this) {
            running = false;
            loop = thread;
            if (selector != null) selector.wakeup();
        }
        if (loop != null && loop != Thread.currentThread()) {
            boolean interrupted = false;
            for (;;) {
                try { loop.join(); break; }
                catch (InterruptedException e) { interrupted = true; }
            }
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws Exception {
        NioServer server = new NioServer(args.length == 0 ? ProtocolLimits.TCP_PORT : Integer.parseInt(args[0]));
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "netsync-tcp-shutdown"));
        System.out.println("NetSync TCP transport listening on 127.0.0.1:" + server.port());
    }
}
