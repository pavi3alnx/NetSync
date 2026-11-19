package com.netsync.server;

import com.netsync.common.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Consumer;

/** Single-use, loopback TCP transport. Callbacks run on the selector thread and must not block. */
public final class NioServer implements AutoCloseable, OutboundSink {
    private static final System.Logger LOG = System.getLogger(NioServer.class.getName());
    private static final int FRAME_BUDGET = 32;
    private static final int BYTE_BUDGET = 256 * 1024;
    private static final int TASK_BUDGET = 32;
    private final int port;
    private final MessageHandler messages;
    private final BiConsumer<ConnectionContext, FileChunk> chunks;
    private final FrameCodec codec = new FrameCodec(new MessageCodec());
    private final Map<UUID, ConnectionState> connections = new HashMap<>();
    private final Set<Consumer<UUID>> disconnectListeners = new LinkedHashSet<>();
    private final SelectorTaskQueue tasks = new SelectorTaskQueue(ProtocolLimits.SELECTOR_QUEUE_SIZE,
            () -> { if (this.selector != null) this.selector.wakeup(); });
    private SessionRegistry sessions;
    private Selector selector;
    private ServerSocketChannel listener;
    private Thread thread;
    private volatile boolean running;
    private volatile IOException failure;
    private volatile int localPort = -1;

    /** No business handlers: recognized JSON operations receive INVALID_REQUEST. */
    public NioServer() { this(ProtocolLimits.TCP_PORT); }
    public NioServer(int port) { this(port, MessageDispatcher.builder(), (context, chunk) -> {}); }

    /** Snapshots startup registrations and wires dispatch errors to this server's outbound queue. */
    public NioServer(int port, MessageDispatcher.Builder handlers,
                     BiConsumer<ConnectionContext, FileChunk> chunks) {
        this(port, sink -> Objects.requireNonNull(handlers).build(sink), chunks);
    }

    /** Retains the original callback path for custom wiring and transport fixtures. */
    public NioServer(int port, MessageHandler messages,
                     BiConsumer<ConnectionContext, FileChunk> chunks) {
        this(port, sink -> Objects.requireNonNull(messages), chunks);
    }

    private NioServer(int port, Function<OutboundSink, MessageHandler> messageFactory,
                      BiConsumer<ConnectionContext, FileChunk> chunks) {
        if (port < 0 || port > 65535) throw new IllegalArgumentException("Invalid TCP port");
        this.port = port;
        this.messages = messageFactory.apply(this);
        this.chunks = Objects.requireNonNull(chunks);
    }

    /**
     * Startup-only injection. Lookups must be fast, non-blocking, thread-safe memory lookups;
     * database/network access, credential work and waits are not permitted here.
     * Core does not create, authenticate, cache or revoke sessions.
     */
    public synchronized NioServer withSessionRegistry(SessionRegistry sessions) {
        requireConfigurable();
        this.sessions = Objects.requireNonNull(sessions);
        return this;
    }

    /** Startup-only registration. Called after transport cleanup, once per connection, on the selector thread. */
    public synchronized NioServer addDisconnectListener(Consumer<UUID> listener) {
        requireConfigurable();
        if (!disconnectListeners.add(Objects.requireNonNull(listener))) {
            throw new IllegalArgumentException("Disconnect listener already registered");
        }
        return this;
    }

    private void requireConfigurable() {
        if (thread != null || tasks.isShutdown()) throw new IllegalStateException("Server already started or closed");
    }

    /** Background threads submit here; task bodies execute on the selector and must not block. */
    public SelectorTaskQueue selectorTasks() { return tasks; }

    /** Returns only after binding succeeds. Port zero requests an ephemeral test port. */
    public synchronized void start() throws IOException {
        requireConfigurable();
        try {
            selector = Selector.open();
            listener = ServerSocketChannel.open();
            listener.configureBlocking(false);
            listener.bind(new InetSocketAddress(ProtocolLimits.BIND_ADDRESS, port));
            listener.register(selector, SelectionKey.OP_ACCEPT);
            localPort = ((InetSocketAddress) listener.getLocalAddress()).getPort();
            running = true;
            thread = new Thread(this::runLoop, "netsync-tcp-selector");
            tasks.start(thread);
            thread.start();
        } catch (IOException | RuntimeException e) {
            running = false;
            tasks.beginShutdown();
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
                // Never block with queued work; still poll sockets between finite task batches.
                if (tasks.hasTasks()) selector.selectNow();
                else selector.select(1000);
                tasks.runTasks(TASK_BUDGET);
                var keys = selector.selectedKeys().iterator();
                while (running && keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();
                    if (!key.isValid()) {
                        if (key.attachment() instanceof ConnectionState state) disconnect(state);
                        continue;
                    }
                    if (key.isAcceptable()) {
                        accept();
                        continue;
                    }
                    ConnectionState state = (ConnectionState) key.attachment();
                    try {
                        if (key.isReadable()) read(state);
                        if (key.isValid() && key.isWritable()) write(state);
                    } catch (IOException | CancelledKeyException e) {
                        logFailure(System.Logger.Level.DEBUG, "Client transport closed", e);
                        disconnect(state);
                    } catch (RuntimeException e) {
                        logFailure(System.Logger.Level.WARNING, "Client processing failed", e);
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
            logFailure(System.Logger.Level.ERROR, "TCP selector failed", e);
        } catch (RuntimeException e) {
            // Expose an unexpected loop failure through the existing failure accessor too.
            failure = new IOException("Unexpected selector failure: " + e.getClass().getName());
            logFailure(System.Logger.Level.ERROR, "TCP selector failed", e);
        } finally {
            running = false;
            try { tasks.finishShutdown(); }
            finally { cleanup(); }
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
                ConnectionState state = new ConnectionState(key, sessions);
                key.attach(state);
                connections.put(state.connectionId(), state);
            } catch (IOException | RuntimeException e) {
                logFailure(System.Logger.Level.WARNING, "Client registration failed", e);
                closeQuietly(channel);
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
                    if (!state.key.isValid()) { disconnect(state); return; }
                    if (state.closing) return;
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
        requireSelectorThread();
        ConnectionState state = connections.get(connectionId);
        if (state == null) return EnqueueResult.REJECTED;
        if (!state.key.isValid() || !state.key.channel().isOpen()) {
            disconnect(state);
            return EnqueueResult.REJECTED;
        }
        if (state.closing) return EnqueueResult.REJECTED;
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

    /**
     * Immediately close a client from a handler, discarding pending output.
     * Selector-thread only, like send; false means the connection was already absent.
     */
    public boolean closeConnection(UUID connectionId) {
        requireSelectorThread();
        ConnectionState state = connections.get(connectionId);
        if (state == null) return false;
        disconnect(state);
        return true;
    }

    private void requireSelectorThread() {
        if (Thread.currentThread() != thread) throw new IllegalStateException("Selector thread required");
    }

    private void disconnect(ConnectionState state) {
        if (state.closed) return;
        state.closed = true;
        connections.remove(state.connectionId());
        state.key.cancel();
        state.key.attach(null);
        closeQuietly(state.key.channel());
        state.output.clear();
        state.queuedBytes = 0;
        state.payload = null;
        state.prefix.clear();
        for (Consumer<UUID> listener : disconnectListeners) {
            try { listener.accept(state.connectionId()); }
            catch (RuntimeException e) {
                logFailure(System.Logger.Level.WARNING, "Disconnect listener failed", e);
            }
        }
    }

    private void cleanup() {
        for (ConnectionState state : connections.values().toArray(ConnectionState[]::new)) disconnect(state);
        closeQuietly(listener);
        closeQuietly(selector);
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource != null) try { resource.close(); }
        catch (Exception e) { logFailure(System.Logger.Level.WARNING, "Transport resource close failed", e); }
    }

    private static void logFailure(System.Logger.Level level, String operation, Exception error) {
        // Exception messages/causes from handlers may contain credentials or raw input.
        LOG.log(level, operation + " (" + error.getClass().getName() + ")");
    }

    /** Reject new tasks, finish accepted selector tasks, then close clients without draining socket output. */
    @Override public void close() {
        Thread loop;
        synchronized (this) {
            running = false;
            tasks.beginShutdown();
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
