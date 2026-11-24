package com.netsync.client;

import com.netsync.common.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

/** Blocking socket I/O with one reader, one writer and one bounded serial dispatcher.
 * No application callbacks execute on socket threads. Disconnect cancels queued work;
 * close is terminal. Reconnect requires the previous I/O threads to terminate.
 */
public final class TcpClient implements ClientOutbound, AutoCloseable {
    public enum State { DISCONNECTED, CONNECTING, CONNECTED, CLOSED }
    public enum Failure { NONE, CONNECT_FAILED, REMOTE_CLOSED, IO_ERROR, INVALID_FRAME,
        DISPATCH_FULL, NO_BINARY_HANDLER, FRAME_TIMEOUT }
    private final FrameCodec codec = new FrameCodec(new MessageCodec());
    private final ClientDispatcher dispatcher = new ClientDispatcher();
    private final ThreadPoolExecutor dispatch;
    private final int maxFrames, maxBytes;
    private State state = State.DISCONNECTED;
    private Failure failure = Failure.NONE;
    private Link current;
    private SessionView session;
    private long protocolRejections;
    private volatile Thread dispatchThread;
    private final Set<BiConsumer<UUID, Failure>> disconnectListeners = new LinkedHashSet<>();

    public TcpClient() { this(256, 1_048_576, 256); }
    public TcpClient(int outputFrames, int outputBytes, int dispatchCapacity) {
        if (outputFrames < 1 || outputBytes < 1 || dispatchCapacity < 1) throw new IllegalArgumentException("Invalid queue capacity");
        maxFrames = outputFrames; maxBytes = outputBytes;
        dispatch = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(dispatchCapacity), task -> {
                Thread t = new Thread(task, "netsync-client-dispatch");
                dispatchThread = t; t.setDaemon(true); return t;
            }, new ThreadPoolExecutor.AbortPolicy());
    }
    public ClientDispatcher dispatcher() { return dispatcher; }
    public synchronized State state() { return state; }
    public synchronized long protocolRejections() { return protocolRejections; }
    public synchronized Failure failure() { return failure; }
    @Override public synchronized boolean isConnected() { return state == State.CONNECTED; }
    public synchronized Optional<UUID> connectionId() { return isConnected() ? Optional.of(current.id) : Optional.empty(); }
    synchronized boolean isGenerationCurrent(UUID id) { return current != null && current.id.equals(id); }
    synchronized boolean isGenerationActive(UUID id) {
        return current != null && current.id.equals(id) && state == State.CONNECTED;
    }
    public synchronized Optional<SessionView> session() { return isConnected() ? Optional.ofNullable(session) : Optional.empty(); }
    /** Security supplies a shared immutable snapshot bound to this local connection generation. */
    public synchronized void supplySession(UUID connectionId, SessionView value) {
        Objects.requireNonNull(value);
        if (!isConnected() || !current.id.equals(connectionId) || !connectionId.equals(value.connectionId()))
            throw new IllegalStateException("Session belongs to an inactive connection");
        session = value;
    }
    public synchronized void clearSession() { session = null; }
    boolean isDispatchThread() { return Thread.currentThread() == dispatchThread; }
    /** Client-internal lifecycle hook. Runs under the transport monitor: clear state only, never block. */
    synchronized ClientDispatcher.Registration onDisconnect(BiConsumer<UUID, Failure> listener) {
        Objects.requireNonNull(listener);
        disconnectListeners.add(listener);
        return () -> { synchronized (TcpClient.this) { disconnectListeners.remove(listener); } };
    }
    /** Authentication is restricted to the documented unencrypted loopback demo. */
    public synchronized boolean isLoopbackConnection() {
        return isConnected() && current.socket.getInetAddress().isLoopbackAddress();
    }
    public void connect() throws IOException { connect(ProtocolLimits.BIND_ADDRESS, ProtocolLimits.TCP_PORT); }
    public void connect(String host, int port) throws IOException {
        Objects.requireNonNull(host);
        if (host.isBlank() || port < 1 || port > 65535) throw new IllegalArgumentException("Invalid server address");
        Link link;
        synchronized (this) {
            if (state != State.DISCONNECTED) throw new IllegalStateException("Client is " + state);
            if (current != null && (alive(current.reader) || alive(current.writer)))
                throw new IllegalStateException("Previous connection is still stopping");
            if (current != null) current.cancelled = true;
            dispatch.getQueue().clear();
            link = new Link(); current = link; state = State.CONNECTING; failure = Failure.NONE;
        }
        try {
            link.socket.connect(new InetSocketAddress(host, port), 5000);
            link.socket.setTcpNoDelay(true);
            link.socket.setSoTimeout(1000);
            synchronized (this) {
                if (current != link || state != State.CONNECTING) throw new IOException("Connection attempt cancelled");
                state = State.CONNECTED;
                link.reader = thread("netsync-client-read", () -> read(link));
                link.writer = thread("netsync-client-write", () -> write(link));
                link.reader.start(); link.writer.start();
            }
        } catch (IOException | RuntimeException e) {
            stop(link, Failure.CONNECT_FAILED);
            throw e;
        }
    }
    @Override public synchronized EnqueueResult send(OutboundFrame frame) throws ProtocolException {
        Objects.requireNonNull(frame);
        if (!isConnected()) return EnqueueResult.REJECTED;
        ByteBuffer encoded = codec.encode(frame);
        if (current.output.size() >= maxFrames || encoded.remaining() > maxBytes - current.bytes)
            return EnqueueResult.REJECTED;
        current.bytes += encoded.remaining(); current.output.add(encoded); notifyAll();
        return EnqueueResult.ACCEPTED;
    }
    private void write(Link link) {
        try {
            OutputStream out = link.socket.getOutputStream();
            while (true) {
                ByteBuffer frame;
                synchronized (this) {
                    while (active(link) && link.output.isEmpty()) wait();
                    if (!active(link)) return;
                    frame = link.output.peek();
                }
                // OutputStream.write completes the requested bytes or throws; in-flight frame
                // remains charged to both limits until the entire write completes.
                out.write(frame.array(), frame.position(), frame.remaining());
                synchronized (this) {
                    if (!active(link)) return;
                    link.output.remove(); link.bytes -= frame.remaining();
                }
            }
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        catch (IOException e) { stop(link, Failure.IO_ERROR); }
    }
    private void read(Link link) {
        try {
            InputStream in = link.socket.getInputStream();
            while (active(link)) {
                byte[] prefix = readBytes(link, in, 4, false);
                int length = codec.decodeLength(ByteBuffer.wrap(prefix));
                byte[] payload = new byte[length];
                payload[0] = readBytes(link, in, 1, true)[0];
                codec.validateKindLength(payload[0], length);
                byte[] body = readBytes(link, in, length - 1, true);
                System.arraycopy(body, 0, payload, 1, body.length);
                OutboundFrame frame;
                try { frame = codec.decodePayload(ByteBuffer.wrap(payload)); }
                catch (ProtocolException e) {
                    if (e.action() != ProtocolException.Action.KEEP_OPEN) throw e;
                    synchronized (this) { protocolRejections++; }
                    continue;
                }
                try {
                    dispatch.execute(() -> {
                        if (dispatchable(link) && !dispatcher.dispatch(frame, () -> dispatchable(link))) stop(link, Failure.NO_BINARY_HANDLER);
                    });
                } catch (RejectedExecutionException e) { stop(link, Failure.DISPATCH_FULL); return; }
            }
        } catch (EOFException e) { stop(link, Failure.REMOTE_CLOSED); }
        catch (SocketTimeoutException e) { stop(link, Failure.FRAME_TIMEOUT); }
        catch (ProtocolException e) { stop(link, Failure.INVALID_FRAME); }
        catch (IOException e) { stop(link, Failure.IO_ERROR); }
    }
    private byte[] readBytes(Link link, InputStream in, int length, boolean partial) throws IOException {
        byte[] bytes = new byte[length]; int offset = 0;
        long progress = System.nanoTime();
        while (offset < length) {
            if (!active(link)) throw new EOFException();
            try {
                int n = in.read(bytes, offset, length - offset);
                if (n < 0) throw new EOFException();
                offset += n; progress = System.nanoTime();
            } catch (SocketTimeoutException e) {
                if ((partial || offset > 0) && System.nanoTime() - progress >= TimeUnit.SECONDS.toNanos(15)) throw e;
            }
        }
        return bytes;
    }
    private synchronized boolean dispatchable(Link link) {
        return current == link && !link.cancelled && state != State.CLOSED;
    }
    private synchronized boolean active(Link link) { return current == link && state == State.CONNECTED; }
    private synchronized void stop(Link link, Failure reason) {
        try { link.socket.close(); } catch (IOException ignored) { }
        if (current != link) return;
        if (state != State.CLOSED && (state != State.DISCONNECTED
                || (reason == Failure.NO_BINARY_HANDLER && !link.cancelled))) {
            state = State.DISCONNECTED; failure = reason;
        }
        session = null; link.output.clear(); link.bytes = 0;
        // Complete inbound frames preceding orderly EOF remain observable. Explicit
        // disconnect/reconnect, protocol failures and overload cancel pending callbacks.
        if (reason != Failure.REMOTE_CLOSED) { link.cancelled = true; dispatch.getQueue().clear(); }
        if (!link.disconnectNotified) {
            link.disconnectNotified = true;
            for (var listener : List.copyOf(disconnectListeners)) {
                try { listener.accept(link.id, reason); }
                catch (RuntimeException failure) {
                    System.getLogger(TcpClient.class.getName()).log(System.Logger.Level.WARNING,
                            "Client disconnect listener failed (" + failure.getClass().getName() + ")");
                }
            }
        }
        notifyAll();
    }
    public void disconnect() {
        Link link;
        synchronized (this) { link = current; if (link != null) stop(link, Failure.NONE); }
        if (link != null) { join(link.reader); join(link.writer); }
    }
    /** Close only the expected generation; a delayed feature timeout cannot close a reconnect. */
    public void disconnect(UUID expectedConnectionId) {
        Objects.requireNonNull(expectedConnectionId);
        Link link;
        synchronized (this) {
            link = current;
            if (link == null || !link.id.equals(expectedConnectionId)) return;
            stop(link, Failure.NONE);
        }
        join(link.reader); join(link.writer);
    }
    @Override public void close() {
        synchronized (this) { state = State.CLOSED; }
        disconnect(); dispatcher.close(); dispatch.shutdownNow();
        // Do not wait on ourselves when a handler closes the client.
        if (!Thread.currentThread().getName().equals("netsync-client-dispatch")) {
            try { dispatch.awaitTermination(2, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
    private static Thread thread(String name, Runnable task) { Thread t = new Thread(task, name); t.setDaemon(true); return t; }
    private static boolean alive(Thread t) { return t != null && t.isAlive(); }
    private static void join(Thread t) {
        if (t == null || t == Thread.currentThread()) return;
        try { t.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
    private static final class Link {
        final UUID id = UUID.randomUUID();
        final Socket socket = new Socket();
        final ArrayDeque<ByteBuffer> output = new ArrayDeque<>();
        int bytes;
        boolean cancelled;
        boolean disconnectNotified;
        Thread reader, writer;
    }
}

