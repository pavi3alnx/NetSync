package com.netsync.client;

import com.netsync.common.*;
import com.netsync.presence.HeartbeatSender;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns heartbeat resources off-dispatch; no presence tokens are rendered. */
public final class ClientPresenceAdapter implements AutoCloseable {
    /** Structured presence events for GUI and programmatic integration. */
    public sealed interface PresenceEvent {
        UUID connectionId();
        record SnapshotReceived(UUID connectionId, List<MessageBody.PresenceEntry> entries) implements PresenceEvent {}
        record UserUpdated(UUID connectionId, MessageBody.PresenceEntry entry) implements PresenceEvent {}
        record QueryFailed(UUID connectionId, ErrorCode code) implements PresenceEvent {}
        record HeartbeatStartFailed(UUID connectionId, String reason) implements PresenceEvent {}
    }

    private final TcpClient client;
    private final ClientAuthenticationAdapter authentication;
    private final CommandRegistry commands;
    private final ClientOutputQueue output;
    private final List<ClientDispatcher.Registration> registrations = new ArrayList<>();
    private final Set<java.util.function.Consumer<PresenceEvent>> eventListeners = new LinkedHashSet<>();
    private final CommandRegistry.Command queryCommand = args -> {
        if (!args.isEmpty()) throw new CommandRegistry.InputException("Usage: /presence");
        query();
    };
    private final AtomicBoolean queued = new AtomicBoolean(), closed = new AtomicBoolean();
    private final ThreadPoolExecutor lifecycle;
    private volatile Thread worker;
    private volatile HeartbeatSender sender;
    private ClientAuthenticationAdapter.PresenceCredentials owned;
    private volatile int udpPort;
    // Query state is guarded by the transport monitor.
    private UUID queryId, queryConnection;
    private long queryStarted;

    /** Public constructor for GUI and programmatic integration without a CLI output queue. */
    public ClientPresenceAdapter(TcpClient client, ClientAuthenticationAdapter authentication,
                                 CommandRegistry commands, int udpPort) {
        this(client, authentication, commands, null, udpPort);
    }

    ClientPresenceAdapter(TcpClient client, ClientAuthenticationAdapter authentication,
                          CommandRegistry commands, ClientOutputQueue output, int udpPort) {
        this.client = Objects.requireNonNull(client); this.authentication = Objects.requireNonNull(authentication);
        this.commands = Objects.requireNonNull(commands); this.output = output;
        setUdpPort(udpPort);
        lifecycle = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1), task -> {
            Thread thread = new Thread(() -> {
                try { task.run(); }
                finally { if (sender != null) { sender.close(); sender = null; } owned = null; }
            }, "netsync-client-presence");
            thread.setDaemon(true); worker = thread; return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
        try {
            commands.register("/presence", "/presence", "Query authenticated presence", queryCommand);
            registrations.add(authentication.onStateChanged(this::signal));
            registrations.add(client.dispatcher().register(MessageType.PRESENCE_SNAPSHOT, this::snapshot));
            registrations.add(client.dispatcher().register(MessageType.PRESENCE_UPDATE, this::update));
            registrations.add(client.dispatcher().register(MessageType.ERROR, this::error));
            signal();
        } catch (RuntimeException failure) { close(); throw failure; }
    }

    public void setUdpPort(int port) {
        if (port < 1 || port > 65535) throw new CommandRegistry.InputException("UDP port must be from 1 to 65535.");
        synchronized (client) {
            if (client.isConnected()) throw new CommandRegistry.InputException("Set the presence UDP port before connecting.");
            udpPort = port;
        }
    }

    private void signal() {
        if (closed.get() || !queued.compareAndSet(false, true)) return;
        try { lifecycle.execute(() -> { queued.set(false); synchronizeSender(); }); }
        catch (RejectedExecutionException stopping) { queued.set(false); }
    }

    private void synchronizeSender() {
        var current = authentication.presenceCredentials().orElse(null);
        if (closed.get()) current = null;
        if (owned != current) {
            if (sender != null) sender.close();
            sender = null; owned = current;
        }
        synchronized (client) {
            if (queryConnection != null && client.connectionId().filter(queryConnection::equals).isEmpty()) queryId = queryConnection = null;
        }
        if (current == null) return;
        if (authentication.isLoggingOut()) { if (sender != null) sender.stop(); return; }
        final var captured = current;
        try {
            if (sender == null) sender = new HeartbeatSender(new InetSocketAddress(ProtocolLimits.BIND_ADDRESS, udpPort),
                    current.session().clientId(), current.presenceToken(),
                    () -> !closed.get() && authentication.isCurrent(captured) && !authentication.isLoggingOut());
            if (!closed.get() && authentication.isCurrent(captured) && !authentication.isLoggingOut()) sender.start();
        } catch (Exception failure) {
            if (output != null) output.offerImportant("Presence heartbeat could not start; reconnect to retry.", current.session().connectionId());
            publishEvent(new PresenceEvent.HeartbeatStartFailed(current.session().connectionId(),
                    "Presence heartbeat could not start; reconnect to retry."));
        }
    }

    public ClientDispatcher.Registration onPresenceEvent(java.util.function.Consumer<PresenceEvent> listener) {
        Objects.requireNonNull(listener);
        synchronized (client) { eventListeners.add(listener); }
        return () -> { synchronized (client) { eventListeners.remove(listener); } };
    }

    private void publishEvent(PresenceEvent event) {
        List<java.util.function.Consumer<PresenceEvent>> snapshot;
        synchronized (client) { snapshot = List.copyOf(eventListeners); }
        for (java.util.function.Consumer<PresenceEvent> listener : snapshot) {
            try { listener.accept(event); } catch (RuntimeException ignored) { }
        }
    }

    public boolean isSendingHeartbeats() { HeartbeatSender active = sender; return active != null && active.isRunning(); }

    public void query() {
        synchronized (client) {
            if (closed.get() || authentication.presenceCredentials().isEmpty() || authentication.isLoggingOut())
                throw new CommandRegistry.InputException("Log in before querying presence.");
            long now = System.nanoTime();
            if (queryId != null && now - queryStarted < TimeUnit.SECONDS.toNanos(5))
                throw new CommandRegistry.InputException("Presence query is pending; retry after five seconds.");
            UUID id = UUID.randomUUID();
            try {
                if (client.sendJson(new MessageEnvelope(ProtocolLimits.VERSION, MessageType.PRESENCE_QUERY,
                        id, null, new MessageBody.Empty())) != EnqueueResult.ACCEPTED)
                    throw new CommandRegistry.InputException("Presence query could not be queued.");
                queryId = id; queryConnection = client.connectionId().orElseThrow(); queryStarted = now;
            } catch (ProtocolException invalid) { throw new CommandRegistry.InputException("Presence query failed."); }
        }
    }

    private boolean authenticated() { return !closed.get() && authentication.presenceCredentials().isPresent(); }
    private void snapshot(MessageEnvelope message) {
        List<String> lines = new ArrayList<>();
        List<MessageBody.PresenceEntry> entries;
        UUID connection;
        synchronized (client) {
            if (!authenticated() || queryId == null || !queryId.equals(message.replyTo())
                    || client.connectionId().filter(queryConnection::equals).isEmpty()
                    || !(message.body() instanceof MessageBody.PresenceSnapshot snapshot)) return;
            connection = queryConnection; queryId = queryConnection = null;
            entries = snapshot.clients();
            lines.add("Presence snapshot:");
            entries.forEach(entry -> lines.add(entry.username() + " [" + entry.clientId() + "]: " + entry.state()));
            if (entries.isEmpty()) lines.add("No active users.");
        }
        if (client.connectionId().filter(connection::equals).isPresent()) {
            if (output != null) lines.forEach(line -> output.offerImportant(line, connection));
            publishEvent(new PresenceEvent.SnapshotReceived(connection, entries));
        }
    }
    private void update(MessageEnvelope message) {
        String text; UUID generation, clientId; MessageBody.PresenceEntry entry;
        synchronized (client) {
            if (!authenticated() || !(message.body() instanceof MessageBody.PresenceUpdate update)) return;
            generation = client.connectionId().orElse(null);
            if (generation == null) return;
            clientId = update.clientId();
            entry = new MessageBody.PresenceEntry(update.clientId(), update.username(), update.state());
            text = "Presence: " + update.username() + " [" + update.clientId() + "] is " + update.state() + ".";
        }
        if (output != null) output.offerPresence(generation, clientId, text);
        publishEvent(new PresenceEvent.UserUpdated(generation, entry));
    }
    private void error(MessageEnvelope message) {
        String text; UUID generation; ErrorCode code;
        synchronized (client) {
            if (closed.get() || queryId == null || !queryId.equals(message.replyTo())
                    || client.connectionId().filter(queryConnection::equals).isEmpty()
                    || !(message.body() instanceof MessageBody.Error error)) return;
            generation = queryConnection;
            code = error.code();
            queryId = queryConnection = null; text = "Presence query failed (" + code + ").";
        }
        if (output != null) output.offerImportant(text, generation);
        publishEvent(new PresenceEvent.QueryFailed(generation, code));
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        synchronized (client) { eventListeners.clear(); }
        registrations.forEach(ClientDispatcher.Registration::close);
        commands.unregister("/presence", queryCommand);
        synchronized (client) { queryId = queryConnection = null; }
        lifecycle.shutdownNow();
        if (Thread.currentThread() != worker && !client.isDispatchThread()) {
            try { lifecycle.awaitTermination(2, TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }
    }
}
