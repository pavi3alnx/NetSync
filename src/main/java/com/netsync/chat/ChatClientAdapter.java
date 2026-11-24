package com.netsync.chat;

import com.netsync.client.*;
import com.netsync.common.*;
import java.io.Closeable;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Production client adapter for private TCP chat. Output callbacks only enqueue display work. */
public final class ChatClientAdapter implements Closeable {
    public static final int MAX_PENDING_REQUESTS = 128;
    public static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(30);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    /** A generation-aware output sink; production supplies the shared bounded client queue. */
    @FunctionalInterface
    public interface Output {
        void offer(String text, UUID connectionId, boolean activeGenerationOnly);
    }

    private record PendingChat(UUID recipientId, UUID connectionId, UUID senderClientId, long startedNanos) {}

    private final ClientOutbound outbound;
    private final TcpClient transport;
    private final ClientAuthenticationAdapter authentication;
    private final CommandRegistry commands;
    private final Output output;
    private final Consumer<String> legacyOutput;
    private final BooleanSupplier authenticated;
    private final Supplier<UUID> currentConnection;
    private final LongSupplier nanoTime;
    private final long timeoutNanos;
    private final int pendingLimit;
    private final Object lock = new Object();
    private final Map<UUID, PendingChat> pendingChats = new LinkedHashMap<>();
    private final LinkedHashSet<UUID> recentlyCompleted = new LinkedHashSet<>();
    private final List<ClientDispatcher.Registration> registrations = new ArrayList<>();
    private final ScheduledExecutorService timer;
    private final CommandRegistry.Command chatCommand = this::executeChatCommand;
    private volatile UUID authenticatedGeneration;
    private volatile Thread timerThread;
    private volatile boolean closed;

    public ChatClientAdapter(TcpClient client, ClientAuthenticationAdapter authentication,
                             CommandRegistry commands, Output output) {
        this(client, authentication, commands, output, System::nanoTime, RESPONSE_TIMEOUT, MAX_PENDING_REQUESTS);
    }

    ChatClientAdapter(TcpClient client, ClientAuthenticationAdapter authentication,
                      CommandRegistry commands, Output output, LongSupplier nanoTime,
                      Duration timeout, int pendingLimit) {
        this.outbound = Objects.requireNonNull(client);
        this.transport = client;
        this.authentication = Objects.requireNonNull(authentication);
        this.commands = Objects.requireNonNull(commands);
        this.output = Objects.requireNonNull(output);
        this.legacyOutput = null;
        this.authenticated = this::hasActiveAuthentication;
        this.currentConnection = () -> client.connectionId().orElse(null);
        this.nanoTime = Objects.requireNonNull(nanoTime);
        this.timeoutNanos = Objects.requireNonNull(timeout).toNanos();
        if (timeoutNanos <= 0 || pendingLimit < 1) throw new IllegalArgumentException("Invalid chat pending limits");
        this.pendingLimit = pendingLimit;
        this.timer = newTimer();
        initialize(client.dispatcher());
        registrations.add(authentication.onStateChanged(this::authenticationChanged));
        registrations.add(client.onDisconnect(this::disconnected));
    }

    /** Compatibility constructor for standalone component use and existing clients. */
    public ChatClientAdapter(TcpClient client, CommandRegistry commands, Consumer<String> output) {
        this((ClientOutbound) client, client.dispatcher(), commands, output);
    }

    public ChatClientAdapter(TcpClient client, CommandRegistry commands, PrintWriter printWriter) {
        this((ClientOutbound) client, client.dispatcher(), commands, printWriter::println);
    }

    public ChatClientAdapter(ClientOutbound outbound, ClientDispatcher dispatcher,
                             CommandRegistry commands, Consumer<String> output) {
        this(outbound, dispatcher, commands, output, System::nanoTime, RESPONSE_TIMEOUT, MAX_PENDING_REQUESTS);
    }

    ChatClientAdapter(ClientOutbound outbound, ClientDispatcher dispatcher,
                      CommandRegistry commands, Consumer<String> output, LongSupplier nanoTime,
                      Duration timeout, int pendingLimit) {
        this.outbound = Objects.requireNonNull(outbound, "outbound cannot be null");
        this.transport = outbound instanceof TcpClient tcp ? tcp : null;
        this.authentication = null;
        this.commands = Objects.requireNonNull(commands, "commands cannot be null");
        this.legacyOutput = Objects.requireNonNull(output, "output cannot be null");
        this.output = null;
        this.authenticated = transport == null ? outbound::isConnected : () -> transport.session().isPresent();
        this.currentConnection = () -> transport == null ? null : transport.connectionId().orElse(null);
        this.nanoTime = Objects.requireNonNull(nanoTime);
        this.timeoutNanos = Objects.requireNonNull(timeout).toNanos();
        if (timeoutNanos <= 0 || pendingLimit < 1) throw new IllegalArgumentException("Invalid chat pending limits");
        this.pendingLimit = pendingLimit;
        this.timer = newTimer();
        initialize(Objects.requireNonNull(dispatcher, "dispatcher cannot be null"));
        if (transport != null) registrations.add(transport.onDisconnect(this::disconnected));
    }

    private ScheduledExecutorService newTimer() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "netsync-chat-timeout");
            thread.setDaemon(true);
            timerThread = thread;
            return thread;
        });
        executor.scheduleWithFixedDelay(this::expirePending, 100, 100, TimeUnit.MILLISECONDS);
        return executor;
    }

    private void initialize(ClientDispatcher dispatcher) {
        try {
            registrations.add(dispatcher.register(MessageType.CHAT_DELIVER, this::onChatDeliver));
            registrations.add(dispatcher.register(MessageType.ACK, this::onAck));
            registrations.add(dispatcher.register(MessageType.ERROR, this::onError));
            commands.register("/chat", "/chat <recipientClientId> <message>",
                    "Send a direct chat message to another user", chatCommand);
        } catch (RuntimeException failure) {
            close();
            throw failure;
        }
    }

    private boolean hasActiveAuthentication() {
        if (closed || authentication == null || !outbound.isConnected() || authentication.isLoggingOut()) return false;
        return authentication.presenceCredentials().map(credentials ->
                credentials.session().connectionId().equals(currentConnection.get())).orElse(false);
    }

    private void executeChatCommand(List<String> args) {
        if (args.size() < 2) throw new CommandRegistry.InputException("Usage: /chat <recipientClientId> <message>");
        UUID recipientId;
        try { recipientId = UUID.fromString(args.getFirst()); }
        catch (IllegalArgumentException invalid) { throw new CommandRegistry.InputException("Invalid recipient UUID."); }
        String messageText = String.join(" ", args.subList(1, args.size()));
        if (messageText.isBlank()) throw new CommandRegistry.InputException("Message text cannot be empty.");
        if (messageText.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > ProtocolLimits.MAX_CHAT_BYTES)
            throw new CommandRegistry.InputException("Message text exceeds 4096 UTF-8 bytes.");
        if (closed) throw new CommandRegistry.InputException("Chat adapter is closed.");

        UUID connectionId;
        UUID senderClientId;
        synchronized (lockObject()) {
            if (!authenticated.getAsBoolean())
                throw new CommandRegistry.InputException("Log in before sending chat messages.");
            if (transport != null) {
                SessionView session = authentication == null ? transport.session().orElseThrow(() ->
                        new CommandRegistry.InputException("Log in before sending chat messages."))
                        : authentication.presenceCredentials().map(ClientAuthenticationAdapter.PresenceCredentials::session)
                            .orElseThrow(() -> new CommandRegistry.InputException("Log in before sending chat messages."));
                connectionId = session.connectionId();
                senderClientId = session.clientId();
                authenticatedGeneration = connectionId;
            } else {
                connectionId = currentConnection.get();
                senderClientId = null;
            }
            synchronized (lock) {
                if (pendingChats.size() >= pendingLimit)
                    throw new CommandRegistry.InputException("Chat has too many unanswered requests; wait for a response or timeout.");
                UUID messageId = UUID.randomUUID();
                MessageEnvelope request = new MessageEnvelope(ProtocolLimits.VERSION, MessageType.CHAT_SEND,
                        messageId, null, new MessageBody.ChatSend(recipientId, messageText));
                pendingChats.put(messageId, new PendingChat(recipientId, connectionId, senderClientId, nanoTime.getAsLong()));
                EnqueueResult result;
                try { result = outbound.send(request); }
                catch (ProtocolException | RuntimeException failure) {
                    pendingChats.remove(messageId);
                    throw new CommandRegistry.InputException("Chat message could not be queued; check connection status.");
                }
                if (result != EnqueueResult.ACCEPTED) {
                    pendingChats.remove(messageId);
                    throw new CommandRegistry.InputException("Chat message could not be queued; check connection status.");
                }
                emit("[" + TIME_FORMATTER.format(Instant.now()) + "] [Me -> " + recipientId + "]: "
                        + messageText + " (sending...)", connectionId, false);
            }
        }
    }

    private Object lockObject() { return transport == null ? lock : transport; }

    void onChatDeliver(MessageEnvelope envelope) {
        if (closed || !(envelope.body() instanceof MessageBody.ChatDeliver deliver)) return;
        UUID generation = currentConnection.get();
        if (transport != null) {
            if (authentication == null) {
                SessionView session = transport.session().orElse(null);
                if (session == null || !session.connectionId().equals(generation)) return;
                authenticatedGeneration = generation;
            }
            // During orderly EOF, TcpClient drains frames received before EOF with
            // the active connection accessor already empty. Retain the authenticated
            // generation for those callbacks; dispatchable() still rejects stale links.
            if (generation == null) generation = authenticatedGeneration;
            if (generation == null || !transport.isGenerationCurrent(generation)
                    || !generation.equals(authenticatedGeneration)) return;
        }
        emit("[" + TIME_FORMATTER.format(Instant.now()) + "] [From " + deliver.senderClientId() + "]: " + deliver.text(),
                generation, false);
    }

    void onAck(MessageEnvelope envelope) {
        if (closed || !(envelope.body() instanceof MessageBody.Ack ack)
                || ack.operation() != MessageType.CHAT_SEND || ack.status() != AckStatus.QUEUED
                || ack.transferId() != null || envelope.replyTo() == null) return;
        PendingChat pending;
        synchronized (lockObject()) {
            synchronized (lock) {
                pending = pendingChats.get(envelope.replyTo());
                if (!validPending(pending)) return;
                pendingChats.remove(envelope.replyTo());
                rememberCompleted(envelope.replyTo());
            }
        }
        emit("[Chat] Queued for recipient " + pending.recipientId()
                + "; this does not confirm that it was read.", pending.connectionId(), false);
    }

    void onError(MessageEnvelope envelope) {
        if (closed || !(envelope.body() instanceof MessageBody.Error error) || envelope.replyTo() == null) return;
        PendingChat pending;
        synchronized (lockObject()) {
            synchronized (lock) {
                pending = pendingChats.get(envelope.replyTo());
                if (!validPending(pending)) return;
                pendingChats.remove(envelope.replyTo());
                rememberCompleted(envelope.replyTo());
            }
        }
        emit("[Chat] Delivery failed for recipient " + pending.recipientId() + " (" + error.code() + ").",
                pending.connectionId(), false);
    }

    /** Used by the generic shell renderer to avoid printing chat-owned responses twice. */
    public boolean ownsResponse(MessageEnvelope envelope) {
        if (envelope == null) return false;
        if (envelope.type() == MessageType.CHAT_DELIVER) return true;
        if (envelope.type() == MessageType.ACK && envelope.body() instanceof MessageBody.Ack ack)
            return ack.operation() == MessageType.CHAT_SEND;
        if (envelope.type() == MessageType.ERROR && envelope.replyTo() != null) {
            synchronized (lock) { return pendingChats.containsKey(envelope.replyTo()) || recentlyCompleted.contains(envelope.replyTo()); }
        }
        return false;
    }

    private boolean validPending(PendingChat pending) {
        if (pending == null) return false;
        if (transport == null) return true;
        boolean sameSession = authentication == null
                ? transport.session().map(session -> session.clientId().equals(pending.senderClientId())
                    && session.connectionId().equals(pending.connectionId())).orElse(false)
                : authentication.presenceCredentials().map(credentials ->
                    credentials.session().clientId().equals(pending.senderClientId())
                        && credentials.session().connectionId().equals(pending.connectionId())).orElse(false);
        return transport.isGenerationCurrent(pending.connectionId()) && pending.senderClientId() != null
                && (sameSession || !transport.isConnected());
    }

    /** Bounded tombstones keep duplicate terminal chat errors out of generic rendering. */
    private void rememberCompleted(UUID requestId) {
        recentlyCompleted.add(requestId);
        while (recentlyCompleted.size() > MAX_PENDING_REQUESTS) {
            Iterator<UUID> iterator = recentlyCompleted.iterator();
            iterator.next(); iterator.remove();
        }
    }

    private void expirePending() {
        if (closed) return;
        long now = nanoTime.getAsLong();
        List<PendingChat> expired = new ArrayList<>();
        synchronized (lockObject()) {
            synchronized (lock) {
                Iterator<Map.Entry<UUID, PendingChat>> iterator = pendingChats.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<UUID, PendingChat> entry = iterator.next();
                    PendingChat pending = entry.getValue();
                    if (now - pending.startedNanos() >= timeoutNanos) {
                        iterator.remove();
                        rememberCompleted(entry.getKey());
                        expired.add(pending);
                    }
                }
            }
        }
        for (PendingChat pending : expired) {
            if (closed || (transport != null && (!transport.isConnected()
                    || !transport.connectionId().filter(pending.connectionId()::equals).isPresent()))) continue;
            emit("[Chat] No response within 30 seconds; delivery outcome is unknown. No retry was sent.",
                    pending.connectionId(), true);
        }
    }

    /** Deterministic test seam; production also runs this from one fixed-delay timer. */
    void expirePendingForTesting() { expirePending(); }
    int pendingRequestCountForTesting() { synchronized (lock) { return pendingChats.size(); } }
    int terminalTombstoneCountForTesting() { synchronized (lock) { return recentlyCompleted.size(); } }
    boolean awaitTimerTerminationForTesting(long timeout, TimeUnit unit) throws InterruptedException {
        return timer.awaitTermination(timeout, unit);
    }
    boolean timerShutdownForTesting() { return timer.isShutdown(); }

    private void authenticationChanged() {
        if (closed || authentication == null) return;
        synchronized (lockObject()) {
            if (authentication.isLoggingOut()) {
                synchronized (lock) { pendingChats.clear(); }
                synchronized (lock) { recentlyCompleted.clear(); }
                authenticatedGeneration = null;
                return;
            }
            var credentials = authentication.presenceCredentials().orElse(null);
            if (credentials != null) {
                authenticatedGeneration = credentials.session().connectionId();
                synchronized (lock) {
                    UUID current = authenticatedGeneration;
                    pendingChats.entrySet().removeIf(entry -> !entry.getValue().connectionId().equals(current));
                    recentlyCompleted.clear();
                }
            }
        }
    }

    private void disconnected(UUID connectionId, TcpClient.Failure reason) {
        synchronized (lock) {
            if (reason == TcpClient.Failure.REMOTE_CLOSED) return; // Keep queued pre-EOF replies correlatable.
            pendingChats.entrySet().removeIf(entry -> entry.getValue().connectionId().equals(connectionId));
            recentlyCompleted.clear();
            if (connectionId.equals(authenticatedGeneration)) authenticatedGeneration = null;
        }
    }

    private void emit(String text, UUID generation, boolean activeOnly) {
        if (output != null) {
            try { output.offer(text, generation, activeOnly); }
            catch (RuntimeException ignored) { }
        } else if (legacyOutput != null) {
            try { legacyOutput.accept(text); }
            catch (RuntimeException ignored) { }
        }
    }

    @Override public void close() {
        synchronized (lockObject()) {
            if (closed) return;
            closed = true;
            synchronized (lock) { pendingChats.clear(); }
            synchronized (lock) { recentlyCompleted.clear(); }
            authenticatedGeneration = null;
        }
        timer.shutdownNow();
        if (Thread.currentThread() != timerThread) {
            try { timer.awaitTermination(1, TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }
        registrations.forEach(ClientDispatcher.Registration::close);
        commands.unregister("/chat", chatCommand);
    }
}
