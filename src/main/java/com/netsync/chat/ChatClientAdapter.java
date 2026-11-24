package com.netsync.chat;

import com.netsync.client.ClientDispatcher;
import com.netsync.client.ClientOutbound;
import com.netsync.client.CommandRegistry;
import com.netsync.client.TcpClient;
import com.netsync.common.AckStatus;
import com.netsync.common.ErrorCode;
import com.netsync.common.MessageBody;
import com.netsync.common.MessageEnvelope;
import com.netsync.common.MessageType;
import com.netsync.common.ProtocolLimits;

import java.io.Closeable;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Client-side adapter integrating TCP Chat with NetSync CLI client infrastructure
 * ({@link TcpClient}, {@link ClientDispatcher}, and {@link CommandRegistry}).
 *
 * <p>Registers the {@code /chat} command and listens for incoming {@code CHAT_DELIVER},
 * {@code ACK}, and {@code ERROR} envelopes with message correlation and timestamps.
 */
public final class ChatClientAdapter implements Closeable {

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    private final ClientOutbound outbound;
    private final CommandRegistry commands;
    private final Consumer<String> output;
    private final List<ClientDispatcher.Registration> registrations = new ArrayList<>();
    private final Map<UUID, PendingChat> pendingChats = new ConcurrentHashMap<>();
    private volatile boolean closed;

    private record PendingChat(UUID recipientId, String text, Instant sentTime) {}

    public ChatClientAdapter(TcpClient client, CommandRegistry commands, Consumer<String> output) {
        this((ClientOutbound) client, client.dispatcher(), commands, output);
    }

    public ChatClientAdapter(TcpClient client, CommandRegistry commands, PrintWriter printWriter) {
        this((ClientOutbound) client, client.dispatcher(), commands, printWriter::println);
    }

    public ChatClientAdapter(
            ClientOutbound outbound,
            ClientDispatcher dispatcher,
            CommandRegistry commands,
            Consumer<String> output) {
        this.outbound = Objects.requireNonNull(outbound, "outbound cannot be null");
        this.commands = Objects.requireNonNull(commands, "commands cannot be null");
        this.output = Objects.requireNonNull(output, "output cannot be null");

        Objects.requireNonNull(dispatcher, "dispatcher cannot be null");

        registrations.add(dispatcher.register(MessageType.CHAT_DELIVER, this::onChatDeliver));
        registrations.add(dispatcher.register(MessageType.ACK, this::onAck));
        registrations.add(dispatcher.register(MessageType.ERROR, this::onError));

        commands.register("/chat", "/chat <recipientClientId> <message>",
                "Send a direct chat message to another user", this::executeChatCommand);
    }

    private void executeChatCommand(List<String> args) {
        if (args.size() < 2) {
            throw new CommandRegistry.InputException("Usage: /chat <recipientClientId> <message>");
        }

        UUID recipientId;
        try {
            recipientId = UUID.fromString(args.get(0));
        } catch (IllegalArgumentException e) {
            throw new CommandRegistry.InputException("Invalid recipient UUID: " + args.get(0));
        }

        String messageText = String.join(" ", args.subList(1, args.size()));
        if (messageText.isBlank()) {
            throw new CommandRegistry.InputException("Message text cannot be empty");
        }

        UUID messageId = UUID.randomUUID();
        MessageEnvelope envelope = new MessageEnvelope(
                ProtocolLimits.VERSION,
                MessageType.CHAT_SEND,
                messageId,
                null,
                new MessageBody.ChatSend(recipientId, messageText)
        );

        pendingChats.put(messageId, new PendingChat(recipientId, messageText, Instant.now()));

        try {
            outbound.sendOrThrow(envelope);
            String timestamp = TIME_FORMATTER.format(Instant.now());
            output.accept("[" + timestamp + "] [Me -> " + recipientId + "]: " + messageText + " (sending...)");
        } catch (Exception e) {
            pendingChats.remove(messageId);
            output.accept("Failed to send message: " + e.getMessage());
        }
    }

    void onChatDeliver(MessageEnvelope envelope) {
        if (envelope.body() instanceof MessageBody.ChatDeliver deliver) {
            String timestamp = TIME_FORMATTER.format(Instant.now());
            output.accept("[" + timestamp + "] [From " + deliver.senderClientId() + "]: " + deliver.text());
        }
    }

    void onAck(MessageEnvelope envelope) {
        if (envelope.body() instanceof MessageBody.Ack ack) {
            if (ack.operation() == MessageType.CHAT_SEND && envelope.replyTo() != null) {
                PendingChat pending = pendingChats.remove(envelope.replyTo());
                if (pending != null) {
                    output.accept("[Delivered to server: " + pending.recipientId() + "]");
                }
            }
        }
    }

    void onError(MessageEnvelope envelope) {
        if (envelope.body() instanceof MessageBody.Error error) {
            if (envelope.replyTo() != null) {
                PendingChat pending = pendingChats.remove(envelope.replyTo());
                if (pending != null) {
                    output.accept("Delivery failed for message to " + pending.recipientId() + ": "
                            + error.code() + " - " + error.message());
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        for (ClientDispatcher.Registration registration : registrations) {
            registration.close();
        }
        registrations.clear();
        pendingChats.clear();
    }
}
