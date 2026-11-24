package com.netsync.chat;

import com.netsync.client.*;
import com.netsync.common.*;
import com.netsync.testsupport.ControlledTcpPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(12)
class ChatClientLifecycleTest {
    private static final UUID RECIPIENT = UUID.fromString("22222222-2222-2222-2222-222222222222");

    @Test
    void explicitDisconnectClearsOutstandingChatAndSuppressesLateCompletionAndTimeout() throws Exception {
        try (ControlledTcpPeer peer = new ControlledTcpPeer(); Harness h = new Harness(peer)) {
            ControlledTcpPeer.Connection connection = h.login();
            h.commands.execute("/chat " + RECIPIENT + " disconnect pending");
            MessageEnvelope request = connection.nextRequest(MessageType.CHAT_SEND);
            assertEquals(1, h.chat.pendingRequestCountForTesting());
            UUID generation = h.client.connectionId().orElseThrow();

            h.client.disconnect();
            assertEquals(0, h.chat.pendingRequestCountForTesting());
            h.clock.set(TimeUnit.MINUTES.toNanos(2));
            h.chat.expirePendingForTesting();
            h.chat.onAck(ack(request)); // delayed callback after explicit cancellation
            assertFalse(h.output.stream().anyMatch(text -> text.contains("Queued for recipient")
                    || text.contains("outcome is unknown")));
            assertTrue(h.client.connectionId().isEmpty());
            assertTrue(h.client.isGenerationCurrent(generation));
        }
    }

    @Test
    void nonOrderlyTransportFailureClearsOutstandingChatState() throws Exception {
        try (ControlledTcpPeer peer = new ControlledTcpPeer(); Harness h = new Harness(peer)) {
            ControlledTcpPeer.Connection connection = h.login();
            h.commands.execute("/chat " + RECIPIENT + " transport failure pending");
            connection.nextRequest(MessageType.CHAT_SEND);
            assertEquals(1, h.chat.pendingRequestCountForTesting());

            connection.sendInvalidFrame();
            await(() -> h.client.failure() == TcpClient.Failure.INVALID_FRAME);
            assertEquals(0, h.chat.pendingRequestCountForTesting());
            assertTrue(h.authentication.presenceCredentials().isEmpty());
            h.clock.set(TimeUnit.MINUTES.toNanos(2));
            h.chat.expirePendingForTesting();
            assertFalse(h.output.stream().anyMatch(text -> text.contains("outcome is unknown")));
        }
    }

    @Test
    void logoutResponsesRemainAuthenticationOwnedAndClearPendingChat() throws Exception {
        try (ControlledTcpPeer peer = new ControlledTcpPeer(); Harness h = new Harness(peer)) {
            ControlledTcpPeer.Connection connection = h.login();
            h.commands.execute("/chat " + RECIPIENT + " logout pending");
            MessageEnvelope chatRequest = connection.nextRequest(MessageType.CHAT_SEND);
            assertEquals(1, h.chat.pendingRequestCountForTesting());

            h.authentication.logout();
            MessageEnvelope logout = connection.nextRequest(MessageType.LOGOUT_REQUEST);
            assertEquals(0, h.chat.pendingRequestCountForTesting());
            connection.send(new MessageEnvelope(1, MessageType.ERROR, UUID.randomUUID(), logout.messageId(),
                    new MessageBody.Error(ErrorCode.BUSY, "safe", null)));
            await(() -> h.output.stream().anyMatch(text -> text.startsWith("Logout failed")));
            assertTrue(h.authentication.presenceCredentials().isPresent());
            assertFalse(h.output.stream().anyMatch(text -> text.contains("Delivery failed for recipient " + RECIPIENT)));

            h.authentication.logout();
            MessageEnvelope acceptedLogout = connection.nextRequest(MessageType.LOGOUT_REQUEST);
            connection.acknowledge(acceptedLogout, MessageType.LOGOUT_REQUEST, AckStatus.ACCEPTED);
            await(() -> h.client.state() == TcpClient.State.DISCONNECTED);
            assertTrue(h.authentication.presenceCredentials().isEmpty());
            assertEquals(0, h.chat.pendingRequestCountForTesting());
            h.chat.onAck(ack(chatRequest));
            assertFalse(h.output.stream().anyMatch(text -> text.contains("Queued for recipient")));
        }
    }

    @Test
    void completeChatAckBeforeOrderlyEofIsDispatchedBeforeConnectionCleanup() throws Exception {
        try (ControlledTcpPeer peer = new ControlledTcpPeer(); Harness h = new Harness(peer)) {
            ControlledTcpPeer.Connection connection = h.login();
            h.commands.execute("/chat " + RECIPIENT + " eof pending");
            MessageEnvelope request = connection.nextRequest(MessageType.CHAT_SEND);
            connection.acknowledge(request, MessageType.CHAT_SEND, AckStatus.QUEUED);
            connection.close(); // complete ACK bytes precede orderly EOF on the same TCP stream

            await(() -> h.output.stream().anyMatch(text -> text.contains("Queued for recipient " + RECIPIENT)));
            assertEquals(0, h.chat.pendingRequestCountForTesting());
        }
    }

    @Test
    void adapterCloseClearsPendingStopsTimerAndIsIdempotent() throws Exception {
        try (ControlledTcpPeer peer = new ControlledTcpPeer(); Harness h = new Harness(peer)) {
            ControlledTcpPeer.Connection connection = h.login();
            h.commands.execute("/chat " + RECIPIENT + " close pending");
            connection.nextRequest(MessageType.CHAT_SEND);
            assertEquals(1, h.chat.pendingRequestCountForTesting());

            h.chat.close();
            h.chat.close();
            assertEquals(0, h.chat.pendingRequestCountForTesting());
            assertTrue(h.chat.timerShutdownForTesting());
            assertTrue(h.chat.awaitTimerTerminationForTesting(2, TimeUnit.SECONDS));
            assertThrows(CommandRegistry.InputException.class,
                    () -> h.commands.execute("/chat " + RECIPIENT + " after close"));
            h.clock.set(TimeUnit.MINUTES.toNanos(2));
            h.chat.expirePendingForTesting();
            assertFalse(h.output.stream().anyMatch(text -> text.contains("outcome is unknown")));
        }
    }

    @Test
    void repeatedAdapterCreationAndCloseTerminatesEveryOwnedTimer() throws Exception {
        List<Thread> ownedTimerThreads = new java.util.ArrayList<>();
        for (int i = 0; i < 12; i++) {
            TcpClient client = new TcpClient();
            CommandRegistry commands = new CommandRegistry();
            List<String> output = new CopyOnWriteArrayList<>();
            ClientAuthenticationAdapter auth = new ClientAuthenticationAdapter(client, commands,
                    output::add, () -> "disposable-password".toCharArray());
            ChatClientAdapter chat = new ChatClientAdapter(client, auth, commands,
                    (text, generation, activeOnly) -> output.add(text));
            chat.close(); auth.close(); client.close();
            assertTrue(chat.timerShutdownForTesting());
            assertTrue(chat.awaitTimerTerminationForTesting(2, TimeUnit.SECONDS));
            Thread timerThread = chat.timerThreadForTesting();
            if (timerThread != null) ownedTimerThreads.add(timerThread);
        }
        await(() -> ownedTimerThreads.stream().noneMatch(Thread::isAlive));
    }

    @Test
    void closeReturnsWithinBoundWhenTimeoutOutputConsumerIsBlocked() throws Exception {
        ClientOutbound outbound = new ClientOutbound() {
            @Override public EnqueueResult send(OutboundFrame frame) { return EnqueueResult.ACCEPTED; }
            @Override public boolean isConnected() { return true; }
        };
        ClientDispatcher dispatcher = new ClientDispatcher();
        CommandRegistry commands = new CommandRegistry();
        AtomicLong clock = new AtomicLong();
        java.util.concurrent.CountDownLatch timeoutEntered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        ChatClientAdapter adapter = new ChatClientAdapter(outbound, dispatcher, commands, text -> {
            if (text.contains("outcome is unknown")) {
                timeoutEntered.countDown();
                while (release.getCount() > 0) {
                    try { release.await(); }
                    catch (InterruptedException ignored) { /* Deliberately uncooperative output sink. */ }
                }
            }
        }, clock::get, Duration.ofSeconds(1), 2);
        try {
            commands.execute("/chat " + RECIPIENT + " blocking timeout sink");
            clock.set(TimeUnit.SECONDS.toNanos(1));
            assertTrue(timeoutEntered.await(2, TimeUnit.SECONDS));
            long started = System.nanoTime();
            adapter.close();
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2));
        } finally {
            release.countDown();
            adapter.close();
            dispatcher.close();
        }
        assertTrue(adapter.awaitTimerTerminationForTesting(2, TimeUnit.SECONDS));
    }

    private static MessageEnvelope ack(MessageEnvelope request) {
        return new MessageEnvelope(1, MessageType.ACK, UUID.randomUUID(), request.messageId(),
                new MessageBody.Ack(MessageType.CHAT_SEND, AckStatus.QUEUED, null));
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.yield();
        assertTrue(condition.getAsBoolean(), "Condition not reached before deadline");
    }

    private static final class Harness implements AutoCloseable {
        final TcpClient client = new TcpClient();
        final CommandRegistry commands = new CommandRegistry();
        final List<String> output = new CopyOnWriteArrayList<>();
        final AtomicLong clock = new AtomicLong();
        final ClientAuthenticationAdapter authentication = new ClientAuthenticationAdapter(client, commands,
                output::add, () -> "disposable-alice".toCharArray());
        final ChatClientAdapter chat = new ChatClientAdapter(client, authentication, commands,
                (text, generation, activeOnly) -> output.add(text), clock::get, Duration.ofSeconds(30), 128);
        final ControlledTcpPeer peer;
        Harness(ControlledTcpPeer peer) { this.peer = peer; }

        ControlledTcpPeer.Connection login() throws Exception {
            client.connect("127.0.0.1", peer.port());
            ControlledTcpPeer.Connection connection = peer.nextConnection();
            authentication.login("alice");
            await(() -> authentication.presenceCredentials().isPresent());
            return connection;
        }

        @Override public void close() {
            chat.close(); authentication.close(); client.close();
        }
    }
}
