package com.netsync.client;

import com.netsync.common.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(12)
class ClientAuthenticationAdapterTest {
    static final String TOKEN = Base64.getEncoder().encodeToString(new byte[32]);
    static final FrameCodec CODEC = new FrameCodec(new MessageCodec());

    static void send(Socket socket, MessageEnvelope message) throws Exception {
        ByteBuffer buffer = CODEC.encode(message);
        socket.getOutputStream().write(buffer.array(), buffer.position(), buffer.remaining());
    }
    static MessageEnvelope receive(Socket socket) throws Exception {
        socket.setSoTimeout(3000);
        DataInputStream in = new DataInputStream(socket.getInputStream());
        int size = CODEC.decodeLength(ByteBuffer.wrap(in.readNBytes(4)));
        return (MessageEnvelope) CODEC.decodePayload(ByteBuffer.wrap(in.readNBytes(size)));
    }
    static void await(Callable<Boolean> condition) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.call() && System.nanoTime() < until) Thread.sleep(2);
        assertTrue(condition.call());
    }
    static MessageEnvelope response(UUID replyTo, String username) {
        return new MessageEnvelope(1, MessageType.LOGIN_RESPONSE, UUID.randomUUID(), replyTo,
                new MessageBody.LoginResponse(UUID.randomUUID(), username, TOKEN, 5, 15));
    }
    static MessageEnvelope error(UUID replyTo, ErrorCode code, String text) {
        return new MessageEnvelope(1, MessageType.ERROR, UUID.randomUUID(), replyTo,
                new MessageBody.Error(code, text, null));
    }
    static MessageEnvelope ack(UUID replyTo) {
        return new MessageEnvelope(1, MessageType.ACK, UUID.randomUUID(), replyTo,
                new MessageBody.Ack(MessageType.LOGOUT_REQUEST, AckStatus.ACCEPTED, null));
    }

    static final class Peer implements AutoCloseable {
        final ServerSocket listener = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        final TcpClient client = new TcpClient();
        final CommandRegistry commands = new CommandRegistry();
        final List<String> output = new CopyOnWriteArrayList<>();
        final AtomicLong clock = new AtomicLong(123);
        final AtomicReference<char[]> lastPassword = new AtomicReference<>();
        final ClientAuthenticationAdapter adapter;
        Socket socket;
        Peer() throws Exception { this(null, c -> {}); }
        Peer(ClientAuthenticationAdapter.PasswordInput input) throws Exception { this(input, c -> {}); }
        Peer(ClientAuthenticationAdapter.PasswordInput input, Consumer<TcpClient> before) throws Exception {
            before.accept(client);
            adapter = new ClientAuthenticationAdapter(client, commands, output::add,
                    input != null ? input : () -> { char[] chars = "disposable-only".toCharArray(); lastPassword.set(chars); return chars; },
                    clock::get, Duration.ofSeconds(10));
            client.connect("127.0.0.1", listener.getLocalPort()); socket = listener.accept();
        }
        MessageEnvelope login() throws Exception { commands.execute("/login alice"); return receive(socket); }
        void authenticate() throws Exception {
            MessageEnvelope request = login(); send(socket, response(request.messageId(), "alice"));
            await(() -> adapter.presenceCredentials().isPresent());
        }
        void reconnect() throws Exception {
            client.disconnect(); socket.close();
            client.connect("127.0.0.1", listener.getLocalPort()); socket = listener.accept();
        }
        public void close() throws Exception { adapter.close(); client.close(); socket.close(); listener.close(); }
    }

    @Test void successfulResponseUsesServerIdentityAndClearsPasswordBuffer() throws Exception {
        try (Peer p = new Peer()) {
            UUID generation = p.client.connectionId().orElseThrow();
            MessageEnvelope request = p.login();
            assertEquals(MessageType.LOGIN_REQUEST, request.type());
            assertEquals("alice", ((MessageBody.LoginRequest) request.body()).username());
            assertArrayEquals(new char[p.lastPassword.get().length], p.lastPassword.get());
            MessageEnvelope reply = response(request.messageId(), "alice");
            send(p.socket, reply);
            await(() -> p.adapter.presenceCredentials().isPresent());
            var credentials = p.adapter.presenceCredentials().orElseThrow();
            assertEquals(((MessageBody.LoginResponse) reply.body()).clientId(), credentials.session().clientId());
            assertEquals(generation, credentials.session().connectionId());
            assertEquals(Optional.of(credentials.session()), p.client.session());
            assertEquals(TOKEN, credentials.presenceToken());
            assertEquals(5, credentials.heartbeatIntervalSeconds());
            assertEquals(15, credentials.presenceTimeoutSeconds());
            assertTrue(p.adapter.isCurrent(credentials));
            assertFalse(credentials.toString().contains(TOKEN));
            assertFalse(p.output.toString().contains(TOKEN));
            assertFalse(p.output.toString().contains("disposable-only"));
        }
    }

    @Test void unrelatedResponsesCannotCompleteOrFailPendingLogin() throws Exception {
        try (Peer p = new Peer()) {
            MessageEnvelope request = p.login();
            send(p.socket, response(UUID.randomUUID(), "alice"));
            send(p.socket, error(UUID.randomUUID(), ErrorCode.AUTH_FAILED, "secret-server-text"));
            CompletableFuture<Void> processed = new CompletableFuture<>();
            p.client.dispatcher().register(MessageType.ACK, message -> processed.complete(null));
            send(p.socket, ack(UUID.randomUUID())); processed.get(3, TimeUnit.SECONDS);
            assertTrue(p.client.session().isEmpty()); assertTrue(p.client.isConnected());
            assertThrows(CommandRegistry.InputException.class, p.adapter::logout);
            send(p.socket, response(request.messageId(), "alice"));
            await(() -> p.client.session().isPresent());
        }
    }

    @Test void identityMismatchIsRejectedWithoutLoggingResponseSecrets() throws Exception {
        try (Peer p = new Peer()) {
            MessageEnvelope request = p.login();
            send(p.socket, response(request.messageId(), "bob"));
            await(() -> !p.client.isConnected());
            assertTrue(p.adapter.presenceCredentials().isEmpty());
            assertTrue(p.client.session().isEmpty());
            assertFalse(p.output.toString().contains(TOKEN));
        }
    }

    @Test void correlatedFailureAfterOrderlyEofIsStillProcessedAndSanitized() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        try (Peer p = new Peer(null, client -> client.dispatcher().register(MessageType.ERROR, message -> {
            entered.countDown();
            try { assertTrue(release.await(3, TimeUnit.SECONDS)); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }))) {
            try {
                MessageEnvelope request = p.login();
                send(p.socket, error(request.messageId(), ErrorCode.AUTH_FAILED, "secret-password-token"));
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                p.socket.close(); await(() -> !p.client.isConnected());
            } finally { release.countDown(); }
            await(() -> p.output.stream().anyMatch(s -> s.contains("AUTH_FAILED")));
            assertTrue(p.client.session().isEmpty()); assertTrue(p.adapter.presenceCredentials().isEmpty());
            assertFalse(p.output.toString().contains("secret-password-token"));
        }
    }

    @Test void duplicateCommandsCannotReplacePendingRequest() throws Exception {
        AtomicInteger prompts = new AtomicInteger();
        try (Peer p = new Peer(() -> { prompts.incrementAndGet(); return "disposable".toCharArray(); })) {
            MessageEnvelope first = p.login();
            assertThrows(CommandRegistry.InputException.class, () -> p.commands.execute("/login bob"));
            assertThrows(CommandRegistry.InputException.class, () -> p.commands.execute("/logout"));
            assertEquals(1, prompts.get());
            send(p.socket, response(first.messageId(), "alice")); await(() -> p.client.session().isPresent());
            p.commands.execute("/logout");
            assertEquals(MessageType.LOGOUT_REQUEST, receive(p.socket).type());
        }
    }

    @Test void logoutRetainsCredentialsUntilCorrelatedAcceptedAck() throws Exception {
        try (Peer p = new Peer()) {
            p.authenticate(); var credentials = p.adapter.presenceCredentials().orElseThrow();
            p.commands.execute("/logout"); MessageEnvelope logout = receive(p.socket);
            assertEquals(new MessageBody.Empty(), logout.body());
            send(p.socket, ack(UUID.randomUUID()));
            assertTrue(p.adapter.isCurrent(credentials));
            send(p.socket, ack(logout.messageId()));
            await(() -> !p.client.isConnected());
            assertTrue(p.client.session().isEmpty()); assertFalse(p.adapter.isCurrent(credentials));
            assertTrue(p.adapter.presenceCredentials().isEmpty());
            await(() -> p.output.contains("Logged out."));
        }
    }

    @Test void deniedLogoutDoesNotPrematurelyClearIdentity() throws Exception {
        try (Peer p = new Peer()) {
            p.authenticate();
            p.adapter.logout(); MessageEnvelope request = receive(p.socket);
            send(p.socket, error(request.messageId(), ErrorCode.FORBIDDEN, "secret-error"));
            await(() -> p.output.stream().anyMatch(s -> s.contains("Logout failed")));
            assertTrue(p.client.session().isPresent()); assertTrue(p.adapter.presenceCredentials().isPresent());
            p.adapter.logout(); send(p.socket, ack(receive(p.socket).messageId()));
            await(() -> !p.client.isConnected());
        }
    }

    @Test void abruptDisconnectMakesTokenImmediatelyUnavailable() throws Exception {
        try (Peer p = new Peer()) {
            p.authenticate(); var old = p.adapter.presenceCredentials().orElseThrow();
            p.socket.setSoLinger(true, 0); p.socket.close();
            await(() -> !p.client.isConnected());
            assertTrue(p.adapter.presenceCredentials().isEmpty()); assertFalse(p.adapter.isCurrent(old));
            assertTrue(p.client.session().isEmpty());
        }
    }

    @Test void staleResponseAndOldGenerationDisconnectCannotAffectReconnect() throws Exception {
        try (Peer p = new Peer()) {
            MessageEnvelope old = p.login(); UUID generation = p.client.connectionId().orElseThrow();
            p.reconnect(); p.adapter.login("bob"); MessageEnvelope fresh = receive(p.socket);
            p.client.disconnect(generation); assertTrue(p.client.isConnected());
            send(p.socket, response(old.messageId(), "alice"));
            send(p.socket, response(fresh.messageId(), "bob"));
            await(() -> p.client.session().isPresent());
            assertEquals("bob", p.client.session().orElseThrow().username());
            assertNotEquals(generation, p.client.session().orElseThrow().connectionId());
        }
    }

    @Test void alreadyRunningOldDispatchCallbackCannotAuthenticateNewConnection() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        try (Peer p = new Peer(null, client -> client.dispatcher().register(MessageType.LOGIN_RESPONSE, message -> {
            if (calls.getAndIncrement() == 0) {
                entered.countDown();
                try { release.await(3, TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }))) {
            try {
                MessageEnvelope old = p.login(); send(p.socket, response(old.messageId(), "alice"));
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                p.reconnect(); p.adapter.login("bob"); MessageEnvelope fresh = receive(p.socket);
                release.countDown(); send(p.socket, response(fresh.messageId(), "bob"));
                await(() -> p.client.session().isPresent());
                assertEquals("bob", p.client.session().orElseThrow().username());
            } finally { release.countDown(); }
        }
    }

    @Test void timeoutClosesUncertainLoginWithoutRetryOrRealTenSecondSleep() throws Exception {
        try (Peer p = new Peer()) {
            p.login(); p.clock.addAndGet(TimeUnit.SECONDS.toNanos(10)); p.adapter.checkTimeouts();
            assertFalse(p.client.isConnected()); assertTrue(p.client.session().isEmpty());
            assertTrue(p.adapter.presenceCredentials().isEmpty());
            assertEquals(-1, p.socket.getInputStream().read());
            assertTrue(p.output.stream().anyMatch(s -> s.contains("timed out")));
        }
    }

    @Test void responsePastDeadlineCannotBeatTimerAndCreateSession() throws Exception {
        try (Peer p = new Peer()) {
            MessageEnvelope request = p.login();
            p.clock.addAndGet(TimeUnit.SECONDS.toNanos(11));
            try { send(p.socket, response(request.messageId(), "alice")); } catch (IOException alreadyClosed) { }
            await(() -> !p.client.isConnected()); assertTrue(p.client.session().isEmpty());
        }
    }

    @Test void logoutTimeoutClearsIdentityAndClosesUncertainConnection() throws Exception {
        try (Peer p = new Peer()) {
            p.authenticate(); p.adapter.logout(); receive(p.socket);
            p.clock.addAndGet(TimeUnit.SECONDS.toNanos(10)); p.adapter.checkTimeouts();
            assertFalse(p.client.isConnected()); assertTrue(p.client.session().isEmpty());
            assertTrue(p.adapter.presenceCredentials().isEmpty());
        }
    }

    @Test void hiddenPromptDoesNotBlockDispatchAndLatePasswordIsWiped() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        char[] password = "disposable-only".toCharArray();
        try (Peer p = new Peer(() -> {
            entered.countDown();
            try { release.await(3, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return password;
        }); var executor = Executors.newSingleThreadExecutor()) {
            Future<?> login = executor.submit(() -> assertThrows(CommandRegistry.InputException.class, () -> p.adapter.login("alice")));
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                CompletableFuture<Void> dispatched = new CompletableFuture<>();
                p.client.dispatcher().register(MessageType.ACK, message -> dispatched.complete(null));
                send(p.socket, ack(UUID.randomUUID())); dispatched.get(3, TimeUnit.SECONDS);
                p.clock.addAndGet(TimeUnit.SECONDS.toNanos(10)); p.adapter.checkTimeouts();
                assertFalse(p.client.isConnected());
            } finally { release.countDown(); }
            login.get(3, TimeUnit.SECONDS); assertArrayEquals(new char[password.length], password);
        }
    }

    @Test void loginFromDispatchThreadIsRejectedBeforePasswordInput() throws Exception {
        AtomicInteger prompts = new AtomicInteger();
        try (Peer p = new Peer(() -> { prompts.incrementAndGet(); return new char[]{'x'}; })) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            p.client.dispatcher().register(MessageType.ACK, message -> {
                try { assertThrows(CommandRegistry.InputException.class, () -> p.adapter.login("alice")); result.complete(null); }
                catch (Throwable failure) { result.completeExceptionally(failure); }
            });
            send(p.socket, ack(UUID.randomUUID())); result.get(3, TimeUnit.SECONDS);
            assertEquals(0, prompts.get());
        }
    }

    @Test void passwordCancellationAndValidationPermitSafeRetryAndWipeBuffers() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        char[] empty = new char[0], oversized = "x".repeat(129).toCharArray();
        try (Peer p = new Peer(() -> switch (calls.getAndIncrement()) {
            case 0 -> null; case 1 -> empty; case 2 -> oversized; default -> "disposable".toCharArray();
        })) {
            for (int i = 0; i < 3; i++) assertThrows(CommandRegistry.InputException.class, () -> p.adapter.login("alice"));
            assertArrayEquals(new char[129], oversized);
            p.login(); assertEquals(4, calls.get());
        }
    }

    @Test void latePasswordFromPreviousConnectionCannotSendOrCloseNewAuthenticatedConnection() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger(); char[] oldPassword = "old-disposable".toCharArray();
        try (Peer p = new Peer(() -> {
            if (calls.getAndIncrement() == 0) {
                entered.countDown();
                try { release.await(3, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                return oldPassword;
            }
            return "new-disposable".toCharArray();
        }); var executor = Executors.newSingleThreadExecutor()) {
            Future<?> oldLogin = executor.submit(() -> assertThrows(CommandRegistry.InputException.class, () -> p.adapter.login("alice")));
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                p.reconnect(); p.adapter.login("bob"); MessageEnvelope fresh = receive(p.socket);
                send(p.socket, response(fresh.messageId(), "bob")); await(() -> p.client.session().isPresent());
                release.countDown(); oldLogin.get(3, TimeUnit.SECONDS);
                assertEquals("bob", p.client.session().orElseThrow().username());
                assertTrue(p.client.isConnected()); assertTrue(p.adapter.presenceCredentials().isPresent());
                assertArrayEquals(new char[oldPassword.length], oldPassword);
            } finally { release.countDown(); }
        }
    }

    @Test void unauthenticatedLogoutErrorClearsCredentialsAndClosesConnection() throws Exception {
        try (Peer p = new Peer()) {
            p.authenticate(); p.adapter.logout(); MessageEnvelope request = receive(p.socket);
            send(p.socket, error(request.messageId(), ErrorCode.UNAUTHENTICATED, "Authentication required"));
            await(() -> !p.client.isConnected());
            assertTrue(p.client.session().isEmpty()); assertTrue(p.adapter.presenceCredentials().isEmpty());
        }
    }

    @Test void passwordProviderExceptionsAreNeverExposedEvenWhenMarkedInputException() throws Exception {
        try (Peer p = new Peer(() -> { throw new CommandRegistry.InputException("secret-password"); })) {
            var error = assertThrows(CommandRegistry.InputException.class, () -> p.adapter.login("alice"));
            assertFalse(error.getMessage().contains("secret-password"));
            assertTrue(p.client.isConnected());
        }
    }

    @Test void passwordArgumentsAndInvalidUsernamesAreRejectedBeforePrompt() throws Exception {
        AtomicInteger prompts = new AtomicInteger();
        try (Peer p = new Peer(() -> { prompts.incrementAndGet(); return new char[]{'x'}; })) {
            for (String command : List.of("/login alice secret", "/login", "/login invalid-user", "/logout extra"))
                assertThrows(CommandRegistry.InputException.class, () -> p.commands.execute(command));
            assertEquals(0, prompts.get());
        }
    }

    @Test void closeClearsCredentialsDisconnectsAndUnregistersOwnedCommands() throws Exception {
        try (Peer p = new Peer()) {
            p.authenticate(); var old = p.adapter.presenceCredentials().orElseThrow();
            p.adapter.close(); p.adapter.close();
            assertFalse(p.client.isConnected()); assertTrue(p.client.session().isEmpty());
            assertTrue(p.adapter.presenceCredentials().isEmpty()); assertFalse(p.adapter.isCurrent(old));
            assertTrue(p.commands.descriptions().isEmpty());
            assertThrows(CommandRegistry.InputException.class, () -> p.adapter.login("alice"));
        }
    }

    @Test void constructorRollbackKeepsOtherOwnersCommand() throws Exception {
        try (TcpClient client = new TcpClient()) {
            CommandRegistry commands = new CommandRegistry(); AtomicInteger calls = new AtomicInteger();
            commands.register("/logout", "/logout", "Existing", args -> calls.incrementAndGet());
            assertThrows(IllegalArgumentException.class, () -> new ClientAuthenticationAdapter(client, commands,
                    s -> {}, () -> new char[]{'x'}));
            assertEquals(1, commands.descriptions().size()); commands.execute("/logout"); assertEquals(1, calls.get());
        }
    }
}
