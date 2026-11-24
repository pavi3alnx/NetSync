package com.netsync.app;

import com.netsync.client.NetSyncClient;
import com.netsync.client.TcpClient;
import com.netsync.common.*;
import com.netsync.presence.HeartbeatTracker;
import com.netsync.security.CredentialStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class ChatProductionIntegrationTest {
    @Test
    void productionClientsAuthenticateRoutePrivateChatAndRejectUnauthenticatedRequests() throws Exception {
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob", "carol"), 0);
        CredentialStore credentials = DemoAccounts.provision(config, name -> ("disposable-" + name).toCharArray());
        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials)) {
            app.start();
            try (Client alice = client(app, "alice"); Client bob = client(app, "bob");
                 Client carol = client(app, "carol"); Client stranger = client(app, null)) {
                connectAndLogin(alice, "alice", app);
                connectAndLogin(bob, "bob", app);
                connectAndLogin(carol, "carol", app);
                connectAndLogin(stranger, null, app);
                await(() -> app.decodedHeartbeatsForTest() > 0);

                alice.client.execute("/presence");
                await(() -> alice.text().contains("bob [") && alice.text().contains("carol ["));
                UUID bobId = clientIdFrom(alice.text(), "bob");
                UUID aliceId = clientIdFrom(alice.text(), "alice");

                alice.client.execute("/chat " + bobId + " hello Bob from production client");
                await(() -> bob.text().contains("hello Bob from production client"));
                await(() -> alice.text().contains("Queued for recipient " + bobId));
                assertTrue(bob.text().contains("From " + aliceId));
                assertTrue(alice.text().contains("does not confirm that it was read"));
                assertFalse(carol.text().contains("hello Bob from production client"));

                bob.client.execute("/chat " + aliceId + " reply from Bob");
                await(() -> alice.text().contains("reply from Bob"));
                await(() -> bob.text().contains("Queued for recipient " + aliceId));
                assertFalse(carol.text().contains("reply from Bob"));

                bob.close();
                alice.client.execute("/chat " + bobId + " disconnected recipient");
                await(() -> alice.text().contains("RECIPIENT_UNAVAILABLE"));

                UUID missing = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
                alice.client.execute("/chat " + missing + " unavailable test");
                await(() -> alice.text().contains("RECIPIENT_UNAVAILABLE"));

                CountDownLatch rejected = new CountDownLatch(1);
                UUID requestId = UUID.randomUUID();
                stranger.client.transport().dispatcher().register(MessageType.ERROR, message -> {
                    if (requestId.equals(message.replyTo()) && message.body() instanceof MessageBody.Error error
                            && error.code() == ErrorCode.UNAUTHENTICATED) rejected.countDown();
                });
                assertEquals(EnqueueResult.ACCEPTED, stranger.client.transport().sendJson(new MessageEnvelope(
                        ProtocolLimits.VERSION, MessageType.CHAT_SEND, requestId, null,
                        new MessageBody.ChatSend(bobId, "must not route"))));
                assertTrue(rejected.await(3, TimeUnit.SECONDS));
                assertFalse(bob.text().contains("must not route"));
            }
        }
    }

    @Test
    void chatRemainsAuthorizedAfterPresenceExpiryAndLogoutReconnectClearsIdentity() throws Exception {
        AtomicLong clock = new AtomicLong();
        DemoServerConfig config = new DemoServerConfig(true, 0, List.of("alice", "bob"), 0);
        CredentialStore credentials = DemoAccounts.provision(config, name -> ("disposable-" + name).toCharArray());
        try (NetSyncServerApplication app = new NetSyncServerApplication(config, credentials,
                com.netsync.server.NioServer::new, com.netsync.transfer.TransferWorkerPool::new, clock::get)) {
            app.start();
            try (Client alice = client(app, "alice"); Client bob = client(app, "bob")) {
                connectAndLogin(alice, "alice", app); connectAndLogin(bob, "bob", app);
                alice.client.execute("/presence");
                await(() -> alice.text().contains("bob ["));
                UUID bobId = clientIdFrom(alice.text(), "bob");

                alice.client.presence().close(); // stop heartbeats without logging out TCP
                clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
                app.presenceTracker().checkTimeouts();
                assertEquals(PresenceState.OFFLINE, app.presenceTracker().getPresenceState(
                        clientIdFrom(alice.text(), "alice")).orElseThrow());

                alice.client.execute("/chat " + bobId + " still authenticated over TCP");
                await(() -> bob.text().contains("still authenticated over TCP"));
                await(() -> alice.text().contains("Queued for recipient " + bobId));
                assertTrue(app.sessions().findByClientId(clientIdFrom(alice.text(), "alice")).isPresent());

                UUID oldGeneration = alice.client.transport().connectionId().orElseThrow();
                alice.client.execute("/logout");
                await(() -> alice.text().contains("Logged out."));
                assertTrue(alice.client.authentication().presenceCredentials().isEmpty());
                alice.client.execute("/connect 127.0.0.1 " + app.port() + " " + app.udpPort());
                alice.client.execute("/login alice");
                await(() -> alice.client.authentication().presenceCredentials().isPresent());
                assertNotEquals(oldGeneration, alice.client.authentication().presenceCredentials()
                        .orElseThrow().session().connectionId());
                assertNotNull(alice.client.authentication().presenceCredentials().orElseThrow().presenceToken());
                alice.client.execute("/chat " + bobId + " fresh session chat");
                await(() -> bob.text().contains("fresh session chat"));
            }
        }
    }

    private static Client client(NetSyncServerApplication app, String username) {
        StringWriter text = new StringWriter();
        NetSyncClient cli = new NetSyncClient(new TcpClient(), new PrintWriter(text, true),
                () -> username == null ? new char[0] : ("disposable-" + username).toCharArray(), app.udpPort());
        return new Client(cli, text);
    }

    private static void connectAndLogin(Client client, String username, NetSyncServerApplication app) {
        client.client.execute("/connect 127.0.0.1 " + app.port() + " " + app.udpPort());
        if (username != null) {
            client.client.execute("/login " + username);
            await(() -> client.text().contains("Logged in as " + username));
        }
    }

    private static UUID clientIdFrom(String text, String username) {
        Matcher matcher = Pattern.compile(Pattern.quote(username) + " \\[([0-9a-fA-F-]{36})\\]").matcher(text);
        if (!matcher.find()) {
            matcher = Pattern.compile("Logged in as " + Pattern.quote(username) + " \\(client ID: ([0-9a-fA-F-]{36})\\)").matcher(text);
        }
        assertTrue(matcher.find(0), "No client id found for " + username + " in output: " + text);
        return UUID.fromString(matcher.group(1));
    }

    private static void await(java.util.function.BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            try { Thread.sleep(5); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); fail("Interrupted"); }
        }
        assertTrue(condition.getAsBoolean(), "Condition did not become true before timeout");
    }

    private record Client(NetSyncClient client, StringWriter writer) implements AutoCloseable {
        String text() { synchronized (writer) { return writer.toString(); } }
        @Override public void close() { client.close(); }
    }
}
