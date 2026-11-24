package com.netsync.client;

import com.netsync.app.DemoServerConfig;
import com.netsync.app.NetSyncServerApplication;
import com.netsync.security.CredentialStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.io.*;
import java.util.*;
import static com.netsync.client.ClientAuthenticationAdapterTest.await;
import static org.junit.jupiter.api.Assertions.*;

/** Real production server, real TCP transport, real CLI commands and authentication adapter. */
@Timeout(15)
class ClientAuthenticationIntegrationTest {
    private static final CredentialStore ACCOUNTS = new CredentialStore(Map.of(
            "alice", "disposable-alice".toCharArray(), "bob", "disposable-bob".toCharArray()));

    private static NetSyncServerApplication server() throws Exception {
        var app = new NetSyncServerApplication(new DemoServerConfig(true, 0, List.of("alice", "bob")), ACCOUNTS);
        app.start(); return app;
    }
    private static NetSyncClient client(StringWriter output, String password) {
        return new NetSyncClient(new TcpClient(), new PrintWriter(output), () -> password.toCharArray());
    }
    private static void connect(NetSyncClient cli, NetSyncServerApplication server) {
        cli.execute("/connect 127.0.0.1 " + server.port()); assertTrue(cli.transport().isConnected());
    }

    @Test void cliLoginLogoutWorksAgainstProductionCompositionAndPreservesCoreCommands() throws Exception {
        StringWriter output = new StringWriter();
        try (var server = server(); var cli = client(output, "disposable-alice")) {
            cli.execute("/help");
            await(() -> List.of("/help", "/connect", "/disconnect", "/status", "/quit", "/login", "/logout")
                    .stream().allMatch(output.toString()::contains));
            connect(cli, server); cli.execute("/login alice");
            await(() -> cli.authentication().presenceCredentials().isPresent());
            var credentials = cli.authentication().presenceCredentials().orElseThrow();
            assertEquals("alice", credentials.session().username());
            assertTrue(server.sessions().findByClientId(credentials.session().clientId()).isPresent());
            cli.execute("/status"); cli.execute("/logout");
            await(() -> !cli.transport().isConnected());
            await(() -> server.sessions().activeSessions().isEmpty());
            assertTrue(cli.transport().session().isEmpty()); assertTrue(cli.authentication().presenceCredentials().isEmpty());
            await(() -> output.toString().contains("Logged out."));
            assertFalse(output.toString().contains(credentials.presenceToken()));
            assertFalse(output.toString().contains("disposable-alice"));
        }
    }

    @Test void invalidCredentialsFailWithoutCreatingSession() throws Exception {
        StringWriter output = new StringWriter();
        try (var server = server(); var cli = client(output, "incorrect-disposable")) {
            connect(cli, server); cli.execute("/login alice");
            await(() -> !cli.transport().isConnected());
            await(() -> output.toString().contains("AUTH_FAILED"));
            assertTrue(cli.transport().session().isEmpty()); assertTrue(cli.authentication().presenceCredentials().isEmpty());
            assertTrue(server.sessions().activeSessions().isEmpty());
            assertFalse(output.toString().contains("incorrect-disposable"));
        }
    }

    @Test void duplicateActiveUsernameDoesNotDisturbFirstClient() throws Exception {
        StringWriter outputA = new StringWriter(), outputB = new StringWriter();
        try (var server = server(); var a = client(outputA, "disposable-alice"); var b = client(outputB, "disposable-alice")) {
            connect(a, server); connect(b, server);
            a.execute("/login alice"); await(() -> a.transport().session().isPresent());
            b.execute("/login alice"); await(() -> !b.transport().isConnected());
            await(() -> outputB.toString().contains("AUTH_FAILED"));
            assertTrue(a.authentication().presenceCredentials().isPresent());
            assertTrue(b.authentication().presenceCredentials().isEmpty()); assertEquals(1, server.sessions().activeSessions().size());
        }
    }

    @Test void explicitDisconnectAndReconnectRequireFreshSessionAndToken() throws Exception {
        try (var server = server(); var cli = client(new StringWriter(), "disposable-alice")) {
            connect(cli, server); cli.execute("/login alice");
            await(() -> cli.authentication().presenceCredentials().isPresent());
            var old = cli.authentication().presenceCredentials().orElseThrow();
            cli.execute("/disconnect");
            assertTrue(cli.authentication().presenceCredentials().isEmpty());
            await(() -> server.sessions().activeSessions().isEmpty());
            connect(cli, server); assertTrue(cli.transport().session().isEmpty());
            cli.execute("/login alice"); await(() -> cli.authentication().presenceCredentials().isPresent());
            var fresh = cli.authentication().presenceCredentials().orElseThrow();
            assertNotEquals(old.session().connectionId(), fresh.session().connectionId());
            assertNotEquals(old.session().clientId(), fresh.session().clientId());
            assertNotEquals(old.presenceToken(), fresh.presenceToken()); assertFalse(cli.authentication().isCurrent(old));
        }
    }

    @Test void twoIndependentAuthenticatedClientsKeepSeparateState() throws Exception {
        try (var server = server(); var a = client(new StringWriter(), "disposable-alice"); var b = client(new StringWriter(), "disposable-bob")) {
            connect(a, server); connect(b, server); a.execute("/login alice"); b.execute("/login bob");
            await(() -> a.authentication().presenceCredentials().isPresent() && b.authentication().presenceCredentials().isPresent());
            var bob = b.authentication().presenceCredentials().orElseThrow();
            assertEquals(2, server.sessions().activeSessions().size());
            a.execute("/logout"); await(() -> !a.transport().isConnected());
            assertTrue(b.authentication().isCurrent(bob)); assertTrue(b.transport().isConnected());
        }
    }

    @Test void serverShutdownClearsClientSessionAndToken() throws Exception {
        try (var server = server(); var cli = client(new StringWriter(), "disposable-alice")) {
            connect(cli, server); cli.execute("/login alice");
            await(() -> cli.authentication().presenceCredentials().isPresent());
            server.close(); await(() -> !cli.transport().isConnected());
            assertTrue(cli.authentication().presenceCredentials().isEmpty()); assertTrue(cli.transport().session().isEmpty());
        }
    }

    @Test void cliCloseRevokesServerSessionAndErasesToken() throws Exception {
        try (var server = server(); var cli = client(new StringWriter(), "disposable-alice")) {
            connect(cli, server); cli.execute("/login alice");
            await(() -> cli.authentication().presenceCredentials().isPresent());
            cli.execute("/quit");
            assertTrue(cli.authentication().presenceCredentials().isEmpty());
            await(() -> server.sessions().activeSessions().isEmpty());
        }
    }

    @Test void productionPresenceHeartbeatsQueryAndUpdatesReachOtherClient() throws Exception {
        StringWriter outputA = new StringWriter(), outputB = new StringWriter();
        try (var server = server();
             var a = client(outputA, "disposable-alice");
             var b = client(outputB, "disposable-bob")) {
            a.execute("/connect 127.0.0.1 " + server.port() + " " + server.udpPort());
            b.execute("/connect 127.0.0.1 " + server.port() + " " + server.udpPort());
            a.execute("/login alice");
            await(() -> a.authentication().presenceCredentials().isPresent());
            await(() -> a.presence().isSendingHeartbeats());
            b.execute("/login bob");
            await(() -> b.authentication().presenceCredentials().isPresent());
            await(() -> b.presence().isSendingHeartbeats());
            var bob = b.authentication().presenceCredentials().orElseThrow();
            await(() -> outputA.toString().contains("Presence: bob [" + bob.session().clientId() + "] is ONLINE."));
            a.execute("/presence");
            await(() -> outputA.toString().contains("Presence snapshot:")
                    && outputA.toString().contains("alice [")
                    && outputA.toString().contains("bob ["));
            a.execute("/logout");
            await(() -> !a.presence().isSendingHeartbeats() && !a.transport().isConnected());
            await(() -> outputB.toString().contains("Presence: alice [")
                    && outputB.toString().contains("is OFFLINE."));
            assertFalse(outputA.toString().contains(a.authentication().presenceCredentials()
                    .map(ClientAuthenticationAdapter.PresenceCredentials::presenceToken).orElse("never")));
        }
    }
}
