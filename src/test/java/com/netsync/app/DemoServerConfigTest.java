package com.netsync.app;

import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class DemoServerConfigTest {
    private static DemoServerConfig config() {
        return new DemoServerConfig(true, 0, List.of("alice", "bob"));
    }

    @Test void explicitDemoFlagAndUsersAreRequired() {
        assertThrows(IllegalArgumentException.class, () -> DemoServerConfig.parse(new String[]{"--users", "alice"}));
        assertThrows(IllegalArgumentException.class, () -> DemoServerConfig.parse(new String[]{"--insecure-demo"}));
    }

    @Test void validOptionsUseDocumentedDefaultPortAndImmutableUsers() {
        var config = DemoServerConfig.parse(new String[]{"--insecure-demo", "--users", "alice,bob"});
        assertEquals(5050, config.port());
        assertEquals(5051, config.udpPort());
        assertEquals(List.of("alice", "bob"), config.users());
        assertThrows(UnsupportedOperationException.class, () -> config.users().add("charlie"));
        assertEquals(0, DemoServerConfig.parse(new String[]{"--users", "alice", "--port", "0", "--insecure-demo"}).port());
        assertEquals(5052, DemoServerConfig.parse(new String[]{"--users", "alice", "--udp-port", "5052", "--insecure-demo"}).udpPort());
    }

    @Test void invalidOptionsPortsAndUsernamesAreRejectedWithoutEchoingArguments() {
        for (String[] args : List.of(
                new String[]{"--insecure-demo", "--users", "alice", "--bind", "0.0.0.0"},
                new String[]{"--insecure-demo", "--insecure-demo", "--users", "alice"},
                new String[]{"--insecure-demo", "--users", "alice", "--port", "65536"},
                new String[]{"--insecure-demo", "--users", "alice", "--port", "secret-value"},
                new String[]{"--insecure-demo", "--users", "alice", "--udp-port", "65536"},
                new String[]{"--insecure-demo", "--users", "alice,alice"},
                new String[]{"--insecure-demo", "--users", "alice,"},
                new String[]{"--insecure-demo", "--users", "invalid-user"},
                new String[]{"--insecure-demo", "--users", "alice", "--port"})) {
            var failure = assertThrows(IllegalArgumentException.class, () -> DemoServerConfig.parse(args));
            assertFalse(failure.getMessage().contains("secret-value"));
        }
    }

    @Test void invalidConfigurationIsRejectedBeforePasswordPromptsOrResources() {
        assertThrows(IllegalArgumentException.class, () -> new DemoServerConfig(false, 5050, List.of("alice")));
        assertThrows(IllegalArgumentException.class, () -> new DemoServerConfig(true, -1, List.of("alice")));
        assertThrows(IllegalArgumentException.class, () -> new DemoServerConfig(true, 5050,
                java.util.stream.IntStream.range(0, 101).mapToObj(i -> "u" + i).toList()));
    }

    @Test void provisioningBuildsRealVerifiersAndClearsInputArrays() {
        Map<String, char[]> passwords = new HashMap<>();
        var credentials = DemoAccounts.provision(config(), user -> {
            char[] password = ("disposable-" + user).toCharArray();
            passwords.put(user, password);
            return password;
        });
        assertTrue(credentials.verify("alice", "disposable-alice"));
        assertTrue(credentials.verify("bob", "disposable-bob"));
        assertEquals(2, credentials.accountCount());
        passwords.values().forEach(password -> assertArrayEquals(new char[password.length], password));
    }

    @Test void cancelledProvisioningClearsPreviouslyReadPasswords() {
        char[] password = "disposable-only".toCharArray();
        AtomicInteger calls = new AtomicInteger();
        assertThrows(IllegalArgumentException.class, () -> DemoAccounts.provision(config(),
                user -> calls.getAndIncrement() == 0 ? password : null));
        assertArrayEquals(new char[password.length], password);
    }

    @Test void invalidPasswordClearsAllArraysAndDoesNotLeakSecretInError() {
        char[] longPassword = "s".repeat(129).toCharArray();
        char[] other = "disposable-only".toCharArray();
        var failure = assertThrows(IllegalArgumentException.class, () -> DemoAccounts.provision(config(),
                user -> user.equals("alice") ? longPassword : other));
        assertArrayEquals(new char[129], longPassword);
        assertArrayEquals(new char[other.length], other);
        assertFalse(failure.getMessage().contains("disposable-only"));
    }

    @Test void noninteractivePasswordInputIsRefused() {
        assertThrows(IllegalStateException.class, () -> DemoAccounts.read(config(), null));
    }
}
