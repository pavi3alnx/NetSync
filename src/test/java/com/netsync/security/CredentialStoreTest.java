package com.netsync.security;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CredentialStoreTest {
    @Test
    void verifiesCorrectCharArrayCredentials() {
        CredentialStore store = new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray())));

        assertTrue(store.verify("alice", "demo-alice-pass".toCharArray()));
        assertEquals(1, store.accountCount());
    }

    @Test
    void verifiesCorrectStringCredentials() {
        CredentialStore store = new CredentialStore(Map.of(
                "bob", "demo-bob-pass".toCharArray()));

        assertTrue(store.verify("bob", "demo-bob-pass"));
    }

    @Test
    void rejectsIncorrectPasswordAndUnknownUsername() {
        CredentialStore store = new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray())));

        assertFalse(store.verify("alice", "wrong-password".toCharArray()));
        assertFalse(store.verify("unknown", "demo-alice-pass".toCharArray()));
    }

    @Test
    void unknownUsernameReturnsSafeBooleanOnly() {
        CredentialStore store = new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray())));

        assertFalse(store.verify("different_user", "anything"));
        assertFalse(store.verify("different_user", "demo-alice-pass"));
    }

    @Test
    void rejectsNullEmptyAndOversizedPasswords() {
        CredentialStore store = new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray())));

        assertFalse(store.verify("alice", (char[]) null));
        assertFalse(store.verify("alice", new char[0]));
        assertFalse(store.verify("alice", "x".repeat(129).toCharArray()));
        assertFalse(store.verify("alice", (String) null));
    }

    @Test
    void rejectsMalformedUsernames() {
        CredentialStore store = new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray())));

        assertFalse(store.verify(null, "demo-alice-pass"));
        assertFalse(store.verify("", "demo-alice-pass"));
        assertFalse(store.verify("bad-name", "demo-alice-pass"));
        assertFalse(store.verify("a".repeat(33), "demo-alice-pass"));
    }

    @Test
    void rejectsDuplicateDemoAccountUsernames() {
        assertThrows(IllegalArgumentException.class, () -> new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("alice", "first-pass".toCharArray()),
                new CredentialStore.DemoAccountSpec("alice", "second-pass".toCharArray()))));
    }

    @Test
    void rejectsMalformedAccountConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("bad-name", "pass".toCharArray()))));
        assertThrows(IllegalArgumentException.class, () -> new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("alice", new char[0]))));
        assertThrows(IllegalArgumentException.class, () -> new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("alice", "x".repeat(129).toCharArray()))));
        assertThrows(NullPointerException.class, () -> new CredentialStore((List<CredentialStore.DemoAccountSpec>) null));
        assertThrows(NullPointerException.class, () -> new CredentialStore((Map<String, char[]>) null));
    }

    @Test
    void accountSpecCopiesPasswordInput() {
        char[] password = "demo-alice-pass".toCharArray();
        CredentialStore.DemoAccountSpec account = new CredentialStore.DemoAccountSpec("alice", password);
        CredentialStore store = new CredentialStore(List.of(account));

        java.util.Arrays.fill(password, 'x');

        assertTrue(store.verify("alice", "demo-alice-pass"));
        assertFalse(store.verify("alice", "xxxxxxxxxxxxxxx"));
        assertArrayEquals(new char[password.length], account.password());
    }

    @Test
    void mapInputCopiesPasswordArrays() {
        char[] password = "demo-bob-pass".toCharArray();
        Map<String, char[]> accounts = new LinkedHashMap<>();
        accounts.put("bob", password);
        CredentialStore store = new CredentialStore(accounts);

        java.util.Arrays.fill(password, 'x');

        assertTrue(store.verify("bob", "demo-bob-pass"));
        assertEquals(1, store.accountCount());
    }

    @Test
    void mapInputRejectsNullAndDuplicateUsernamesAreNormalizedByMap() {
        Map<String, char[]> accounts = new LinkedHashMap<>();
        accounts.put("alice", null);

        assertThrows(NullPointerException.class, () -> new CredentialStore(accounts));

        CredentialStore store = new CredentialStore(Map.of(
                "alice", "first".toCharArray()));
        assertTrue(store.verify("alice", "first"));
        assertEquals(1, store.accountCount());
    }

    @Test
    void supportsMaximumPasswordLength() {
        char[] password = "x".repeat(128).toCharArray();
        CredentialStore store = new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("alice", password)));

        assertTrue(store.verify("alice", password));
    }

    @Test
    void verifierStateAndAccountSpecsDoNotExposePasswordsInToString() {
        CredentialStore store = new CredentialStore(List.of(
                new CredentialStore.DemoAccountSpec("alice", "demo-alice-pass".toCharArray())));
        CredentialStore.DemoAccountSpec spec =
                new CredentialStore.DemoAccountSpec("bob", "demo-bob-pass".toCharArray());

        assertFalse(store.toString().contains("demo-alice-pass"));
        assertFalse(spec.toString().contains("demo-bob-pass"));
        assertEquals("demo-bob-pass", new String(spec.password()));
    }
}
