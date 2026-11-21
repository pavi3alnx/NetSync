package com.netsync.security;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * In-memory credential validation for the loopback-only disposable demo.
 * Plaintext passwords are retained only by caller-owned input arrays and are never
 * stored by this class after account setup.
 */
public final class CredentialStore {
    static final int ITERATIONS = 120_000;
    static final int KEY_LENGTH_BITS = 256;
    static final int SALT_LENGTH_BYTES = 16;

    private final Map<String, PasswordVerifier> verifiers;
    private final PasswordVerifier dummyVerifier;
    private final SecureRandom secureRandom;

    public record DemoAccountSpec(String username, char[] password) {
        public DemoAccountSpec {
            Objects.requireNonNull(username, "username");
            Objects.requireNonNull(password, "password");
            password = Arrays.copyOf(password, password.length);
        }

        @Override
        public char[] password() {
            return Arrays.copyOf(password, password.length);
        }
    }

    public CredentialStore(List<DemoAccountSpec> accounts) {
        Objects.requireNonNull(accounts, "accounts");
        secureRandom = new SecureRandom();

        Map<String, char[]> plaintextAccounts = new LinkedHashMap<>();
        Map<String, PasswordVerifier> createdVerifiers;
        PasswordVerifier createdDummyVerifier;
        try {
            for (DemoAccountSpec account : accounts) {
                Objects.requireNonNull(account, "account");
                validateUsername(account.username());
                char[] password = account.password;
                try {
                    validatePassword(password);
                    if (plaintextAccounts.putIfAbsent(
                            account.username(), Arrays.copyOf(password, password.length)) != null) {
                        throw new IllegalArgumentException("Duplicate demo account username");
                    }
                } finally {
                    Arrays.fill(password, '\0');
                }
            }
            createdVerifiers = createVerifiers(plaintextAccounts);
            createdDummyVerifier = createVerifier(
                    new char[]{'n', 'o', 't', '-', 'a', '-', 'r', 'e', 'a', 'l', '-', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd'});
        } finally {
            clearPlaintextAccounts(plaintextAccounts);
            for (DemoAccountSpec account : accounts) {
                if (account != null) {
                    Arrays.fill(account.password, '\0');
                }
            }
        }
        verifiers = createdVerifiers;
        dummyVerifier = createdDummyVerifier;
    }

    public CredentialStore(Map<String, char[]> demoAccounts) {
        this(toAccountSpecs(demoAccounts));
    }

    public boolean verify(String username, char[] password) {
        if (!isValidUsername(username) || !isValidPassword(password)) {
            deriveDummy(password);
            return false;
        }

        PasswordVerifier matched = null;
        for (Map.Entry<String, PasswordVerifier> entry : verifiers.entrySet()) {
            boolean usernameMatches = MessageDigest.isEqual(
                    entry.getKey().getBytes(StandardCharsets.UTF_8),
                    username.getBytes(StandardCharsets.UTF_8));
            if (usernameMatches) {
                matched = entry.getValue();
            }
        }

        PasswordVerifier verifier = matched == null ? dummyVerifier : matched;
        PBEKeySpec keySpec = null;
        try {
            keySpec = new PBEKeySpec(password, verifier.salt(), verifier.iterations(), KEY_LENGTH_BITS);
            byte[] candidate = deriveKey(keySpec);
            try {
                return matched != null && MessageDigest.isEqual(candidate, verifier.hash());
            } finally {
                Arrays.fill(candidate, (byte) 0);
            }
        } finally {
            if (keySpec != null) {
                keySpec.clearPassword();
            }
        }
    }

    public boolean verify(String username, String password) {
        if (password == null) {
            deriveDummy(null);
            return false;
        }
        char[] chars = password.toCharArray();
        try {
            return verify(username, chars);
        } finally {
            Arrays.fill(chars, '\0');
        }
    }

    public int accountCount() {
        return verifiers.size();
    }

    private void deriveDummy(char[] password) {
        PBEKeySpec keySpec = null;
        char[] dummyPassword = password == null ? new char[]{'d', 'u', 'm', 'm', 'y'} : null;
        try {
            char[] candidatePassword = password == null ? dummyPassword : password;
            keySpec = new PBEKeySpec(candidatePassword, dummyVerifier.salt(), dummyVerifier.iterations(), KEY_LENGTH_BITS);
            byte[] candidate = deriveKey(keySpec);
            Arrays.fill(candidate, (byte) 0);
        } finally {
            if (keySpec != null) {
                keySpec.clearPassword();
            }
            if (dummyPassword != null) {
                Arrays.fill(dummyPassword, '\0');
            }
        }
    }

    private Map<String, PasswordVerifier> createVerifiers(Map<String, char[]> plaintextAccounts) {
        Map<String, PasswordVerifier> created = new LinkedHashMap<>();
        for (Map.Entry<String, char[]> account : plaintextAccounts.entrySet()) {
            created.put(account.getKey(), createVerifier(account.getValue()));
        }
        return Map.copyOf(created);
    }

    private PasswordVerifier createVerifier(char[] password) {
        byte[] salt = new byte[SALT_LENGTH_BYTES];
        secureRandom.nextBytes(salt);
        PBEKeySpec keySpec = null;
        try {
            keySpec = new PBEKeySpec(password, salt, ITERATIONS, KEY_LENGTH_BITS);
            byte[] hash = deriveKey(keySpec);
            return new PasswordVerifier(salt.clone(), hash, ITERATIONS);
        } finally {
            if (keySpec != null) {
                keySpec.clearPassword();
            }
        }
    }

    private static byte[] deriveKey(PBEKeySpec keySpec) {
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            return factory.generateSecret(keySpec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException exception) {
            throw new IllegalStateException("Credential KDF is unavailable");
        }
    }

    private static List<DemoAccountSpec> toAccountSpecs(Map<String, char[]> demoAccounts) {
        Objects.requireNonNull(demoAccounts, "demoAccounts");
        Map<String, char[]> copy = new LinkedHashMap<>();
        try {
            for (Map.Entry<String, char[]> account : demoAccounts.entrySet()) {
                Objects.requireNonNull(account.getKey(), "username");
                Objects.requireNonNull(account.getValue(), "password");
                copy.put(account.getKey(), Arrays.copyOf(account.getValue(), account.getValue().length));
            }
        } catch (RuntimeException exception) {
            clearPlaintextAccounts(copy);
            throw exception;
        }
        List<DemoAccountSpec> specs = copy.entrySet().stream()
                .map(account -> new DemoAccountSpec(account.getKey(), account.getValue()))
                .toList();
        clearPlaintextAccounts(copy);
        return specs;
    }

    private static void clearPlaintextAccounts(Map<String, char[]> accounts) {
        for (char[] password : accounts.values()) {
            Arrays.fill(password, '\0');
        }
        accounts.clear();
    }

    private static void validateUsername(String username) {
        if (!isValidUsername(username)) {
            throw new IllegalArgumentException("Invalid demo account username");
        }
    }

    private static boolean isValidUsername(String username) {
        return username != null && username.matches("[A-Za-z0-9_]{1,32}");
    }

    private static void validatePassword(char[] password) {
        if (!isValidPassword(password)) {
            throw new IllegalArgumentException("Invalid demo account password");
        }
    }

    private static boolean isValidPassword(char[] password) {
        if (password == null || password.length == 0 || password.length > 128) {
            return false;
        }
        int codePoints = 0;
        for (int offset = 0; offset < password.length;) {
            int codePoint = Character.codePointAt(password, offset);
            codePoints++;
            if (codePoints > 128) {
                return false;
            }
            offset += Character.charCount(codePoint);
        }
        return true;
    }

    private record PasswordVerifier(byte[] salt, byte[] hash, int iterations) {
        private PasswordVerifier {
            salt = salt.clone();
            hash = hash.clone();
        }

        @Override
        public byte[] salt() {
            return salt.clone();
        }

        @Override
        public byte[] hash() {
            return hash.clone();
        }

        @Override
        public String toString() {
            return "PasswordVerifier[secret=REDACTED]";
        }
    }
}
