package com.netsync.app;

import com.netsync.security.CredentialStore;
import java.io.Console;
import java.util.*;
import java.util.function.Function;

/** Provisions in-memory verifiers before any listener/worker starts. No credential files. */
final class DemoAccounts {
    private DemoAccounts() {}

    static CredentialStore read(DemoServerConfig config, Console console) {
        if (console == null)
            throw new IllegalStateException("An interactive console is required for hidden password entry");
        return provision(config, user -> console.readPassword("Disposable password for %s: ", user));
    }

    /** Takes ownership of returned arrays and clears them, including on cancellation/failure. */
    static CredentialStore provision(DemoServerConfig config, Function<String, char[]> readPassword) {
        Objects.requireNonNull(config);
        Objects.requireNonNull(readPassword);
        Map<String, char[]> accounts = new LinkedHashMap<>();
        try {
            for (String user : config.users()) {
                char[] password = readPassword.apply(user);
                if (password == null) throw new IllegalArgumentException("Demo account entry was cancelled");
                accounts.put(user, password);
            }
            return new CredentialStore(accounts);
        } finally {
            accounts.values().forEach(password -> Arrays.fill(password, '\0'));
            accounts.clear();
        }
    }
}
