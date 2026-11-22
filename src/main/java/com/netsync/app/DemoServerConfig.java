package com.netsync.app;

import com.netsync.common.ProtocolLimits;
import java.util.*;

/** Explicit, loopback-only disposable authentication demo; never contains passwords. */
public record DemoServerConfig(boolean insecureDemo, int port, List<String> users) {
    public DemoServerConfig {
        if (!insecureDemo) throw new IllegalArgumentException("Explicit --insecure-demo enablement is required");
        if (port < 0 || port > 65535) throw new IllegalArgumentException("Port must be from 0 to 65535");
        if (users == null || users.isEmpty() || users.size() > ProtocolLimits.MAX_CONNECTIONS)
            throw new IllegalArgumentException("Provide between 1 and 100 disposable demo usernames");
        Set<String> unique = new HashSet<>();
        for (String user : users) {
            if (user == null || !user.matches("[A-Za-z0-9_]{1,32}") || !unique.add(user))
                throw new IllegalArgumentException("Demo usernames must be valid and unique");
        }
        users = List.copyOf(users);
    }

    public static DemoServerConfig parse(String[] args) {
        Objects.requireNonNull(args);
        boolean demo = false;
        int port = ProtocolLimits.TCP_PORT;
        List<String> users = List.of();
        Set<String> options = new HashSet<>();
        for (int i = 0; i < args.length; i++) {
            String option = args[i];
            if (!Set.of("--insecure-demo", "--port", "--users").contains(option) || !options.add(option))
                throw new IllegalArgumentException("Unknown or repeated option");
            switch (option) {
                case "--insecure-demo" -> demo = true;
                case "--port" -> {
                    if (++i == args.length) throw new IllegalArgumentException("Missing port");
                    try { port = Integer.parseInt(args[i]); }
                    catch (NumberFormatException e) { throw new IllegalArgumentException("Invalid port"); }
                }
                case "--users" -> {
                    if (++i == args.length) throw new IllegalArgumentException("Missing demo usernames");
                    users = Arrays.asList(args[i].split(",", -1));
                }
                default -> throw new AssertionError();
            }
        }
        return new DemoServerConfig(demo, port, users);
    }
}
