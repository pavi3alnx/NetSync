package com.netsync.client;

import java.util.*;

/** Module-owned commands execute on the calling CLI thread, never a socket thread. */
public final class CommandRegistry {
    /** Safe user-facing input error. Module implementations should never put secrets here. */
    public static final class InputException extends IllegalArgumentException {
        public InputException(String message) { super(message); }
    }
    @FunctionalInterface public interface Command { void execute(List<String> arguments) throws Exception; }
    public record Description(String name, String usage, String summary) {}
    private record Entry(Description description, Command command) {}
    private final Map<String, Entry> commands = new LinkedHashMap<>();
    public synchronized void register(String name, String usage, String summary, Command command) {
        Objects.requireNonNull(name); Objects.requireNonNull(usage); Objects.requireNonNull(summary); Objects.requireNonNull(command);
        if (!name.matches("/[a-z][a-z0-9-]*")) throw new IllegalArgumentException("Invalid command name");
        if (commands.containsKey(name)) throw new IllegalArgumentException("Command already registered: " + name);
        commands.put(name, new Entry(new Description(name, usage, summary), command));
    }
    public synchronized List<Description> descriptions() {
        return commands.values().stream().map(Entry::description).toList();
    }
    /** Remove only a command owned by this adapter, including constructor rollback. */
    public synchronized void unregister(String name, Command owner) {
        Entry entry = commands.get(name);
        if (entry != null && entry.command == owner) commands.remove(name);
    }
    public void execute(String line) throws Exception {
        List<String> tokens = parse(line);
        if (tokens.isEmpty()) return;
        Entry entry;
        synchronized (this) { entry = commands.get(tokens.getFirst()); }
        if (entry == null) throw new InputException("Unknown command. Use /help.");
        entry.command.execute(List.copyOf(tokens.subList(1, tokens.size())));
    }
    /** Single/double quotes and backslash escaping; empty quoted arguments are retained. */
    public static List<String> parse(String line) {
        Objects.requireNonNull(line);
        if (line.length() > 65_536) throw new InputException("Command is too long");
        List<String> result = new ArrayList<>();
        StringBuilder token = new StringBuilder();
        char quote = 0; boolean escape = false, started = false;
        for (char c : line.toCharArray()) {
            if (escape) { token.append(c); escape = false; started = true; }
            else if (c == '\\') { escape = true; started = true; }
            else if (quote != 0) { if (c == quote) quote = 0; else token.append(c); }
            else if (c == '\'' || c == '"') { quote = c; started = true; }
            else if (Character.isWhitespace(c)) {
                if (started) { result.add(token.toString()); token.setLength(0); started = false; }
            } else { token.append(c); started = true; }
        }
        if (escape || quote != 0) throw new InputException("Unterminated quote or escape");
        if (started) result.add(token.toString());
        return List.copyOf(result);
    }
}
