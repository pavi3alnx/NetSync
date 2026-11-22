package com.netsync.client;

import com.netsync.common.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Minimal reusable CLI shell. Owns the supplied transport, but not input/output streams. */
public final class NetSyncClient implements AutoCloseable {
    private final TcpClient transport;
    private final CommandRegistry commands = new CommandRegistry();
    private final PrintWriter output;
    private final List<ClientDispatcher.Registration> display = new ArrayList<>();
    private final ClientAuthenticationAdapter authentication;
    private volatile boolean quit;
    public NetSyncClient(TcpClient transport, PrintWriter output) {
        this(transport, output, ClientAuthenticationAdapter.PasswordInput.console());
    }
    public NetSyncClient(TcpClient transport, PrintWriter output,
                         ClientAuthenticationAdapter.PasswordInput passwords) {
        this.transport = Objects.requireNonNull(transport); this.output = Objects.requireNonNull(output);
        commands.register("/help", "/help", "List commands", args -> {
            noArguments(args);
            commands.descriptions().forEach(d -> print(d.usage() + " - " + d.summary()));
        });
        commands.register("/quit", "/quit", "Exit", args -> { noArguments(args); quit = true; transport.close(); });
        commands.register("/connect", "/connect [host [port]]", "Connect (default 127.0.0.1:5050)", args -> {
            if (args.size() > 2) throw new CommandRegistry.InputException("Usage: /connect [host [port]]");
            int port = ProtocolLimits.TCP_PORT;
            if (args.size() == 2) {
                try { port = Integer.parseInt(args.get(1)); }
                catch (NumberFormatException e) { throw new CommandRegistry.InputException("Port must be an integer from 1 to 65535"); }
            }
            if (port < 1 || port > 65535) throw new CommandRegistry.InputException("Port must be from 1 to 65535");
            if (transport.state() != TcpClient.State.DISCONNECTED)
                throw new CommandRegistry.InputException("Disconnect before connecting again");
            transport.connect(args.isEmpty() ? ProtocolLimits.BIND_ADDRESS : args.getFirst(), port);
            print("Connected.");
        });
        commands.register("/disconnect", "/disconnect", "Disconnect", args -> {
            noArguments(args); transport.disconnect(); print("Disconnected.");
        });
        commands.register("/status", "/status", "Show connection status", args -> {
            noArguments(args); print(transport.state() + " (" + transport.failure() + ")");
        });
        for (MessageType type : MessageType.values()) {
            if (type != MessageType.FILE_CHUNK) display.add(transport.dispatcher().register(type,
                message -> print("Received " + message.type() + " [" + message.messageId() + "]")));
        }
        // Binary is deliberately not consumed by the shell: file module must register a receiver.
        authentication = new ClientAuthenticationAdapter(transport, commands, this::print, passwords);
    }
    public TcpClient transport() { return transport; }
    public CommandRegistry commands() { return commands; }
    public ClientAuthenticationAdapter authentication() { return authentication; }
    public void execute(String line) {
        if (quit) return;
        try { commands.execute(line); }
        catch (CommandRegistry.InputException e) { print(e.getMessage()); }
        catch (IllegalArgumentException | IllegalStateException e) {
            // Core errors are safe constants. Module exceptions may contain credentials: never echo them.
            print("Command rejected. Check /help, arguments, and connection status.");
        } catch (IOException e) { print("Connection or I/O operation failed. Check /status and server address."); }
        catch (Exception e) { print("Command failed."); }
    }
    public void run(Reader input) throws IOException {
        Objects.requireNonNull(input);
        welcome();
        BufferedReader reader = input instanceof BufferedReader b ? b : new BufferedReader(input);
        try {
            String line;
            while (!quit && (line = reader.readLine()) != null) execute(line);
        } finally { close(); }
    }
    /** Uses the same Console for commands and hidden passwords; no competing buffered System.in reader. */
    public void run(Console console) {
        Objects.requireNonNull(console);
        welcome();
        try {
            String line;
            while (!quit && (line = console.readLine("netsync> ")) != null) execute(line);
        } finally { close(); }
    }
    private void welcome() {
        print("Welcome to NetSync. Use /help to list commands.");
        print("Unencrypted loopback demo: use only disposable local credentials. Log in within 10 seconds of connecting.");
    }
    private static void noArguments(List<String> args) {
        if (!args.isEmpty()) throw new CommandRegistry.InputException("This command takes no arguments");
    }
    private void print(String text) { synchronized (output) { output.println(text); output.flush(); } }
    @Override public void close() {
        quit = true; authentication.close(); display.forEach(ClientDispatcher.Registration::close); transport.close();
    }
    public static void main(String[] args) throws IOException {
        try (NetSyncClient cli = new NetSyncClient(new TcpClient(), new PrintWriter(System.out, true))) {
            Console console = System.console();
            if (console != null) cli.run(console);
            else cli.run(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        }
    }
}
