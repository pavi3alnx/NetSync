package com.netsync.client;

import com.netsync.chat.ChatClientAdapter;
import com.netsync.transfer.FileTransferClientAdapter;
import com.netsync.common.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.nio.file.Path;

/** Minimal reusable CLI shell. Owns the supplied transport, but not input/output streams. */
public final class NetSyncClient implements AutoCloseable {
    private final TcpClient transport;
    private final CommandRegistry commands = new CommandRegistry();
    private final ClientOutputQueue output;
    private final List<ClientDispatcher.Registration> display = new ArrayList<>();
    private final ClientAuthenticationAdapter authentication;
    private final ClientPresenceAdapter presence;
    private final ChatClientAdapter chat;
    private final FileTransferClientAdapter fileTransfer;
    private final int defaultUdpPort;
    private volatile boolean quit;
    public NetSyncClient(TcpClient transport, PrintWriter output) {
        this(transport, output, ClientAuthenticationAdapter.PasswordInput.console());
    }
    public NetSyncClient(TcpClient transport, PrintWriter output,
                         ClientAuthenticationAdapter.PasswordInput passwords) {
        this(transport, output, passwords, ProtocolLimits.UDP_PORT);
    }
    public NetSyncClient(TcpClient transport, PrintWriter output,
                         ClientAuthenticationAdapter.PasswordInput passwords, int udpPort) {
        this(transport, output, passwords, udpPort, Path.of(ProtocolLimits.STORAGE_ROOT, "clients"));
    }
    public NetSyncClient(TcpClient transport, PrintWriter output,
                         ClientAuthenticationAdapter.PasswordInput passwords, int udpPort,
                         Path clientsRoot) {
        this.transport = Objects.requireNonNull(transport);
        PrintWriter writer = Objects.requireNonNull(output);
        if (udpPort < 1 || udpPort > 65535) throw new IllegalArgumentException("Invalid presence UDP port");
        this.output = new ClientOutputQueue(text -> {
            synchronized (writer) { writer.println(text); writer.flush(); }
        }, transport::isGenerationCurrent, transport::isGenerationActive, ClientOutputQueue.DEFAULT_CAPACITY);
        this.defaultUdpPort = udpPort;
        commands.register("/help", "/help", "List commands", args -> {
            noArguments(args);
            commands.descriptions().forEach(d -> print(d.usage() + " - " + d.summary()));
        });
        commands.register("/quit", "/quit", "Exit", args -> { noArguments(args); close(); });
        commands.register("/connect", "/connect [host [tcp-port [udp-port]]]", "Connect (default 127.0.0.1 TCP 5050 UDP 5051)", args -> {
            if (args.size() > 3) throw new CommandRegistry.InputException("Usage: /connect [host [tcp-port [udp-port]]]");
            int port = ProtocolLimits.TCP_PORT;
            int presencePort = defaultUdpPort;
            if (args.size() >= 2) {
                try { port = Integer.parseInt(args.get(1)); }
                catch (NumberFormatException e) { throw new CommandRegistry.InputException("Port must be an integer from 1 to 65535"); }
            }
            if (args.size() == 3) {
                try { presencePort = Integer.parseInt(args.get(2)); }
                catch (NumberFormatException e) { throw new CommandRegistry.InputException("UDP port must be from 1 to 65535"); }
            }
            if (port < 1 || port > 65535) throw new CommandRegistry.InputException("Port must be from 1 to 65535");
            if (transport.state() != TcpClient.State.DISCONNECTED)
                throw new CommandRegistry.InputException("Disconnect before connecting again");
            configurePresencePort(presencePort);
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
            if (type != MessageType.FILE_CHUNK) display.add(transport.dispatcher().register(type, this::displayGenericMessage));
        }
        // Binary is deliberately not consumed by the shell: file module must register a receiver.
        authentication = new ClientAuthenticationAdapter(transport, commands, this.output, passwords);
        presence = new ClientPresenceAdapter(transport, authentication, commands, this.output, udpPort);
        chat = new ChatClientAdapter(transport, authentication, commands,
                (text, generation, activeOnly) -> {
                    if (activeOnly) this.output.offerActiveImportant(text, generation);
                    else this.output.offerImportant(text, generation);
                });
        Objects.requireNonNull(clientsRoot);
        fileTransfer = new FileTransferClientAdapter(transport, commands,
                (text, generation) -> this.output.offerActiveImportant(text, generation),
                clientsRoot,
                () -> transport.session().map(session -> clientsRoot.resolve(session.clientId().toString())).orElse(null),
                null);
    }
    public TcpClient transport() { return transport; }
    public CommandRegistry commands() { return commands; }
    public ClientAuthenticationAdapter authentication() { return authentication; }
    public ClientPresenceAdapter presence() { return presence; }
    public ChatClientAdapter chat() { return chat; }
    public FileTransferClientAdapter fileTransfer() { return fileTransfer; }
    ClientOutputQueue outputQueueForTesting() { return output; }
    private void displayGenericMessage(MessageEnvelope message) {
        if (chat != null && chat.ownsResponse(message)) return;
        if (fileTransfer != null && fileTransfer.ownsResponse(message)) return;
        output.offerActiveImportant("Received " + message.type() + " [" + message.messageId() + "]",
                transport.connectionId().orElse(null));
    }
    private void configurePresencePort(int port) { presence.setUdpPort(port); }
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
    private void print(String text) { output.offer(text); }
    @Override public void close() {
        quit = true; fileTransfer.close(); chat.close(); presence.close(); authentication.close(); display.forEach(ClientDispatcher.Registration::close);
        transport.close(); output.close();
    }
    public static void main(String[] args) throws IOException {
        try (NetSyncClient cli = new NetSyncClient(new TcpClient(), new PrintWriter(System.out, true))) {
            Console console = System.console();
            if (console != null) cli.run(console);
            else cli.run(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        }
    }
}
