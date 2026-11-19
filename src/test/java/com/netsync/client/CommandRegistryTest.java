package com.netsync.client;

import org.junit.jupiter.api.*;
import java.io.*;
import java.net.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class CommandRegistryTest {
    @Test void parsesWhitespaceQuotesEscapesAndEmptyArguments() {
        assertEquals(List.of("/msg", "Alice", "hello world", "", "it's fine", "a b"),
            CommandRegistry.parse("  /msg\tAlice \"hello world\" '' \"it's fine\" a\\ b  "));
        assertTrue(CommandRegistry.parse(" \t ").isEmpty());
        assertEquals(List.of("/x", "ab cd"), CommandRegistry.parse("/x ab' c'd"));
    }
    @Test void rejectsMalformedInput() {
        for (String line : List.of("/msg 'oops", "/msg \"oops", "/msg tail\\", "x".repeat(65537)))
            assertThrows(IllegalArgumentException.class, () -> CommandRegistry.parse(line));
    }
    @Test void registrationRejectsDuplicateAndUnknownCommands() throws Exception {
        CommandRegistry registry = new CommandRegistry(); List<List<String>> calls = new ArrayList<>();
        registry.register("/msg", "/msg name text", "Module command", calls::add);
        assertThrows(IllegalArgumentException.class, () -> registry.register("/msg", "", "", a -> {}));
        assertThrows(IllegalArgumentException.class, () -> registry.register("msg", "", "", a -> {}));
        registry.execute("/msg Bob 'hello there'"); assertEquals(List.of(List.of("Bob", "hello there")), calls);
        assertThrows(IllegalArgumentException.class, () -> registry.execute("/missing"));
        registry.execute(" "); assertEquals(1, registry.descriptions().size());
    }
    @Test void helpIncludesModuleCommandsAndStatusAndErrorsAreSafe() {
        StringWriter output = new StringWriter();
        try (NetSyncClient cli = new NetSyncClient(new TcpClient(), new PrintWriter(output))) {
            cli.commands().register("/users", "/users", "Module users command", a -> {});
            cli.commands().register("/bad", "/bad", "Fail", a -> { throw new RuntimeException("secret-token"); });
            cli.execute("/help"); cli.execute("/status"); cli.execute("/missing"); cli.execute("/bad"); cli.execute("/msg 'bad");
            String text = output.toString();
            assertTrue(text.contains("/users - Module users command")); assertTrue(text.contains("DISCONNECTED"));
            assertTrue(text.contains("Command")); assertFalse(text.contains("secret-token"));
        }
    }
    @Test void connectDisconnectStatusQuitAndEofCleanup() throws Exception {
        StringWriter output = new StringWriter();
        TcpClient transport = new TcpClient();
        try (ServerSocket server = new ServerSocket(0); NetSyncClient cli = new NetSyncClient(transport, new PrintWriter(output))) {
            cli.execute("/connect 127.0.0.1 " + server.getLocalPort());
            try (Socket peer = server.accept()) {
                assertTrue(transport.isConnected()); cli.execute("/status");
                cli.execute("/disconnect"); assertFalse(transport.isConnected());
                cli.execute("/connect 127.0.0.1 " + server.getLocalPort());
                try (Socket next = server.accept()) {
                    cli.run(new StringReader("/status\n/quit\n/connect\n"));
                    assertEquals(TcpClient.State.CLOSED, transport.state());
                }
            }
        }
        assertTrue(output.toString().contains("CONNECTED")); assertTrue(output.toString().contains("Welcome to NetSync"));
        TcpClient eofTransport = new TcpClient();
        try (NetSyncClient eof = new NetSyncClient(eofTransport, new PrintWriter(new StringWriter()))) {
            eof.run(new StringReader("")); assertEquals(TcpClient.State.CLOSED, eofTransport.state());
        }
    }
    @Test void eofAndInputFailureCloseConnectedTransport() throws Exception {
        for (boolean fail : List.of(false, true)) {
            TcpClient transport = new TcpClient();
            try (ServerSocket server = new ServerSocket(0); NetSyncClient cli = new NetSyncClient(transport, new PrintWriter(new StringWriter()))) {
                transport.connect("127.0.0.1", server.getLocalPort());
                try (Socket peer = server.accept()) {
                    if (fail) assertThrows(IOException.class, () -> cli.run(new Reader() {
                        public int read(char[] b, int off, int len) throws IOException { throw new IOException("secret"); }
                        public void close() {}
                    }));
                    else cli.run(new StringReader(""));
                    assertEquals(TcpClient.State.CLOSED, transport.state());
                    peer.setSoTimeout(3000); assertEquals(-1, peer.getInputStream().read());
                }
            }
        }
    }
    @Test void invalidCoreArgumentsDoNotCrashOrQuit() {
        StringWriter output = new StringWriter(); TcpClient transport = new TcpClient();
        try (NetSyncClient cli = new NetSyncClient(transport, new PrintWriter(output))) {
            for (String input : List.of("/connect h x", "/connect h 0", "/connect a b c", "/disconnect x", "/quit x", "/status x", "/help x"))
                cli.execute(input);
            assertEquals(TcpClient.State.DISCONNECTED, transport.state());
        }
    }
}
