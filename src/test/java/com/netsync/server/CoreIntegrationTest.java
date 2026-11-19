package com.netsync.server;

import com.netsync.common.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class CoreIntegrationTest {
    private record Closure(UUID id, Thread thread, boolean cleaned) {}
    private final FrameCodec codec = new FrameCodec(new MessageCodec());

    private MessageEnvelope request() {
        return new MessageEnvelope(1, MessageType.PRESENCE_QUERY, UUID.randomUUID(), null, new MessageBody.Empty());
    }

    private Socket connect(NioServer server) throws Exception {
        Socket client = new Socket("127.0.0.1", server.port());
        client.setSoTimeout(3000);
        return client;
    }

    private void write(Socket client) throws Exception {
        ByteBuffer frame = codec.encode(request());
        byte[] bytes = new byte[frame.remaining()];
        frame.get(bytes);
        client.getOutputStream().write(bytes);
    }

    private <T> T next(LinkedBlockingQueue<T> queue) throws Exception {
        T value = queue.poll(3, TimeUnit.SECONDS);
        assertNotNull(value, "Expected selector callback");
        return value;
    }

    @Test void backgroundTasksExecuteOnTheSameThreadAsSocketHandlersAndSurviveExceptions() throws Exception {
        var handlers = new LinkedBlockingQueue<Thread>();
        var tasks = new LinkedBlockingQueue<Thread>();
        try (NioServer server = new NioServer(0,
                (context, message) -> handlers.add(Thread.currentThread()), (context, chunk) -> {})) {
            server.start();
            try (Socket client = connect(server)) {
                write(client);
                Thread selector = next(handlers);
                assertTrue(CompletableFuture.supplyAsync(() -> server.selectorTasks().submit(() -> {
                    throw new IllegalStateException("private task details");
                })).get(3, TimeUnit.SECONDS));
                assertTrue(CompletableFuture.supplyAsync(() -> server.selectorTasks().submit(
                        () -> tasks.add(Thread.currentThread()))).get(3, TimeUnit.SECONDS));
                assertSame(selector, next(tasks));
                assertNotSame(Thread.currentThread(), selector);
                write(client);
                assertSame(selector, next(handlers));
                assertNull(server.failure());
            }
        }
    }

    @Test void selfReschedulingTasksDoNotStarveSocketReadiness() throws Exception {
        var received = new LinkedBlockingQueue<Boolean>();
        AtomicBoolean producing = new AtomicBoolean(true);
        try (NioServer server = new NioServer(0,
                (context, message) -> received.add(true), (context, chunk) -> {})) {
            server.start();
            Runnable load = new Runnable() {
                @Override public void run() { if (producing.get()) server.selectorTasks().submit(this); }
            };
            try (Socket client = connect(server)) {
                assertTrue(server.selectorTasks().submit(load));
                write(client);
                assertTrue(next(received));
            } finally { producing.set(false); }
            assertNull(server.failure());
        }
    }

    @Test void shutdownRejectsNewTasksDrainsAcceptedTasksAndCancelsUnusedReservation() throws Exception {
        var executed = new LinkedBlockingQueue<Thread>();
        var cancelled = new LinkedBlockingQueue<Thread>();
        try (NioServer server = new NioServer(0)) {
            assertFalse(server.selectorTasks().submit(() -> {}));
            server.start();
            SelectorTasks.Reservation reservation = server.selectorTasks().tryReserveCompletion(
                    () -> cancelled.add(Thread.currentThread())).orElseThrow();
            CompletableFuture<Boolean> accepted = new CompletableFuture<>();
            assertTrue(server.selectorTasks().submit(() -> {
                accepted.complete(server.selectorTasks().submit(() -> executed.add(Thread.currentThread())));
                server.close(); // Seal the queue before the queued task can be consumed.
            }));
            assertTrue(accepted.get(3, TimeUnit.SECONDS));
            Thread cancellationThread = next(cancelled);
            assertSame(cancellationThread, next(executed));
            server.close();
            assertNotSame(Thread.currentThread(), cancellationThread);
            assertFalse(server.selectorTasks().submit(() -> {}));
            assertTrue(server.selectorTasks().tryReserveCompletion().isEmpty());
            assertThrows(IllegalStateException.class, () -> reservation.submit(() -> {}));
            assertFalse(server.isRunning());
            assertNull(server.failure());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"eof", "reset", "explicit", "malformed", "overflow", "shutdown"})
    void disconnectListenersRunOnceAfterCleanupForEveryClosurePath(String path) throws Exception {
        var contexts = new LinkedBlockingQueue<ConnectionState>();
        var closures = new LinkedBlockingQueue<Closure>();
        AtomicReference<ConnectionState> observed = new AtomicReference<>();
        try (NioServer server = new NioServer(0, (context, message) -> {
            ConnectionState state = (ConnectionState) context;
            observed.set(state);
            contexts.add(state);
        }, (context, chunk) -> {})) {
            server.addDisconnectListener(id -> {
                ConnectionState state = observed.get();
                closures.add(new Closure(id, Thread.currentThread(), state.closed && !state.key.isValid()
                        && !state.key.channel().isOpen() && state.key.attachment() == null
                        && state.payload == null && state.output.isEmpty() && state.queuedBytes == 0));
            });
            server.start();
            try (Socket client = connect(server)) {
                write(client);
                ConnectionState context = next(contexts);
                switch (path) {
                    case "eof" -> client.shutdownOutput();
                    case "reset" -> { client.setSoLinger(true, 0); client.close(); }
                    case "explicit" -> assertTrue(server.selectorTasks().submit(() -> {
                        server.closeConnection(context.connectionId());
                        server.closeConnection(context.connectionId());
                    }));
                    case "malformed" -> client.getOutputStream().write(new byte[]{0, 0, 0, 0});
                    case "overflow" -> assertTrue(server.selectorTasks().submit(() -> {
                        for (int i = 0; i <= ProtocolLimits.MAX_OUTPUT_FRAMES; i++) server.send(context.connectionId(), request());
                    }));
                    case "shutdown" -> server.close();
                    default -> throw new IllegalArgumentException(path);
                }
                Closure closure = next(closures);
                assertEquals(context.connectionId(), closure.id());
                assertNotSame(Thread.currentThread(), closure.thread());
                assertTrue(closure.cleaned());
                server.close();
                server.close();
                assertTrue(closures.isEmpty(), "Repeated cleanup must not notify twice");
                assertNull(server.failure());
            }
        }
    }

    @Test void failingDisconnectListenerCannotPreventOtherListenersOrHealthyClients() throws Exception {
        var contexts = new LinkedBlockingQueue<ConnectionContext>();
        var closed = new LinkedBlockingQueue<UUID>();
        try (NioServer server = new NioServer(0,
                (context, message) -> contexts.add(context), (context, chunk) -> {})) {
            server.addDisconnectListener(id -> { throw new IllegalStateException("private listener details"); });
            server.addDisconnectListener(closed::add);
            server.start();
            try (Socket departing = connect(server); Socket healthy = connect(server)) {
                write(departing);
                ConnectionContext first = next(contexts);
                departing.shutdownOutput();
                assertEquals(first.connectionId(), next(closed));
                assertEquals(-1, departing.getInputStream().read());
                write(healthy);
                assertNotEquals(first.connectionId(), next(contexts).connectionId());
                assertNull(server.failure());
            }
        }
    }

    @Test void sessionLookupTracksSecurityOwnedChangesAndNeverCrossesConnections() throws Exception {
        TestRegistry registry = new TestRegistry();
        var contexts = new LinkedBlockingQueue<ConnectionContext>();
        var sessions = new LinkedBlockingQueue<Optional<SessionView>>();
        try (NioServer server = new NioServer(0, (context, message) -> {
            sessions.add(context.session());
            contexts.add(context);
        }, (context, chunk) -> {}).withSessionRegistry(registry)) {
            server.start();
            try (Socket first = connect(server); Socket second = connect(server)) {
                write(first);
                ConnectionContext a = next(contexts);
                assertTrue(next(sessions).isEmpty());
                SessionView session = new SessionView(UUID.randomUUID(), "alice", a.connectionId());
                registry.entries.put(a.connectionId(), session);
                write(first);
                assertSame(a, next(contexts));
                assertEquals(Optional.of(session), next(sessions));
                write(second);
                ConnectionContext b = next(contexts);
                assertTrue(next(sessions).isEmpty());
                // Defensively reject an incorrectly associated snapshot returned by a provider.
                registry.entries.put(b.connectionId(), session);
                write(second);
                next(contexts);
                assertTrue(next(sessions).isEmpty());
                registry.entries.remove(a.connectionId());
                write(first);
                next(contexts);
                assertTrue(next(sessions).isEmpty());
                registry.entries.put(a.connectionId(), session);
                CompletableFuture<Boolean> closure = new CompletableFuture<>();
                assertTrue(server.selectorTasks().submit(() -> closure.complete(server.closeConnection(a.connectionId()))));
                assertTrue(closure.get(3, TimeUnit.SECONDS));
                assertTrue(a.session().isEmpty(), "Closed contexts cannot expose a stale session");
                assertNotSame(Thread.currentThread(), registry.lookupThread.get());
                assertEquals(Optional.of(session), registry.findByConnectionId(a.connectionId()),
                        "Core must not mutate the security-owned registry");
            }
        }
    }

    @Test void integrationConfigurationIsFrozenAfterStartupAndDuplicateListenersAreRejected() throws Exception {
        TestRegistry registry = new TestRegistry();
        java.util.function.Consumer<UUID> listener = id -> {};
        try (NioServer server = new NioServer(0)) {
            server.addDisconnectListener(listener);
            assertThrows(IllegalArgumentException.class, () -> server.addDisconnectListener(listener));
            server.withSessionRegistry(registry);
            server.start();
            assertThrows(IllegalStateException.class, () -> server.addDisconnectListener(id -> {}));
            assertThrows(IllegalStateException.class, () -> server.withSessionRegistry(registry));
        }
    }

    /** Test fixture only: no production registry, authentication, or revocation implementation. */
    private static final class TestRegistry implements SessionRegistry {
        final ConcurrentHashMap<UUID, SessionView> entries = new ConcurrentHashMap<>();
        final AtomicReference<Thread> lookupThread = new AtomicReference<>();
        @Override public Optional<SessionView> findByConnectionId(UUID id) {
            lookupThread.set(Thread.currentThread());
            return Optional.ofNullable(entries.get(id));
        }
        @Override public Optional<SessionView> findByClientId(UUID id) {
            return entries.values().stream().filter(session -> session.clientId().equals(id)).findFirst();
        }
        @Override public List<SessionView> activeSessions() { return List.copyOf(entries.values()); }
    }
}
