package com.netsync.client;

import com.netsync.common.*;
import org.junit.jupiter.api.*;
import java.util.*;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;

class ClientDispatcherTest {
    @Test void routesByTypeInRegistrationOrderAndIsolatesFailures() {
        try (ClientDispatcher dispatcher = new ClientDispatcher()) {
            List<Integer> calls = new ArrayList<>();
            dispatcher.register(MessageType.PRESENCE_QUERY, m -> calls.add(1));
            dispatcher.register(MessageType.PRESENCE_QUERY, m -> { throw new IllegalStateException("secret"); });
            dispatcher.register(MessageType.PRESENCE_QUERY, m -> calls.add(2));
            dispatcher.register(MessageType.LOGOUT_REQUEST, m -> fail("Wrong route"));
            assertTrue(dispatcher.dispatch(TcpClientTest.message()));
            assertEquals(List.of(1, 2), calls); assertEquals(1, dispatcher.handlerFailures());
        }
    }
    @Test void duplicateIdentityRejectedAndRegistrationCanBeClosedAndReadded() {
        try (ClientDispatcher dispatcher = new ClientDispatcher()) {
            List<MessageEnvelope> received = new ArrayList<>(); Consumer<MessageEnvelope> handler = received::add;
            var registration = dispatcher.register(MessageType.PRESENCE_QUERY, handler);
            assertThrows(IllegalArgumentException.class, () -> dispatcher.register(MessageType.PRESENCE_QUERY, handler));
            registration.close(); registration.close();
            dispatcher.dispatch(TcpClientTest.message()); assertTrue(received.isEmpty());
            dispatcher.register(MessageType.PRESENCE_QUERY, handler);
            dispatcher.dispatch(TcpClientTest.message()); assertEquals(1, received.size());
        }
    }
    @Test void unhandledJsonCountedAndBinaryRequiresReceiver() {
        try (ClientDispatcher dispatcher = new ClientDispatcher()) {
            assertTrue(dispatcher.dispatch(TcpClientTest.message())); assertEquals(1, dispatcher.unhandledMessages());
            var chunk = new FileChunk(UUID.randomUUID(), UUID.randomUUID(), 0, new byte[]{1});
            assertFalse(dispatcher.dispatch(chunk));
            List<FileChunk> received = new ArrayList<>(); Consumer<FileChunk> handler = received::add;
            var registration = dispatcher.registerBinary(handler);
            assertThrows(IllegalArgumentException.class, () -> dispatcher.registerBinary(handler));
            assertTrue(dispatcher.dispatch(chunk)); assertEquals(List.of(chunk), received);
            registration.close(); assertFalse(dispatcher.dispatch(chunk));
        }
    }
    @Test void removalDuringDispatchPreventsLaterInvocationAndCloseIsTerminal() {
        ClientDispatcher dispatcher = new ClientDispatcher();
        ClientDispatcher.Registration[] later = new ClientDispatcher.Registration[1];
        dispatcher.register(MessageType.PRESENCE_QUERY, m -> later[0].close());
        later[0] = dispatcher.register(MessageType.PRESENCE_QUERY, m -> fail("Removed callback"));
        dispatcher.dispatch(TcpClientTest.message()); dispatcher.close(); dispatcher.close();
        assertThrows(IllegalStateException.class, () -> dispatcher.registerBinary(c -> {}));
        assertThrows(IllegalArgumentException.class, () -> dispatcher.register(MessageType.FILE_CHUNK, m -> {}));
    }
}
