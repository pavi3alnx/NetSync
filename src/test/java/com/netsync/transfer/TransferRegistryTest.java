package com.netsync.transfer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TransferRegistryTest {

    private TransferSession createSession(UUID sender, UUID recipient) {
        return new TransferSession(
                UUID.randomUUID(),
                sender,
                recipient,
                "test.bin",
                1024,
                UUID.randomUUID(),
                System.currentTimeMillis()
        );
    }

    @Test
    @DisplayName("Enforces maximum global active transfers limit of 10")
    void globalTransferLimitEnforcement() {
        TransferRegistry registry = new TransferRegistry(10, 1);

        for (int i = 0; i < 10; i++) {
            TransferSession session = createSession(UUID.randomUUID(), UUID.randomUUID());
            registry.registerOffer(session);
        }

        assertEquals(10, registry.activeCount());

        TransferSession eleventh = createSession(UUID.randomUUID(), UUID.randomUUID());
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> registry.registerOffer(eleventh));
        assertTrue(e.getMessage().contains("Global active transfer limit"));
    }

    @Test
    @DisplayName("Enforces maximum 1 active transfer per client for sender and recipient")
    void perClientLimitEnforcement() {
        TransferRegistry registry = new TransferRegistry(10, 1);
        UUID clientA = UUID.randomUUID();
        UUID clientB = UUID.randomUUID();
        UUID clientC = UUID.randomUUID();

        TransferSession session1 = createSession(clientA, clientB);
        registry.registerOffer(session1);

        assertTrue(registry.isClientActive(clientA));
        assertTrue(registry.isClientActive(clientB));
        assertFalse(registry.isClientActive(clientC));

        // Client A cannot start another transfer as sender
        TransferSession session2 = createSession(clientA, clientC);
        IllegalStateException e1 = assertThrows(IllegalStateException.class, () -> registry.registerOffer(session2));
        assertTrue(e1.getMessage().contains("Sender client"));

        // Client B cannot start another transfer as recipient
        TransferSession session3 = createSession(clientC, clientB);
        IllegalStateException e2 = assertThrows(IllegalStateException.class, () -> registry.registerOffer(session3));
        assertTrue(e2.getMessage().contains("Recipient client"));

        // Client A cannot participate as recipient in another transfer
        TransferSession session4 = createSession(clientC, clientA);
        assertThrows(IllegalStateException.class, () -> registry.registerOffer(session4));
    }

    @Test
    @DisplayName("Duplicate transfer ID is rejected")
    void duplicateTransferIdRejection() {
        TransferRegistry registry = new TransferRegistry(10, 1);
        UUID transferId = UUID.randomUUID();

        TransferSession s1 = new TransferSession(
                transferId, UUID.randomUUID(), UUID.randomUUID(), "file.txt", 100, UUID.randomUUID(), 1000L);
        registry.registerOffer(s1);

        TransferSession s2 = new TransferSession(
                transferId, UUID.randomUUID(), UUID.randomUUID(), "other.txt", 100, UUID.randomUUID(), 1000L);
        assertThrows(IllegalArgumentException.class, () -> registry.registerOffer(s2));
    }

    @Test
    @DisplayName("Removing a transfer releases global and client slots safely")
    void slotReleaseOnRemoval() {
        TransferRegistry registry = new TransferRegistry(10, 1);
        UUID clientA = UUID.randomUUID();
        UUID clientB = UUID.randomUUID();

        TransferSession session = createSession(clientA, clientB);
        registry.registerOffer(session);
        assertEquals(1, registry.activeCount());

        var removed = registry.remove(session.transferId());
        assertTrue(removed.isPresent());
        assertEquals(session.transferId(), removed.get().transferId());

        assertEquals(0, registry.activeCount());
        assertFalse(registry.isClientActive(clientA));
        assertFalse(registry.isClientActive(clientB));

        // Client A and B can now start another transfer
        TransferSession newSession = createSession(clientA, clientB);
        assertDoesNotThrow(() -> registry.registerOffer(newSession));
    }

    @Test
    @DisplayName("Lookups by client ID and connection ID")
    void lookupsByClientAndConnection() {
        TransferRegistry registry = new TransferRegistry(10, 1);
        UUID clientA = UUID.randomUUID();
        UUID clientB = UUID.randomUUID();
        UUID connA = UUID.randomUUID();
        UUID connB = UUID.randomUUID();

        TransferSession session = new TransferSession(
                UUID.randomUUID(), clientA, clientB, "data.bin", 500, connA, 1000L);
        session.setRecipientConnectionId(connB);
        registry.registerOffer(session);

        assertEquals(session, registry.findByClientId(clientA).orElseThrow());
        assertEquals(session, registry.findByClientId(clientB).orElseThrow());
        assertEquals(session, registry.findByConnectionId(connA).orElseThrow());
        assertEquals(session, registry.findByConnectionId(connB).orElseThrow());

        assertTrue(registry.findByClientId(UUID.randomUUID()).isEmpty());
        assertTrue(registry.findByConnectionId(UUID.randomUUID()).isEmpty());
    }

    @Test
    @DisplayName("Null arguments return empty and do not throw exceptions in lookups and removals")
    void nullArgumentsHandledSafely() {
        TransferRegistry registry = new TransferRegistry();
        assertTrue(registry.get(null).isEmpty());
        assertTrue(registry.findByClientId(null).isEmpty());
        assertTrue(registry.findByConnectionId(null).isEmpty());
        assertTrue(registry.remove(null).isEmpty());
        assertFalse(registry.isClientActive(null));
    }

    @Test
    @DisplayName("Finds expired transfers based on offer timeout and inactivity")
    void expiryScanning() {
        TransferRegistry registry = new TransferRegistry(10, 1);
        long startTime = 100_000L;
        long timeout = 30_000L;

        TransferSession session = new TransferSession(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "doc.pdf", 500, UUID.randomUUID(), startTime);
        registry.registerOffer(session);

        // Not expired at 29s
        assertTrue(registry.findExpiredTransfers(startTime + 29_000L, timeout).isEmpty());

        // Expired at 30s
        List<TransferSession> expired = registry.findExpiredTransfers(startTime + 30_000L, timeout);
        assertEquals(1, expired.size());
        assertEquals(session.transferId(), expired.get(0).transferId());
    }
}
