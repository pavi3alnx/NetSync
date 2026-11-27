package com.netsync.gui;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/** Headless unit tests for {@link UiEventDispatcher}. */
class UiEventDispatcherTest {

    @Test
    void coalescesPresenceAndTransferProgressWhilePreservingDistinctMessages() {
        ArrayDeque<Runnable> pendingDrains = new ArrayDeque<>();
        List<String> executed = new ArrayList<>();
        AtomicLong overflows = new AtomicLong();

        try (UiEventDispatcher dispatcher = new UiEventDispatcher(pendingDrains::addLast, 16, overflows::addAndGet)) {
            UUID gen = UUID.randomUUID();
            UUID userA = UUID.randomUUID();
            UUID transfer1 = UUID.randomUUID();
            dispatcher.setActiveGeneration(gen);

            assertTrue(dispatcher.postImportant(gen, () -> executed.add("msg-1")));
            assertTrue(dispatcher.postPresence(gen, userA, () -> executed.add("presence-old")));
            assertTrue(dispatcher.postPresence(gen, userA, () -> executed.add("presence-new")));
            assertTrue(dispatcher.postTransferProgress(gen, transfer1, () -> executed.add("prog-10")));
            assertTrue(dispatcher.postTransferProgress(gen, transfer1, () -> executed.add("prog-90")));
            assertTrue(dispatcher.postImportant(gen, () -> executed.add("msg-2")));

            assertEquals(1, pendingDrains.size(), "At most one drain task should be scheduled");
            while (!pendingDrains.isEmpty()) {
                pendingDrains.pollFirst().run();
            }

            assertEquals(List.of("msg-1", "presence-new", "prog-90", "msg-2"), executed);
            assertEquals(0L, overflows.get());
        }
    }

    @Test
    void evictsCoalescableBeforeDroppingImportantAndReportsOverflow() {
        ArrayDeque<Runnable> pendingDrains = new ArrayDeque<>();
        List<String> executed = new ArrayList<>();
        AtomicLong overflows = new AtomicLong();

        try (UiEventDispatcher dispatcher = new UiEventDispatcher(pendingDrains::addLast, 2, overflows::addAndGet)) {
            UUID gen = UUID.randomUUID();
            dispatcher.setActiveGeneration(gen);

            assertTrue(dispatcher.postPresence(gen, UUID.randomUUID(), () -> executed.add("presence-evicted")));
            assertTrue(dispatcher.postImportant(gen, () -> executed.add("imp-1")));
            // Capacity is 2; adding imp-2 should evict the coalescable presence event instead of dropping imp-2.
            assertTrue(dispatcher.postImportant(gen, () -> executed.add("imp-2")));
            // Queue now holds 2 important events; imp-3 overflows and increments overflow count.
            assertFalse(dispatcher.postImportant(gen, () -> executed.add("imp-3")));

            while (!pendingDrains.isEmpty()) {
                pendingDrains.pollFirst().run();
            }

            assertEquals(List.of("imp-1", "imp-2"), executed);
            assertEquals(1L, overflows.get());
        }
    }

    @Test
    void filtersStaleGenerationsAndClosedDispatcher() {
        ArrayDeque<Runnable> pendingDrains = new ArrayDeque<>();
        List<String> executed = new ArrayList<>();

        UiEventDispatcher dispatcher = new UiEventDispatcher(pendingDrains::addLast, 16, ignored -> {});
        UUID gen1 = UUID.randomUUID();
        UUID gen2 = UUID.randomUUID();
        dispatcher.setActiveGeneration(gen1);

        assertTrue(dispatcher.postImportant(gen1, () -> executed.add("stale-queued")));
        assertFalse(dispatcher.postImportant(gen2, () -> executed.add("wrong-gen")));

        // Switching generation purges queued gen1 events.
        dispatcher.setActiveGeneration(gen2);
        assertTrue(dispatcher.postImportant(gen2, () -> executed.add("gen2-ok")));

        while (!pendingDrains.isEmpty()) {
            pendingDrains.pollFirst().run();
        }
        assertEquals(List.of("gen2-ok"), executed);

        dispatcher.close();
        assertTrue(dispatcher.isClosed());
        assertFalse(dispatcher.postImportant(gen2, () -> executed.add("after-close")));
        assertFalse(dispatcher.postImportant(null, () -> executed.add("global-after-close")));
    }

    @Test
    void containsConsumerExceptionsWithoutStoppingSubsequentEvents() {
        List<String> executed = new ArrayList<>();
        try (UiEventDispatcher dispatcher = new UiEventDispatcher(Runnable::run, 16, ignored -> {})) {
            assertTrue(dispatcher.postImportant(null, () -> { throw new IllegalStateException("boom"); }));
            assertTrue(dispatcher.postImportant(null, () -> executed.add("survived")));

            assertEquals(List.of("survived"), executed);
            assertEquals(1L, dispatcher.consumerFailureCount());
        }
    }
}
