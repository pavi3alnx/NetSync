package com.netsync.gui;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Deterministic headless tests for {@link NotificationState} lifecycle and stale-timer protection. */
class NotificationStateTest {

    @Test
    void assignsMonotonicIdsAndPreventsStaleTimerFromClearingNewerNotice() {
        NotificationState state = new NotificationState();

        NotificationState.ActiveNotice first = state.show(
                NotificationState.Category.SAVE_AS, NotificationState.Severity.ERROR, "Save As failed: exists");
        assertTrue(first.autoDismissSeconds() > 0);
        assertEquals(first, state.current().orElseThrow());

        NotificationState.ActiveNotice second = state.show(
                NotificationState.Category.CONNECTION, NotificationState.Severity.ERROR, "Connection lost");
        assertTrue(second.id() > first.id());
        assertEquals(0, second.autoDismissSeconds(), "Connection errors must not silently auto-dismiss");

        // Stale timer callback for first notice must NOT clear the newer connection error.
        assertFalse(state.dismissIfMatches(first.id()));
        assertEquals(second, state.current().orElseThrow());

        // Matching ID dismisses cleanly.
        assertTrue(state.dismissIfMatches(second.id()));
        assertTrue(state.current().isEmpty());
    }

    @Test
    void clearsObsoleteSaveAsNoticeWithoutSuppressingUnrelatedConnectionErrors() {
        NotificationState state = new NotificationState();

        state.show(NotificationState.Category.SAVE_AS, NotificationState.Severity.ERROR, "Refusing to overwrite");
        assertTrue(state.clearCategory(NotificationState.Category.SAVE_AS));
        assertTrue(state.current().isEmpty());

        NotificationState.ActiveNotice connErr = state.show(
                NotificationState.Category.CONNECTION, NotificationState.Severity.ERROR, "Server disconnected");
        assertFalse(state.clearCategory(NotificationState.Category.SAVE_AS));
        assertEquals(connErr, state.current().orElseThrow());

        assertTrue(state.dismissCurrent());
        assertTrue(state.current().isEmpty());
    }
}
