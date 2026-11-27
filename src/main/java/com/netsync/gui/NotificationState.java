package com.netsync.gui;

import java.util.Objects;
import java.util.Optional;

/**
 * Pure, deterministic notification state manager for the client workspace banner.
 * <p>
 * Tracks monotonic notification IDs so stale auto-dismiss timers can never clear newer
 * notifications, allows manual dismissal, and clears obsolete operation-specific notices
 * (such as Save As errors/successes) when the user performs unrelated actions.
 */
public final class NotificationState {
    public enum Category {
        GENERAL,
        SAVE_AS,
        CHAT,
        TRANSFER,
        CONNECTION
    }

    public enum Severity {
        INFO,
        WARNING,
        ERROR
    }

    public record ActiveNotice(long id, Category category, Severity severity, String message, int autoDismissSeconds) {}

    private long nextId = 1L;
    private ActiveNotice active;

    /** Shows a new notification and returns it with a unique monotonic ID. */
    public synchronized ActiveNotice show(Category category, Severity severity, String message) {
        Objects.requireNonNull(category, "category");
        Objects.requireNonNull(severity, "severity");
        String safeText = message == null || message.isBlank() ? "Notification" : message;
        int autoDismiss = computeAutoDismissSeconds(category, severity);
        active = new ActiveNotice(nextId++, category, severity, safeText, autoDismiss);
        return active;
    }

    /** Dismisses the currently visible notification unconditionally (manual dismiss or session reset). */
    public synchronized boolean dismissCurrent() {
        if (active == null) {
            return false;
        }
        active = null;
        return true;
    }

    /** Dismisses only if the given ID matches the currently visible notification (prevents stale timer races). */
    public synchronized boolean dismissIfMatches(long noticeId) {
        if (active != null && active.id() == noticeId) {
            active = null;
            return true;
        }
        return false;
    }

    /** Clears the notification only if it belongs to the specified obsolete operation category. */
    public synchronized boolean clearCategory(Category category) {
        if (active != null && active.category() == category) {
            active = null;
            return true;
        }
        return false;
    }

    public synchronized Optional<ActiveNotice> current() {
        return Optional.ofNullable(active);
    }

    private static int computeAutoDismissSeconds(Category category, Severity severity) {
        if (severity == Severity.INFO) {
            return 8;
        }
        if (category == Category.SAVE_AS) {
            return 10;
        }
        return 0; // Connection, auth, transfer, or general warnings/errors stay until dismissed or superseded.
    }
}
