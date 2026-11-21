package com.netsync.common;

import java.util.UUID;

/** Selector-thread connection control for closing only after queued output drains. */
@FunctionalInterface
public interface ConnectionCloser {
    boolean closeAfterDrain(UUID connectionId);
}
