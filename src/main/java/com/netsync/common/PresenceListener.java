package com.netsync.common;

import java.util.UUID;
/** Receives transitions only; enqueue Selector work, never write sockets here.
 * Capture username before disconnect removes identity, for the final update. */
@FunctionalInterface
public interface PresenceListener { void onPresenceChanged(UUID clientId, PresenceState state); }
