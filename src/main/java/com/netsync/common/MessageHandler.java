package com.netsync.common;

/** Invoked on the Selector thread. Must not block or perform disk/credential work. */
@FunctionalInterface
public interface MessageHandler { void handle(ConnectionContext context, MessageEnvelope message); }
