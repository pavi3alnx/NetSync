/**
 * NetSync version 1 shared wire models and module ports. This package depends only
 * on the JDK and the approved Jackson library, never on implementation packages.
 *
 * <p>Transport: accumulate four prefix bytes, call FrameCodec.decodeLength before
 * allocating, inspect kind and call validateKindLength, then call decodePayload
 * with exactly N complete payload bytes. Retain partial buffers and outgoing
 * positions in transport code. Do not resynchronize after framing errors.
 *
 * <p>JSON: MessageCodec consumes body bytes without kind/prefix. ProtocolException
 * supplies safe error metadata and connection action. CLOSE has no error code;
 * ERROR_THEN_CLOSE requests best-effort ERROR before closure. KEEP_OPEN rejects
 * only the request. The caller generates fresh response UUIDs. Credentials and
 * raw parser input must never be logged. Models validate wire values at codec
 * boundaries; their constructors are not authorization checks.
 *
 * <p>Threading: dispatch, transfer coordination and OutboundSink run on the
 * Selector. Other threads use bounded SelectorTasks. Reserve completion capacity
 * before starting worker tasks and release reservations when work cannot start.
 * Implementations must document shutdown cancellation, never silently discard
 * credential/transfer results, and recheck active session/transfer state before
 * applying completions. Queue acceptance does not prove delivery.
 *
 * <p>Security owns SessionRegistry/AuthorizationService implementations and session
 * lifecycle. Chat resolves active recipients through those ports. Transfer checks
 * authenticated participant roles and current state internally. Presence retains
 * its own heartbeat timing; HeartbeatCodec only validates datagram syntax. Use a
 * 1,025-byte receive buffer, reject packets above 1,024 bytes, and silently drop
 * empty decode results. Token validity and strictly increasing sequence checks
 * require security/presence state. Capture final presence identity before registry
 * removal so a disconnect update can still include username.
 */
package com.netsync.common;
