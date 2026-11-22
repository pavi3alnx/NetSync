package com.netsync.server;

import com.netsync.common.ConnectionContext;
import com.netsync.common.FileChunk;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/** Test-only bridge to the core's existing package-private clock seam. */
public final class MonotonicServerTestSupport {
    private MonotonicServerTestSupport() {}
    public static NioServer create(int port, NioServer.HandlerFactory handlers,
                                   BiConsumer<ConnectionContext, FileChunk> chunks, LongSupplier clock) {
        return new NioServer(port, handlers, chunks, clock);
    }
}
