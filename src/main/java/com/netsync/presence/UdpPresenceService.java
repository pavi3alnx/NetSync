package com.netsync.presence;

import com.netsync.common.HeartbeatCodec;
import com.netsync.common.ProtocolLimits;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class UdpPresenceService {
    private static final InetSocketAddress DEFAULT_BIND_ADDRESS =
            new InetSocketAddress("127.0.0.1", ProtocolLimits.UDP_PORT);

    private final InetSocketAddress bindAddress;
    private final HeartbeatTracker tracker;
    private final HeartbeatCodec heartbeatCodec;
    private final ThreadFactory threadFactory;
    private final Runnable afterExpiryCheck;

    private final Object lifecycleLock = new Object();
    private volatile DatagramChannel channel;
    private volatile Thread receiveThread;
    private volatile ScheduledExecutorService expiryExecutor;
    private volatile boolean running;
    private final AtomicLong decodedHeartbeats = new AtomicLong();

    public UdpPresenceService(HeartbeatTracker tracker) {
        this(DEFAULT_BIND_ADDRESS, tracker);
    }

    public UdpPresenceService(InetSocketAddress bindAddress, HeartbeatTracker tracker) {
        this(bindAddress, tracker, () -> {});
    }

    public UdpPresenceService(InetSocketAddress bindAddress, HeartbeatTracker tracker, Runnable afterExpiryCheck) {
        this.bindAddress = Objects.requireNonNull(bindAddress);
        this.tracker = Objects.requireNonNull(tracker);
        this.heartbeatCodec = new HeartbeatCodec();
        this.threadFactory = daemonThreadFactory("netsync-udp-presence-");
        this.afterExpiryCheck = Objects.requireNonNull(afterExpiryCheck);
        InetAddress address = bindAddress.getAddress();
        if (bindAddress.isUnresolved()
                || address == null
                || !address.isLoopbackAddress()) {
            throw new IllegalArgumentException("UDP presence binding must use an approved loopback address");
        }
    }

    public UdpPresenceService(HeartbeatTracker tracker, InetSocketAddress bindAddress) {
        this(bindAddress, tracker);
    }

    /** Reserve UDP before TCP starts accepting; no receiver/scheduler runs until start(). */
    public void bind() throws IOException {
        synchronized (lifecycleLock) {
            if (channel != null) return;
            DatagramChannel openedChannel = DatagramChannel.open();
            try {
                openedChannel.setOption(StandardSocketOptions.SO_REUSEADDR, false);
                openedChannel.bind(bindAddress);
                openedChannel.configureBlocking(true);
            } catch (IOException exception) {
                try {
                    openedChannel.close();
                } catch (IOException ignored) {
                    // Startup failure cleanup is best-effort.
                }
                throw exception;
            }
            channel = openedChannel;
        }
    }

    public void start() throws IOException {
        synchronized (lifecycleLock) {
            if (running) return;
            bind();
            running = true;
            try {
                expiryExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
                expiryExecutor.scheduleAtFixedRate(
                        () -> {
                            if (running) {
                                try { tracker.checkTimeouts(); }
                                finally { afterExpiryCheck.run(); }
                            }
                        },
                        ProtocolLimits.PRESENCE_CHECK_SECONDS,
                        ProtocolLimits.PRESENCE_CHECK_SECONDS,
                        TimeUnit.SECONDS);

                Thread startedReceiveThread = new Thread(
                        this::receiveLoop,
                        "netsync-udp-presence-receiver");
                startedReceiveThread.setDaemon(true);
                receiveThread = startedReceiveThread;
                startedReceiveThread.start();
            } catch (RuntimeException failure) {
                running = false;
                if (expiryExecutor != null) expiryExecutor.shutdownNow();
                expiryExecutor = null;
                try { channel.close(); } catch (IOException ignored) { }
                channel = null; receiveThread = null;
                throw failure;
            }
        }
    }

    public void stop() {
        DatagramChannel channelToClose;
        ScheduledExecutorService executorToStop;
        Thread threadToStop;

        synchronized (lifecycleLock) {
            running = false;
            channelToClose = channel;
            channel = null;
            executorToStop = expiryExecutor;
            expiryExecutor = null;
            threadToStop = receiveThread;
            receiveThread = null;
        }

        if (channelToClose != null) {
            try {
                channelToClose.close();
            } catch (IOException ignored) {
                // Channel closure is idempotent for lifecycle purposes.
            }
        }

        if (executorToStop != null) {
            executorToStop.shutdownNow();
            try { executorToStop.awaitTermination(1, TimeUnit.SECONDS); }
            catch (InterruptedException exception) { Thread.currentThread().interrupt(); }
        }

        if (threadToStop != null) {
            threadToStop.interrupt();
            try {
                threadToStop.join(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isRunning() {
        DatagramChannel currentChannel = channel;
        return running
                && currentChannel != null
                && currentChannel.isOpen();
    }

    public long decodedHeartbeatCountForTesting() { return decodedHeartbeats.get(); }

    public InetSocketAddress getLocalAddress() {
        DatagramChannel currentChannel = channel;
        if (currentChannel == null) {
            throw new IllegalStateException("UDP presence service is not started");
        }

        try {
            SocketAddress address = currentChannel.getLocalAddress();
            if (address instanceof InetSocketAddress socketAddress) {
                return socketAddress;
            }
        } catch (IOException ignored) {
            // The configured address is not available after closure.
        }
        throw new IllegalStateException("UDP presence service has no local address");
    }

    private void receiveLoop() {
        DatagramChannel currentChannel = channel;
        while (running && currentChannel != null && currentChannel.isOpen()) {
            ByteBuffer buffer = ByteBuffer.allocate(ProtocolLimits.HEARTBEAT_RECEIVE_BYTES);
            try {
                SocketAddress sender = currentChannel.receive(buffer);
                if (sender == null || !running) {
                    continue;
                }

                int receivedBytes = buffer.position();
                if (receivedBytes > ProtocolLimits.MAX_HEARTBEAT_BYTES) {
                    continue;
                }

                byte[] datagram = new byte[receivedBytes];
                buffer.flip();
                buffer.get(datagram);
                heartbeatCodec.decode(datagram).ifPresent(heartbeat -> {
                    tracker.processHeartbeat(heartbeat);
                    decodedHeartbeats.incrementAndGet();
                });
            } catch (ClosedChannelException exception) {
                break;
            } catch (IOException exception) {
                if (running) {
                    continue;
                }
                break;
            } catch (RuntimeException ignored) {
                // Malformed datagram handling must not terminate the receive loop.
            }
        }
    }

    private static ThreadFactory daemonThreadFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
