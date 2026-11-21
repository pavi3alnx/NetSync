package com.netsync.presence;

import com.netsync.common.Heartbeat;
import com.netsync.common.HeartbeatCodec;
import com.netsync.common.ProtocolLimits;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Sends loopback UDP heartbeats for an authenticated session supplied by the caller. */
public final class HeartbeatSender implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(HeartbeatSender.class.getName());

    private final InetSocketAddress serverAddress;
    private final UUID clientId;
    private final String presenceToken;
    private final HeartbeatCodec codec = new HeartbeatCodec();
    private final long intervalNanos;
    private final AtomicLong nextSequence = new AtomicLong(1);
    private final AtomicReference<Thread> senderThread = new AtomicReference<>();
    private final Object lifecycleLock = new Object();

    private DatagramChannel channel;
    private ScheduledExecutorService scheduler;
    private boolean running;

    public HeartbeatSender(InetSocketAddress serverAddress, UUID clientId, String presenceToken) {
        this(serverAddress, clientId, presenceToken,
                Duration.ofSeconds(ProtocolLimits.HEARTBEAT_INTERVAL_SECONDS));
    }

    HeartbeatSender(
            InetSocketAddress serverAddress,
            UUID clientId,
            String presenceToken,
            Duration interval) {
        this.serverAddress = requireLoopback(serverAddress);
        this.clientId = Objects.requireNonNull(clientId);
        this.presenceToken = Objects.requireNonNull(presenceToken);
        this.intervalNanos = Objects.requireNonNull(interval).toNanos();
        if (intervalNanos <= 0) {
            throw new IllegalArgumentException("Heartbeat interval must be positive");
        }
        codec.encode(new Heartbeat(ProtocolLimits.VERSION, "HEARTBEAT", clientId, presenceToken, 1));
    }

    /** Starts immediate and periodic sends. Starting an active sender is a no-op. */
    public void start() throws IOException {
        synchronized (lifecycleLock) {
            if (running) {
                return;
            }
            if (nextSequence.get() < 0) {
                throw new IllegalStateException("Heartbeat sequence space is exhausted");
            }

            DatagramChannel openedChannel = DatagramChannel.open();
            ScheduledExecutorService openedScheduler = null;
            try {
                openedScheduler = Executors.newSingleThreadScheduledExecutor(task -> {
                    Thread thread = new Thread(task, "netsync-heartbeat-sender");
                    thread.setDaemon(true);
                    senderThread.set(thread);
                    return thread;
                });
                channel = openedChannel;
                scheduler = openedScheduler;
                running = true;
                openedScheduler.scheduleAtFixedRate(
                        this::sendNextHeartbeat,
                        0,
                        intervalNanos,
                        TimeUnit.NANOSECONDS);
            } catch (RuntimeException failure) {
                running = false;
                channel = null;
                scheduler = null;
                closeChannel(openedChannel);
                if (openedScheduler != null) {
                    openedScheduler.shutdownNow();
                }
                throw failure;
            }
        }
    }

    /** Stops sending and releases the UDP socket. The sender may be started again. */
    public void stop() {
        DatagramChannel channelToClose;
        ScheduledExecutorService schedulerToStop;
        Thread threadToJoin;
        synchronized (lifecycleLock) {
            running = false;
            channelToClose = channel;
            schedulerToStop = scheduler;
            threadToJoin = senderThread.get();
            channel = null;
            scheduler = null;
        }

        closeChannel(channelToClose);
        if (schedulerToStop != null) {
            schedulerToStop.shutdownNow();
            if (threadToJoin != null && threadToJoin != Thread.currentThread()) {
                try {
                    if (!schedulerToStop.awaitTermination(1, TimeUnit.SECONDS)) {
                        LOG.log(System.Logger.Level.WARNING, "Heartbeat sender did not stop promptly");
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public boolean isRunning() {
        synchronized (lifecycleLock) {
            return running;
        }
    }

    @Override
    public void close() {
        stop();
    }

    private void sendNextHeartbeat() {
        DatagramChannel currentChannel;
        synchronized (lifecycleLock) {
            if (!running || channel == null) {
                return;
            }
            currentChannel = channel;
        }

        long sequence = nextSequence.getAndIncrement();
        if (sequence < 0) {
            LOG.log(System.Logger.Level.ERROR, "Heartbeat sequence space is exhausted");
            stop();
            return;
        }

        try {
            byte[] datagram = codec.encode(new Heartbeat(
                    ProtocolLimits.VERSION,
                    "HEARTBEAT",
                    clientId,
                    presenceToken,
                    sequence));
            int sent = currentChannel.send(ByteBuffer.wrap(datagram), serverAddress);
            if (sent != datagram.length && isRunning()) {
                LOG.log(System.Logger.Level.WARNING, "Heartbeat datagram was not fully sent");
            }
        } catch (IOException | RuntimeException failure) {
            if (isRunning()) {
                LOG.log(System.Logger.Level.WARNING,
                        "Heartbeat send failed (" + failure.getClass().getName() + ")");
            }
        }
    }

    private static InetSocketAddress requireLoopback(InetSocketAddress address) {
        Objects.requireNonNull(address);
        InetAddress ip = address.getAddress();
        if (address.isUnresolved() || ip == null || !ip.isLoopbackAddress()
                || address.getPort() < 1 || address.getPort() > 65535) {
            throw new IllegalArgumentException("Heartbeat destination must be a resolved loopback address and valid port");
        }
        return address;
    }

    private static void closeChannel(DatagramChannel channel) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException failure) {
            LOG.log(System.Logger.Level.WARNING,
                    "Heartbeat channel close failed (" + failure.getClass().getName() + ")");
        }
    }
}
