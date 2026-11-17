package com.netsync.common;

/** Wire limits and configurable runtime defaults from protocol version 1. */
public final class ProtocolLimits {
    private ProtocolLimits() {}
    public static final int VERSION=1, TCP_PORT=5050, UDP_PORT=5051;
    public static final String BIND_ADDRESS="127.0.0.1", STORAGE_ROOT="data/uploads/";
    public static final int MAX_JSON_BYTES=65_536, MIN_FRAME_LENGTH=2, MAX_FRAME_LENGTH=65_537;
    public static final byte JSON_KIND=1, CHUNK_KIND=2;
    public static final int MAX_CHUNK_BYTES=16_384, CHUNK_HEADER_BYTES=36, MAX_DEPTH=16;
    public static final int MAX_HEARTBEAT_BYTES=1_024, HEARTBEAT_RECEIVE_BYTES=1_025;
    public static final int HEARTBEAT_INTERVAL_SECONDS=5, PRESENCE_TIMEOUT_SECONDS=15, PRESENCE_CHECK_SECONDS=1;
    public static final int MAX_CHAT_BYTES=4_096, MAX_FILE_BYTES=10_485_760;
    public static final int MAX_OUTPUT_FRAMES=256, MAX_OUTPUT_BYTES=1_048_576, MAX_CONNECTIONS=100;
    public static final int LOGIN_TIMEOUT_SECONDS=10, FRAME_TIMEOUT_SECONDS=15, TRANSFER_TIMEOUT_SECONDS=30;
    public static final int WORKER_THREADS=2, WORKER_QUEUE_SIZE=32, SELECTOR_QUEUE_SIZE=256;
    public static final int MAX_RECENT_MESSAGE_IDS=1_024, MAX_CLIENT_TRANSFERS=1, MAX_TRANSFERS=10;
    public static final int SHUTDOWN_GRACE_SECONDS=5;
}
