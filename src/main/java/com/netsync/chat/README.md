# NetSync — TCP Chat and Messaging (`com.netsync.chat`)

**Module Owner:** Member 2 (Sachin Silva)  
**Branch:** `feature/tcp-chat`  
**Protocol Version:** 1 (Strict JSON over length-prefixed TCP)

---

## 1. Module Overview

The `com.netsync.chat` package provides private and direct real-time messaging between authenticated NetSync clients. It operates strictly within the non-blocking Java NIO architecture, using the central `NioServer` event loop and `OutboundSink` without opening competing sockets or blocking threads.

```
                          [ Client A ]
                               |
                        CHAT_SEND Frame
                               v
                     +-------------------+
                     |  MessageDispatcher|
                     +-------------------+
                               |
                               v
                     +-------------------+
                     |    ChatHandler    | <--- AuthorizationService (Verifies A)
                     +-------------------+
                               |
                               v
                     +-------------------+
                     |    ChatService    | <--- Validation (Non-empty, <= 4096 bytes)
                     +-------------------+
                               |
                               v
                     +-------------------+
                     |   MessageRouter   | <--- SessionRegistry (Resolves B)
                     +-------------------+
                       /               \
        CHAT_DELIVER  /                 \  ACK [QUEUED]
                     v                   v
             [ OutboundSink ]     [ OutboundSink ]
               (to Client B)       (to Client A)
```

---

## 2. Component Architecture & Class Responsibilities

| Class | Responsibility | Threading / Concurrency |
| :--- | :--- | :--- |
| **`ChatHandler`** | Implements `com.netsync.common.MessageHandler`. Entry point for `MessageType.CHAT_SEND`. Rejects unauthenticated connections and invalid requests. | Runs synchronously on the single NIO Selector thread. Never blocks. |
| **`ChatService`** | Business logic coordinator. Validates message content (non-null, non-empty, max 4,096 UTF-8 bytes). Dispatches to `MessageRouter` and formats correlated `ACK` or `ERROR` frames. | Runs synchronously on the Selector thread. Pure in-memory operations. |
| **`MessageRouter`** | Resolves `recipientClientId` to `SessionView` via `SessionRegistry`. Constructs `CHAT_DELIVER` frames and dispatches them via `OutboundSink`. Handles queue saturation. | Synchronous, non-blocking. Isolates slow consumers by catching queue saturation. |
| **`ChatClientAdapter`** | Client-side integration for CLI shells (`NetSyncClient`). Registers the `/chat` command and listens for incoming `CHAT_DELIVER`, `ACK`, and `ERROR` responses with timestamps (`HH:mm:ss`). | Thread-safe; handles dispatch on client background thread. |

---

## 3. Protocol & Wire Frames

All chat frames share the standard NetSync length-prefixed format:  
`[4-byte big-endian length N][1-byte kind = 0x01][N-1 bytes UTF-8 JSON]`

### A. Direct Message Request (`CHAT_SEND`)
Client sends to Server:
```json
{
  "version": 1,
  "type": "CHAT_SEND",
  "messageId": "550e8400-e29b-41d4-a716-446655440004",
  "body": {
    "recipientClientId": "550e8400-e29b-41d4-a716-446655440005",
    "text": "Hello Bob!"
  }
}
```
*Note: Sender identity is intentionally omitted from the wire body. The server derives sender identity strictly from the authenticated connection session.*

### B. Message Delivery (`CHAT_DELIVER`)
Server sends to Recipient:
```json
{
  "version": 1,
  "type": "CHAT_DELIVER",
  "messageId": "550e8400-e29b-41d4-a716-446655440007",
  "body": {
    "senderClientId": "550e8400-e29b-41d4-a716-446655440003",
    "text": "Hello Bob!",
    "sourceMessageId": "550e8400-e29b-41d4-a716-446655440004"
  }
}
```

### C. Delivery Acknowledgement (`ACK`)
Server sends to Sender upon enqueue acceptance:
```json
{
  "version": 1,
  "type": "ACK",
  "messageId": "550e8400-e29b-41d4-a716-446655440006",
  "replyTo": "550e8400-e29b-41d4-a716-446655440004",
  "body": {
    "operation": "CHAT_SEND",
    "status": "QUEUED"
  }
}
```
*Semantics: `ACK/QUEUED` guarantees that the recipient's output buffer accepted the message. It does not represent a read receipt.*

### D. Error Responses (`ERROR`)
Server sends to Sender on failure:
- **`RECIPIENT_UNAVAILABLE`**: Recipient client ID is not found or not active in `SessionRegistry`.
- **`BUSY`**: Recipient output queue is full; sender is informed without blocking other peers.
- **`UNAUTHENTICATED`**: Connection has no valid session or session is inactive.
- **`INVALID_REQUEST`**: Text is empty, null, or malformed.
- **`SIZE_LIMIT`**: Text exceeds 4,096 UTF-8 bytes.

---

## 4. Concurrency & Safety Guarantees

1. **Non-Blocking Execution**: `ChatHandler` and `MessageRouter` execute in sub-millisecond in-memory lookups. No file I/O, database access, or sleeps are allowed.
2. **Slow Consumer Isolation**: If a recipient's network buffer is saturated (`EnqueueResult.REJECTED`), `MessageRouter` immediately aborts delivery and returns `BUSY` to the sender. The NIO Selector loop is never blocked.
3. **Session Integrity**: Sender ID is strictly populated from `SessionView.clientId()`. Identity spoofing via JSON payload injection is impossible.
4. **Advisory Offline State**: Per `architecture.md`, UDP presence timeouts (marking a user `OFFLINE`) are advisory. If a user's TCP connection is active, direct chat remains fully routable.

---

## 5. Verification & Tests

To execute all chat module tests:
```bash
mvn test -Dtest="*Chat*,MessageRouterTest"
```

To run the complete project verification:
```bash
mvn clean test
```
