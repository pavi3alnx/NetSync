package com.netsync.transfer;

import com.netsync.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shared test fixtures and controllable doubles for NetSync transfer unit and integration tests.
 */
public final class TransferTestFixtures {

    private TransferTestFixtures() {}

    /**
     * Controllable implementation of {@link ConnectionContext}.
     */
    public static final class TestConnectionContext implements ConnectionContext {
        private final UUID connectionId;
        private SessionView session;

        public TestConnectionContext(UUID connectionId, SessionView session) {
            this.connectionId = Objects.requireNonNull(connectionId);
            this.session = session;
        }

        public static TestConnectionContext create(String username) {
            UUID connId = UUID.randomUUID();
            UUID clientId = UUID.randomUUID();
            SessionView s = new SessionView(clientId, username, connId);
            return new TestConnectionContext(connId, s);
        }

        public static TestConnectionContext unauthenticated() {
            return new TestConnectionContext(UUID.randomUUID(), null);
        }

        @Override
        public UUID connectionId() {
            return connectionId;
        }

        @Override
        public Optional<SessionView> session() {
            return Optional.ofNullable(session);
        }

        public void setSession(SessionView session) {
            this.session = session;
        }
    }

    /**
     * In-memory implementation of {@link SessionRegistry}.
     */
    public static final class TestSessionRegistry implements SessionRegistry {
        private final Map<UUID, SessionView> byConnection = new ConcurrentHashMap<>();
        private final Map<UUID, SessionView> byClient = new ConcurrentHashMap<>();

        public void register(SessionView session) {
            byConnection.put(session.connectionId(), session);
            byClient.put(session.clientId(), session);
        }

        public void unregister(UUID connectionId) {
            SessionView s = byConnection.remove(connectionId);
            if (s != null) {
                byClient.remove(s.clientId());
            }
        }

        @Override
        public Optional<SessionView> findByConnectionId(UUID id) {
            return Optional.ofNullable(byConnection.get(id));
        }

        @Override
        public Optional<SessionView> findByClientId(UUID id) {
            return Optional.ofNullable(byClient.get(id));
        }

        @Override
        public List<SessionView> activeSessions() {
            return List.copyOf(byConnection.values());
        }
    }

    /**
     * Controllable implementation of {@link AuthorizationService}.
     */
    public static final class TestAuthorizationService implements AuthorizationService {
        private final Set<UUID> inactiveClientIds = ConcurrentHashMap.newKeySet();

        public void setInactive(UUID clientId) {
            inactiveClientIds.add(clientId);
        }

        public void setActive(UUID clientId) {
            inactiveClientIds.remove(clientId);
        }

        @Override
        public Optional<SessionView> authenticatedSession(ConnectionContext context) {
            return context.session().filter(s -> !inactiveClientIds.contains(s.clientId()));
        }

        @Override
        public boolean isActive(SessionView session) {
            return session != null && !inactiveClientIds.contains(session.clientId());
        }
    }

    /**
     * Controllable {@link OutboundSink} capturing sent frames with per-connection blocking queues.
     */
    public static final class TestOutboundSink implements OutboundSink {
        private final Map<UUID, BlockingQueue<OutboundFrame>> sentFrames = new ConcurrentHashMap<>();
        private final Set<UUID> rejectingConnections = ConcurrentHashMap.newKeySet();
        private volatile boolean rejectAll = false;

        public void setRejecting(UUID connectionId, boolean rejecting) {
            if (rejecting) rejectingConnections.add(connectionId);
            else rejectingConnections.remove(connectionId);
        }

        public void setRejectAll(boolean rejectAll) {
            this.rejectAll = rejectAll;
        }

        @Override
        public EnqueueResult send(UUID connectionId, OutboundFrame frame) {
            if (rejectAll || rejectingConnections.contains(connectionId)) {
                return EnqueueResult.REJECTED;
            }
            sentFrames.computeIfAbsent(connectionId, k -> new LinkedBlockingQueue<>()).offer(frame);
            return EnqueueResult.ACCEPTED;
        }

        public OutboundFrame poll(UUID connectionId, long timeout, TimeUnit unit) throws InterruptedException {
            BlockingQueue<OutboundFrame> q = sentFrames.computeIfAbsent(connectionId, k -> new LinkedBlockingQueue<>());
            return q.poll(timeout, unit);
        }

        public OutboundFrame poll(UUID connectionId) {
            BlockingQueue<OutboundFrame> q = sentFrames.get(connectionId);
            return q != null ? q.poll() : null;
        }

        public List<OutboundFrame> getAllFrames(UUID connectionId) {
            BlockingQueue<OutboundFrame> q = sentFrames.get(connectionId);
            return q != null ? List.copyOf(q) : List.of();
        }

        public void clear() {
            sentFrames.clear();
            rejectingConnections.clear();
            rejectAll = false;
        }
    }

    /**
     * Direct/synchronous implementation of {@link SelectorTasks} for deterministic unit testing.
     */
    public static final class TestDirectSelectorTasks implements SelectorTasks {
        private final Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();
        private volatile boolean rejectReservations = false;
        private final AtomicBoolean reservationHeld = new AtomicBoolean(false);

        public void setRejectReservations(boolean reject) {
            this.rejectReservations = reject;
        }

        @Override
        public boolean submit(Runnable task) {
            Objects.requireNonNull(task);
            taskQueue.add(task);
            task.run();
            return true;
        }

        @Override
        public Optional<Reservation> tryReserveCompletion() {
            if (rejectReservations) {
                return Optional.empty();
            }
            reservationHeld.set(true);
            return Optional.of(new Reservation() {
                private final AtomicBoolean consumed = new AtomicBoolean(false);

                @Override
                public void submit(Runnable completion) {
                    if (consumed.getAndSet(true)) {
                        throw new IllegalStateException("Reservation already consumed");
                    }
                    reservationHeld.set(false);
                    taskQueue.add(completion);
                    completion.run();
                }

                @Override
                public void close() {
                    reservationHeld.set(false);
                }
            });
        }

        public boolean isReservationHeld() {
            return reservationHeld.get();
        }
    }

    /**
     * Lightweight chat handler fixture conforming to {@link MessageHandler} for concurrent chat testing.
     */
    public static final class TestChatHandler implements MessageHandler {
        private final OutboundSink sink;
        private final SessionRegistry sessions;
        private final List<MessageEnvelope> deliveredChats = Collections.synchronizedList(new ArrayList<>());

        public TestChatHandler(OutboundSink sink, SessionRegistry sessions) {
            this.sink = sink;
            this.sessions = sessions;
        }

        @Override
        public void handle(ConnectionContext context, MessageEnvelope message) {
            if (message.type() != MessageType.CHAT_SEND) return;
            if (!(message.body() instanceof MessageBody.ChatSend chat)) return;

            Optional<SessionView> senderOpt = context.session();
            if (senderOpt.isEmpty()) return;

            Optional<SessionView> recipientOpt = sessions.findByClientId(chat.recipientClientId());
            if (recipientOpt.isPresent()) {
                MessageEnvelope deliver = new MessageEnvelope(
                        1,
                        MessageType.CHAT_DELIVER,
                        UUID.randomUUID(),
                        null,
                        new MessageBody.ChatDeliver(senderOpt.get().clientId(), chat.text(), message.messageId())
                );
                sink.send(recipientOpt.get().connectionId(), deliver);
                deliveredChats.add(deliver);

                MessageEnvelope ack = new MessageEnvelope(
                        1,
                        MessageType.ACK,
                        UUID.randomUUID(),
                        message.messageId(),
                        new MessageBody.Ack(MessageType.CHAT_SEND, AckStatus.QUEUED, null)
                );
                sink.send(context.connectionId(), ack);
            }
        }

        public List<MessageEnvelope> deliveredChats() {
            return List.copyOf(deliveredChats);
        }
    }
}
