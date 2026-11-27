package com.netsync.gui;

import com.netsync.common.PresenceState;
import com.netsync.common.SessionView;
import javafx.animation.KeyFrame;
import javafx.animation.PauseTransition;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.shape.Circle;
import javafx.stage.FileChooser;
import javafx.util.Duration;

import java.awt.Desktop;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

/** Main NetSync desktop workspace: live presence sidebar, private TCP chat, and verified file transfers. */
public final class ClientWorkspaceView extends BorderPane {
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    private final ClientSessionController controller;
    private final Timeline offerCountdownTimer;
    private final List<CountdownBinding> activeCountdowns = new ArrayList<>();
    private final NotificationState notificationState = new NotificationState();
    private PauseTransition noticeDismissTimer;

    private record CountdownBinding(ClientModels.TransferItem item, Label label) {}

    private SessionView activeSession;
    private UUID selectedPeerId;
    private String selectedPeerName;
    private PresenceState selectedPeerState;
    private boolean showTransfersTab;
    private boolean showTechnicalDetails;

    // Sidebar controls
    private final Label currentUserLabel = new Label("Not signed in");
    private final Label currentUserIdLabel = new Label();
    private final Label connectionBadge = new Label("CONNECTED");
    private final Button messagesNavBtn = new Button("Messages");
    private final Button transfersNavBtn = new Button("Transfers");
    private final Button refreshUsersBtn = new Button("Refresh");
    private final ListView<ClientModels.UserItem> userListView = new ListView<>();
    private final CheckBox detailsToggle = new CheckBox("Technical IDs");
    private final Button logoutBtn = new Button("Log Out");
    private final Button disconnectBtn = new Button("Disconnect");

    // Main center stack
    private final StackPane centerStack = new StackPane();
    private final BorderPane messagesPane = new BorderPane();
    private final BorderPane transfersPane = new BorderPane();

    // Messages view controls
    private final Label peerTitleLabel = new Label("Select a user to start chatting");
    private final Label peerStatusBadge = new Label();
    private final Button headerAttachBtn = new Button("Send File...");
    private final VBox activeOffersBanner = new VBox(8);
    private final VBox messagesBox = new VBox(8);
    private final ScrollPane messagesScroll = new ScrollPane(messagesBox);
    private final TextField composerField = new TextField();
    private final Button sendBtn = new Button("Send");
    private final Button attachBtn = new Button("Attach");

    // Transfers view controls
    private final VBox transfersListBox = new VBox(10);
    private final ScrollPane transfersScroll = new ScrollPane(transfersListBox);

    // Status bar & notice banner
    private final Label noticeLabel = new Label();
    private final Button noticeDismissBtn = new Button("Dismiss");
    private final HBox noticeBanner = new HBox(10);
    private final Label statusTcpLabel = new Label();
    private final Label statusUdpLabel = new Label();

    public ClientWorkspaceView(ClientSessionController controller) {
        this.controller = Objects.requireNonNull(controller, "controller");
        getStyleClass().add("app-root");

        offerCountdownTimer = new Timeline(new KeyFrame(Duration.seconds(1), e -> refreshOfferCountdowns()));
        offerCountdownTimer.setCycleCount(Timeline.INDEFINITE);

        setLeft(buildSidebar());
        setCenter(buildCenter());
        setBottom(buildBottomBar());

        switchView(false);
        updateComposerEnabled();
    }

    private VBox buildSidebar() {
        Label brand = new Label("NetSync");
        brand.getStyleClass().add("brand-title");

        currentUserLabel.getStyleClass().add("text-primary");
        currentUserLabel.setStyle("-fx-font-weight: bold; -fx-font-size: 14px;");
        currentUserIdLabel.getStyleClass().addAll("text-secondary", "text-mono");
        currentUserIdLabel.setVisible(false);
        currentUserIdLabel.setManaged(false);

        connectionBadge.getStyleClass().addAll("badge", "badge-success");
        HBox userHeader = new HBox(8, currentUserLabel, connectionBadge);
        userHeader.setAlignment(Pos.CENTER_LEFT);

        VBox profileCard = new VBox(4, brand, userHeader, currentUserIdLabel);
        profileCard.getStyleClass().add("card-subtle");

        messagesNavBtn.getStyleClass().addAll("nav-button", "nav-button-active");
        messagesNavBtn.setMaxWidth(Double.MAX_VALUE);
        messagesNavBtn.setOnAction(e -> switchView(false));

        transfersNavBtn.getStyleClass().add("nav-button");
        transfersNavBtn.setMaxWidth(Double.MAX_VALUE);
        transfersNavBtn.setOnAction(e -> switchView(true));

        VBox navBox = new VBox(4, messagesNavBtn, transfersNavBtn);

        Label usersTitle = new Label("USERS");
        usersTitle.getStyleClass().add("subsection-title");
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        refreshUsersBtn.setTooltip(new Tooltip("Query live presence snapshot from server"));
        refreshUsersBtn.setStyle("-fx-padding: 3 8 3 8; -fx-font-size: 11px;");
        refreshUsersBtn.setOnAction(e -> controller.refreshUsers());
        HBox usersHeader = new HBox(8, usersTitle, spacer, refreshUsersBtn);
        usersHeader.setAlignment(Pos.CENTER_LEFT);

        userListView.setPlaceholder(new Label("No other users connected yet."));
        userListView.setCellFactory(lv -> new ListCell<>() {
            @Override
            protected void updateItem(ClientModels.UserItem item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setGraphic(null);
                    setText(null);
                    return;
                }
                Circle dot = new Circle(5);
                dot.getStyleClass().add(item.state() == PresenceState.ONLINE
                        ? "status-dot-online" : "status-dot-offline");
                Label name = new Label(item.username());
                name.getStyleClass().add("text-primary");
                name.setStyle("-fx-font-weight: 600;");

                VBox nameBox = new VBox(1, name);
                if (showTechnicalDetails) {
                    Label id = new Label(item.clientId().toString().substring(0, 8) + "...");
                    id.getStyleClass().addAll("text-secondary", "text-mono");
                    nameBox.getChildren().add(id);
                }

                Region s = new Region();
                HBox.setHgrow(s, Priority.ALWAYS);
                Label stateLbl = new Label(item.state().name());
                stateLbl.getStyleClass().addAll("badge",
                        item.state() == PresenceState.ONLINE ? "badge-success" : "badge-neutral");
                HBox row = new HBox(8, dot, nameBox, s, stateLbl);
                row.setAlignment(Pos.CENTER_LEFT);
                row.setPadding(new Insets(5, 8, 5, 8));
                setGraphic(row);
            }
        });
        userListView.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null) {
                selectPeer(newVal.clientId(), newVal.username(), newVal.state());
                switchView(false);
            }
        });
        VBox.setVgrow(userListView, Priority.ALWAYS);

        detailsToggle.setOnAction(e -> {
            showTechnicalDetails = detailsToggle.isSelected();
            currentUserIdLabel.setVisible(showTechnicalDetails);
            currentUserIdLabel.setManaged(showTechnicalDetails);
            userListView.refresh();
            renderTransfers(controller.transfersSnapshot());
        });

        logoutBtn.getStyleClass().add("btn-danger");
        logoutBtn.setMaxWidth(Double.MAX_VALUE);
        logoutBtn.setOnAction(e -> controller.logout());

        disconnectBtn.setMaxWidth(Double.MAX_VALUE);
        disconnectBtn.setOnAction(e -> controller.disconnect());

        HBox actionRow = new HBox(8, logoutBtn, disconnectBtn);
        HBox.setHgrow(logoutBtn, Priority.ALWAYS);
        HBox.setHgrow(disconnectBtn, Priority.ALWAYS);

        VBox sidebar = new VBox(12, profileCard, navBox, usersHeader, userListView, detailsToggle, actionRow);
        sidebar.getStyleClass().add("sidebar");
        sidebar.setPadding(new Insets(14));
        sidebar.setPrefWidth(275);
        sidebar.setMinWidth(240);
        return sidebar;
    }

    private VBox buildCenter() {
        noticeLabel.setWrapText(true);
        noticeLabel.setMaxWidth(Double.MAX_VALUE);
        HBox.setHgrow(noticeLabel, Priority.ALWAYS);

        noticeDismissBtn.setMinWidth(Region.USE_PREF_SIZE);
        noticeDismissBtn.setStyle("-fx-padding: 3 10 3 10; -fx-font-size: 11px;");
        noticeDismissBtn.setOnAction(e -> {
            notificationState.dismissCurrent();
            hideNoticeBanner();
        });

        noticeBanner.getChildren().setAll(noticeLabel, noticeDismissBtn);
        noticeBanner.setAlignment(Pos.CENTER_LEFT);
        noticeBanner.getStyleClass().add("warning-banner");
        noticeBanner.setVisible(false);
        noticeBanner.setManaged(false);
        VBox.setMargin(noticeBanner, new Insets(8, 16, 0, 16));

        buildMessagesPane();
        buildTransfersPane();
        centerStack.getChildren().addAll(messagesPane, transfersPane);
        VBox.setVgrow(centerStack, Priority.ALWAYS);
        return new VBox(noticeBanner, centerStack);
    }

    private void buildMessagesPane() {
        peerTitleLabel.getStyleClass().add("section-title");
        peerStatusBadge.getStyleClass().addAll("badge", "badge-neutral");
        peerStatusBadge.setVisible(false);
        peerStatusBadge.setManaged(false);

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        headerAttachBtn.getStyleClass().add("btn-primary");
        headerAttachBtn.setMinWidth(Region.USE_PREF_SIZE);
        headerAttachBtn.setOnAction(e -> chooseAndSendFile());

        HBox topBar = new HBox(10, peerTitleLabel, peerStatusBadge, spacer, headerAttachBtn);
        topBar.setAlignment(Pos.CENTER_LEFT);
        topBar.getStyleClass().add("topbar");

        activeOffersBanner.setPadding(new Insets(8, 16, 0, 16));
        activeOffersBanner.setVisible(false);
        activeOffersBanner.setManaged(false);

        VBox topWrapper = new VBox(topBar, activeOffersBanner);
        messagesPane.setTop(topWrapper);

        messagesBox.setPadding(new Insets(16));
        messagesBox.setFillWidth(true);
        messagesScroll.setFitToWidth(true);
        messagesScroll.setVbarPolicy(ScrollPane.ScrollBarPolicy.AS_NEEDED);
        renderEmptyChatState();
        messagesPane.setCenter(messagesScroll);

        composerField.setPromptText("Write a private message...");
        composerField.setOnAction(e -> sendCurrentMessage());
        HBox.setHgrow(composerField, Priority.ALWAYS);

        attachBtn.setTooltip(new Tooltip("Send a file to the selected user"));
        attachBtn.setOnAction(e -> chooseAndSendFile());

        sendBtn.getStyleClass().add("btn-primary");
        sendBtn.setOnAction(e -> sendCurrentMessage());

        HBox composer = new HBox(8, attachBtn, composerField, sendBtn);
        composer.setAlignment(Pos.CENTER_LEFT);
        composer.setPadding(new Insets(12, 16, 12, 16));
        composer.setStyle("-fx-background-color: #111B2E; -fx-border-color: #334155; -fx-border-width: 1 0 0 0;");
        messagesPane.setBottom(composer);
    }

    private void buildTransfersPane() {
        Label title = new Label("File Transfers");
        title.getStyleClass().add("section-title");
        Label subtitle = new Label("Consent-based stop-and-wait transfers with SHA-256 verification.");
        subtitle.getStyleClass().add("text-secondary");
        VBox titleBox = new VBox(2, title, subtitle);

        HBox header = new HBox(12, titleBox);
        header.getStyleClass().add("topbar");
        transfersPane.setTop(header);

        transfersListBox.setPadding(new Insets(16));
        transfersListBox.setFillWidth(true);
        transfersScroll.setFitToWidth(true);
        transfersPane.setCenter(transfersScroll);
        renderTransfers(List.of());
    }

    private HBox buildBottomBar() {
        statusTcpLabel.getStyleClass().add("text-secondary");
        statusUdpLabel.getStyleClass().add("text-secondary");
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        Label demoTag = new Label("Local Loopback Demo (No TLS)");
        demoTag.getStyleClass().add("text-secondary");
        HBox bar = new HBox(14, statusTcpLabel, statusUdpLabel, spacer, demoTag);
        bar.getStyleClass().add("statusbar");
        return bar;
    }

    // --- Controller callbacks ---

    public void onSessionEstablished(SessionView session) {
        this.activeSession = session;
        this.selectedPeerId = null;
        this.selectedPeerName = null;
        this.selectedPeerState = null;

        currentUserLabel.setText(session.username());
        currentUserIdLabel.setText("Client ID: " + session.clientId());
        statusTcpLabel.setText("TCP: " + controller.currentHost() + ":" + controller.currentTcpPort() + " (Connected)");
        statusUdpLabel.setText("UDP Presence: :" + controller.currentUdpPort()
                + " (heartbeat " + controller.heartbeatIntervalSeconds() + "s / expiry " + controller.presenceTimeoutSeconds() + "s)");
        hideNotice();
        peerTitleLabel.setText("Select a user to start chatting");
        peerStatusBadge.setVisible(false);
        peerStatusBadge.setManaged(false);
        renderEmptyChatState();
        renderTransfers(List.of());
        updateComposerEnabled();
    }

    public void onUsersChanged(List<ClientModels.UserItem> allUsers) {
        UUID myId = activeSession != null ? activeSession.clientId() : null;
        List<ClientModels.UserItem> peers = new ArrayList<>();
        ClientModels.UserItem refreshedSelected = null;
        for (ClientModels.UserItem u : allUsers) {
            if (myId != null && myId.equals(u.clientId())) {
                continue;
            }
            peers.add(u);
            if (selectedPeerId != null && selectedPeerId.equals(u.clientId())) {
                refreshedSelected = u;
            }
        }
        userListView.getItems().setAll(peers);
        if (refreshedSelected != null) {
            selectedPeerName = refreshedSelected.username();
            selectedPeerState = refreshedSelected.state();
            updatePeerHeader();
            userListView.getSelectionModel().select(refreshedSelected);
        }
    }

    public void onConversationUpdated(UUID peerClientId, List<ClientModels.ChatMessageItem> messages) {
        if (selectedPeerId == null) {
            selectPeer(peerClientId, resolvePeerName(peerClientId), resolvePeerState(peerClientId));
        }
        if (peerClientId.equals(selectedPeerId)) {
            renderConversation(messages);
        }
    }

    public void onTransfersChanged(List<ClientModels.TransferItem> transfers) {
        long pendingOrActive = transfers.stream()
                .filter(t -> t.status() == ClientModels.TransferStatus.OFFERED
                        || t.status() == ClientModels.TransferStatus.OFFERING
                        || t.status() == ClientModels.TransferStatus.TRANSFERRING)
                .count();
        transfersNavBtn.setText(pendingOrActive > 0 ? "Transfers (" + pendingOrActive + ")" : "Transfers");
        activeCountdowns.clear();
        renderOffersBanner(transfers);
        renderTransfers(transfers);
        syncOfferCountdownTimer();
    }

    public void onSessionDisconnected() {
        stopOfferCountdownTimer();
        stopNoticeDismissTimer();
        notificationState.dismissCurrent();
        hideNoticeBanner();
        activeCountdowns.clear();
        activeSession = null;
        selectedPeerId = null;
        selectedPeerName = null;
        selectedPeerState = null;
        updateComposerEnabled();
    }

    public void dispose() {
        stopOfferCountdownTimer();
        stopNoticeDismissTimer();
        notificationState.dismissCurrent();
        activeCountdowns.clear();
    }

    private void syncOfferCountdownTimer() {
        if (activeCountdowns.isEmpty()) {
            stopOfferCountdownTimer();
        } else {
            refreshOfferCountdowns();
            if (offerCountdownTimer.getStatus() != javafx.animation.Animation.Status.RUNNING) {
                offerCountdownTimer.play();
            }
        }
    }

    private void stopOfferCountdownTimer() {
        if (offerCountdownTimer.getStatus() == javafx.animation.Animation.Status.RUNNING) {
            offerCountdownTimer.stop();
        }
    }

    private void refreshOfferCountdowns() {
        if (activeCountdowns.isEmpty()) {
            stopOfferCountdownTimer();
            return;
        }
        long nowNanos = controller.nanoTime();
        for (CountdownBinding binding : activeCountdowns) {
            TransferPresentation.countdownLabel(binding.item(), nowNanos)
                    .ifPresent(binding.label()::setText);
        }
    }

    public void showNotice(String severity, String message) {
        NotificationState.Severity sev = "ERROR".equalsIgnoreCase(severity)
                ? NotificationState.Severity.ERROR
                : "INFO".equalsIgnoreCase(severity)
                        ? NotificationState.Severity.INFO
                        : NotificationState.Severity.WARNING;
        showNotice(NotificationState.Category.GENERAL, sev, message);
    }

    private void showNotice(NotificationState.Category category, NotificationState.Severity severity, String message) {
        stopNoticeDismissTimer();
        NotificationState.ActiveNotice active = notificationState.show(category, severity, message);
        noticeLabel.setText(active.message());
        noticeBanner.getStyleClass().removeAll("warning-banner", "error-banner", "success-banner");
        switch (active.severity()) {
            case ERROR -> noticeBanner.getStyleClass().add("error-banner");
            case INFO -> noticeBanner.getStyleClass().add("success-banner");
            case WARNING -> noticeBanner.getStyleClass().add("warning-banner");
        }
        noticeBanner.setVisible(true);
        noticeBanner.setManaged(true);

        if (active.autoDismissSeconds() > 0) {
            long expectedId = active.id();
            PauseTransition timer = new PauseTransition(Duration.seconds(active.autoDismissSeconds()));
            timer.setOnFinished(e -> {
                if (notificationState.dismissIfMatches(expectedId)) {
                    hideNoticeBanner();
                }
            });
            noticeDismissTimer = timer;
            timer.play();
        }
    }

    private void clearSaveAsNotice() {
        if (notificationState.clearCategory(NotificationState.Category.SAVE_AS)) {
            hideNoticeBanner();
        }
    }

    private void hideNotice() {
        notificationState.dismissCurrent();
        hideNoticeBanner();
    }

    private void hideNoticeBanner() {
        stopNoticeDismissTimer();
        noticeBanner.setVisible(false);
        noticeBanner.setManaged(false);
    }

    private void stopNoticeDismissTimer() {
        if (noticeDismissTimer != null) {
            noticeDismissTimer.stop();
            noticeDismissTimer = null;
        }
    }

    // --- UI Actions & Rendering ---

    private void switchView(boolean transfers) {
        clearSaveAsNotice();
        showTransfersTab = transfers;
        messagesPane.setVisible(!transfers);
        messagesPane.setManaged(!transfers);
        transfersPane.setVisible(transfers);
        transfersPane.setManaged(transfers);
        messagesNavBtn.getStyleClass().remove("nav-button-active");
        transfersNavBtn.getStyleClass().remove("nav-button-active");
        if (transfers) {
            transfersNavBtn.getStyleClass().add("nav-button-active");
        } else {
            messagesNavBtn.getStyleClass().add("nav-button-active");
        }
    }

    private void selectPeer(UUID peerId, String peerName, PresenceState state) {
        clearSaveAsNotice();
        this.selectedPeerId = peerId;
        this.selectedPeerName = peerName;
        this.selectedPeerState = state;
        updatePeerHeader();
        updateComposerEnabled();
        renderConversation(controller.conversationSnapshot(peerId));
    }

    private void updatePeerHeader() {
        if (selectedPeerId == null) {
            peerTitleLabel.setText("Select a user to start chatting");
            peerStatusBadge.setVisible(false);
            peerStatusBadge.setManaged(false);
            return;
        }
        peerTitleLabel.setText(selectedPeerName != null ? selectedPeerName : selectedPeerId.toString());
        if (selectedPeerState != null) {
            peerStatusBadge.setText(selectedPeerState.name());
            peerStatusBadge.getStyleClass().removeAll("badge-success", "badge-neutral");
            peerStatusBadge.getStyleClass().add(selectedPeerState == PresenceState.ONLINE ? "badge-success" : "badge-neutral");
            peerStatusBadge.setVisible(true);
            peerStatusBadge.setManaged(true);
        } else {
            peerStatusBadge.setVisible(false);
            peerStatusBadge.setManaged(false);
        }
    }

    private void updateComposerEnabled() {
        boolean hasPeer = selectedPeerId != null;
        composerField.setDisable(!hasPeer);
        sendBtn.setDisable(!hasPeer);
        attachBtn.setDisable(!hasPeer);
        headerAttachBtn.setDisable(!hasPeer);
    }

    private void sendCurrentMessage() {
        if (selectedPeerId == null) return;
        String text = composerField.getText();
        if (text == null || text.isBlank()) return;
        hideNotice();
        try {
            controller.sendChatMessage(selectedPeerId, text);
            composerField.clear();
        } catch (RuntimeException failure) {
            showNotice("ERROR", failure.getMessage() != null ? failure.getMessage() : "Could not send message.");
        }
    }

    private void chooseAndSendFile() {
        if (selectedPeerId == null || getScene() == null || getScene().getWindow() == null) return;
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Select File to Send (Max 10 MiB)");
        File chosen = chooser.showOpenDialog(getScene().getWindow());
        if (chosen == null) return;
        hideNotice();
        try {
            controller.sendFile(selectedPeerId, chosen.toPath());
            switchView(true);
        } catch (RuntimeException failure) {
            showNotice("ERROR", failure.getMessage() != null ? failure.getMessage() : "Could not offer file.");
        }
    }

    private void renderEmptyChatState() {
        messagesBox.getChildren().clear();
        Label empty = new Label(selectedPeerId == null
                ? "Choose a user from the sidebar to send private TCP messages or files."
                : "No messages yet with " + selectedPeerName + ". Say hello!");
        empty.getStyleClass().add("text-secondary");
        empty.setPadding(new Insets(24));
        messagesBox.getChildren().add(empty);
    }

    private void renderConversation(List<ClientModels.ChatMessageItem> messages) {
        if (messages.isEmpty()) {
            renderEmptyChatState();
            return;
        }
        messagesBox.getChildren().clear();
        for (ClientModels.ChatMessageItem msg : messages) {
            Label textLabel = new Label(msg.text());
            textLabel.setWrapText(true);
            textLabel.setMaxWidth(480);
            textLabel.getStyleClass().add("text-primary");

            String metaText = TIME_FMT.format(msg.timestamp());
            if (msg.outgoing()) {
                metaText += " • " + formatDeliveryStatus(msg.status(), msg.statusDetail());
            } else {
                metaText = msg.senderLabel() + " • " + metaText;
            }
            Label meta = new Label(metaText);
            meta.getStyleClass().add("bubble-meta");
            if (msg.status() == ClientModels.DeliveryStatus.FAILED) {
                meta.setStyle("-fx-text-fill: #FCA5A5;");
            } else if (msg.status() == ClientModels.DeliveryStatus.UNKNOWN_OUTCOME) {
                meta.setStyle("-fx-text-fill: #FCD34D;");
            }

            VBox bubble = new VBox(4, textLabel, meta);
            bubble.getStyleClass().add(msg.outgoing() ? "bubble-outgoing" : "bubble-incoming");

            HBox row = new HBox(bubble);
            row.setAlignment(msg.outgoing() ? Pos.CENTER_RIGHT : Pos.CENTER_LEFT);
            messagesBox.getChildren().add(row);
        }
        messagesScroll.setVvalue(1.0);
    }

    private void renderOffersBanner(List<ClientModels.TransferItem> transfers) {
        activeOffersBanner.getChildren().clear();
        List<ClientModels.TransferItem> pendingOffers = transfers.stream()
                .filter(t -> t.direction() == ClientModels.TransferDirection.INCOMING
                        && t.status() == ClientModels.TransferStatus.OFFERED)
                .toList();
        if (pendingOffers.isEmpty()) {
            activeOffersBanner.setVisible(false);
            activeOffersBanner.setManaged(false);
            return;
        }
        for (ClientModels.TransferItem offer : pendingOffers) {
            activeOffersBanner.getChildren().add(buildTransferCard(offer));
        }
        activeOffersBanner.setVisible(true);
        activeOffersBanner.setManaged(true);
    }

    private void renderTransfers(List<ClientModels.TransferItem> transfers) {
        transfersListBox.getChildren().clear();
        if (transfers.isEmpty()) {
            Label empty = new Label("No file transfers in this session yet. Select a user and click Send File to start.");
            empty.getStyleClass().add("text-secondary");
            empty.setPadding(new Insets(20));
            transfersListBox.getChildren().add(empty);
            return;
        }
        for (ClientModels.TransferItem item : transfers) {
            transfersListBox.getChildren().add(buildTransferCard(item));
        }
    }

    private VBox buildTransferCard(ClientModels.TransferItem item) {
        String dir = item.direction() == ClientModels.TransferDirection.OUTGOING ? "Outgoing" : "Incoming";
        Label title = new Label(dir + " • " + item.fileName() + " (" + formatBytes(item.totalBytes()) + ")");
        title.getStyleClass().add("text-primary");
        title.setStyle("-fx-font-weight: bold;");
        title.setWrapText(true);

        Label statusBadge = new Label(item.status().name());
        statusBadge.getStyleClass().addAll("badge", transferBadgeClass(item.status()));
        statusBadge.setMinWidth(Region.USE_PREF_SIZE);

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox header = new HBox(10, title, spacer, statusBadge);
        header.setAlignment(Pos.CENTER_LEFT);

        String peerRole = item.direction() == ClientModels.TransferDirection.OUTGOING ? "To: " : "From: ";
        Label detail = new Label(peerRole + item.peerLabel() + "  •  " + item.statusDetail());
        detail.getStyleClass().add("text-secondary");
        detail.setWrapText(true);

        VBox card = new VBox(8, header, detail);
        card.getStyleClass().add("card");

        TransferPresentation.countdownLabel(item, controller.nanoTime()).ifPresent(countdownText -> {
            Label countdownLabel = new Label(countdownText);
            countdownLabel.setStyle("-fx-text-fill: #F59E0B; -fx-font-weight: 600; -fx-font-size: 12px;");
            countdownLabel.setWrapText(true);
            activeCountdowns.add(new CountdownBinding(item, countdownLabel));
            card.getChildren().add(countdownLabel);
        });

        if (showTechnicalDetails && item.transferId() != null) {
            Label tid = new Label("Transfer ID: " + item.transferId());
            tid.getStyleClass().addAll("text-secondary", "text-mono");
            tid.setWrapText(true);
            card.getChildren().add(tid);
        }

        if (item.status() == ClientModels.TransferStatus.TRANSFERRING
                || item.status() == ClientModels.TransferStatus.COMPLETED) {
            ProgressBar bar = new ProgressBar(item.progressRatio());
            bar.setMaxWidth(Double.MAX_VALUE);
            Label progLbl = new Label(String.format(Locale.ROOT, "%.1f%% (%s / %s)",
                    item.percentage(), formatBytes(item.bytesTransferred()), formatBytes(item.totalBytes())));
            progLbl.getStyleClass().add("text-secondary");
            card.getChildren().addAll(bar, progLbl);
        }

        if (TransferPresentation.shouldShowSha256Verified(item)) {
            Label sha = new Label("SHA-256 Verified: " + item.sha256());
            sha.getStyleClass().add("text-mono");
            sha.setStyle("-fx-text-fill: #22C55E;");
            sha.setWrapText(true);
            card.getChildren().add(sha);
        }
        TransferPresentation.completedFilePathLabel(item).ifPresent(pathText -> {
            Label pathLabel = new Label(pathText);
            pathLabel.getStyleClass().addAll("text-secondary", "text-mono");
            pathLabel.setWrapText(true);
            card.getChildren().add(pathLabel);
        });

        HBox actions = new HBox(8);
        actions.setAlignment(Pos.CENTER_LEFT);
        if (item.direction() == ClientModels.TransferDirection.INCOMING
                && item.status() == ClientModels.TransferStatus.OFFERED && item.transferId() != null) {
            Button accept = new Button("Accept");
            accept.getStyleClass().add("btn-success");
            accept.setMinWidth(Region.USE_PREF_SIZE);
            accept.setOnAction(e -> {
                accept.setDisable(true);
                try {
                    controller.acceptFileOffer(item.transferId());
                } catch (RuntimeException ex) {
                    showNotice("ERROR", ex.getMessage());
                }
            });
            Button reject = new Button("Reject");
            reject.getStyleClass().add("btn-danger");
            reject.setMinWidth(Region.USE_PREF_SIZE);
            reject.setOnAction(e -> {
                reject.setDisable(true);
                try {
                    controller.rejectFileOffer(item.transferId());
                } catch (RuntimeException ex) {
                    showNotice("ERROR", ex.getMessage());
                }
            });
            actions.getChildren().addAll(accept, reject);
        } else if (item.status() == ClientModels.TransferStatus.OFFERING
                || item.status() == ClientModels.TransferStatus.TRANSFERRING) {
            Button cancel = new Button("Cancel Transfer");
            cancel.getStyleClass().add("btn-danger");
            cancel.setMinWidth(Region.USE_PREF_SIZE);
            cancel.setOnAction(e -> {
                cancel.setDisable(true);
                try {
                    controller.cancelFileTransfer(item.transferId());
                } catch (RuntimeException ex) {
                    showNotice("ERROR", ex.getMessage());
                }
            });
            actions.getChildren().add(cancel);
        } else if (item.status() == ClientModels.TransferStatus.COMPLETED && item.localFile() != null) {
            if (TransferPresentation.canSaveAs(item)) {
                Button saveAs = new Button("Save As...");
                saveAs.getStyleClass().add("btn-primary");
                saveAs.setMinWidth(Region.USE_PREF_SIZE);
                saveAs.setTooltip(new Tooltip("Export a verified copy using the original filename without overwriting existing files"));
                saveAs.setOnAction(e -> triggerSaveAs(item, saveAs));
                actions.getChildren().add(saveAs);
            }
            Button openFolder = new Button(TransferPresentation.openFolderButtonText(item));
            openFolder.setMinWidth(Region.USE_PREF_SIZE);
            openFolder.setTooltip(new Tooltip(item.direction() == ClientModels.TransferDirection.OUTGOING
                    ? "Opens the source file folder"
                    : "Opens the download folder without executing the received file"));
            openFolder.setOnAction(e -> openParentFolderSafely(item.localFile()));
            actions.getChildren().add(openFolder);
        }

        if (!actions.getChildren().isEmpty()) {
            card.getChildren().add(actions);
        }
        return card;
    }

    private void triggerSaveAs(ClientModels.TransferItem item, Button saveAsButton) {
        if (!TransferPresentation.canSaveAs(item) || getScene() == null || getScene().getWindow() == null) {
            return;
        }
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Save Verified Received File As");
        chooser.setInitialFileName(SafeFileExportService.safeSuggestedFileName(item.fileName(), item.transferId()));
        File chosen = chooser.showSaveDialog(getScene().getWindow());
        if (chosen == null) {
            return; // User cancelled; no error reported.
        }
        saveAsButton.setDisable(true);
        hideNotice();
        controller.exportReceivedFile(item.transferId(), chosen.toPath()).whenComplete((result, err) ->
                Platform.runLater(() -> {
                    saveAsButton.setDisable(false);
                    if (err != null) {
                        Throwable cause = err.getCause() != null ? err.getCause() : err;
                        showNotice(NotificationState.Category.SAVE_AS, NotificationState.Severity.ERROR,
                                "Save As failed: " + (cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName()));
                    } else {
                        showNotice(NotificationState.Category.SAVE_AS, NotificationState.Severity.INFO,
                                "Exported verified copy (" + formatBytes(result.bytesCopied())
                                        + ", SHA-256 verified) to: " + result.destinationFile());
                    }
                }));
    }

    private void openParentFolderSafely(Path receivedFile) {
        try {
            Path folder = receivedFile.toAbsolutePath().normalize().getParent();
            if (folder == null || !Files.isDirectory(folder)) {
                return;
            }
            String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
            if (os.contains("win")) {
                // Avoid initializing the AWT toolkit inside JavaFX (AWT spawns non-daemon threads that can keep javaw alive after window close).
                new ProcessBuilder("explorer.exe", folder.toString()).start();
            } else if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().open(folder.toFile());
            }
        } catch (Exception failure) {
            showNotice("WARNING", "Could not open folder automatically; path: " + receivedFile.getParent());
        }
    }

    private String resolvePeerName(UUID peerId) {
        for (ClientModels.UserItem u : userListView.getItems()) {
            if (u.clientId().equals(peerId)) return u.username();
        }
        return "User " + peerId.toString().substring(0, 8);
    }

    private PresenceState resolvePeerState(UUID peerId) {
        for (ClientModels.UserItem u : userListView.getItems()) {
            if (u.clientId().equals(peerId)) return u.state();
        }
        return null;
    }

    private static String formatDeliveryStatus(ClientModels.DeliveryStatus status, String detail) {
        return switch (status) {
            case SENDING -> "Sending...";
            case QUEUED -> "Queued";
            case FAILED -> detail != null && !detail.isBlank() ? detail : "Failed";
            case UNKNOWN_OUTCOME -> "Outcome unknown";
            case RECEIVED -> "";
        };
    }

    private static String transferBadgeClass(ClientModels.TransferStatus status) {
        return switch (status) {
            case COMPLETED -> "badge-success";
            case OFFERED, OFFERING, TRANSFERRING -> "badge-warning";
            case REJECTED, CANCELLED, FAILED -> "badge-error";
        };
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format(Locale.ROOT, "%.1f KiB", bytes / 1024.0);
        return String.format(Locale.ROOT, "%.2f MiB", bytes / (1024.0 * 1024.0));
    }
}
