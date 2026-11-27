package com.netsync.gui;

import com.netsync.app.NetSyncServerApplication;
import com.netsync.common.PresenceState;
import com.netsync.common.ProtocolLimits;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.PasswordField;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.shape.Circle;
import javafx.util.Duration;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/** JavaFX Server Administration Dashboard bound to {@link ServerDashboardController}. */
public final class ServerDashboardView extends BorderPane {
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    private static final class AccountRowControls {
        final HBox container;
        final TextField usernameField;
        final PasswordField passwordField;
        final Button removeButton;

        AccountRowControls(String initialUsername, Runnable onRemove) {
            usernameField = new TextField(initialUsername);
            usernameField.setPromptText("Username (e.g. alice)");
            usernameField.setPrefWidth(120);
            usernameField.setMinWidth(95);
            HBox.setHgrow(usernameField, Priority.SOMETIMES);

            passwordField = new PasswordField();
            passwordField.setPromptText("Disposable password");
            passwordField.setPrefWidth(145);
            passwordField.setMinWidth(110);
            HBox.setHgrow(passwordField, Priority.ALWAYS);

            removeButton = new Button("Remove");
            removeButton.getStyleClass().add("btn-danger");
            removeButton.setMinWidth(Region.USE_PREF_SIZE);
            removeButton.setPrefWidth(82);
            HBox.setHgrow(removeButton, Priority.NEVER);
            removeButton.setOnAction(e -> onRemove.run());

            container = new HBox(8, usernameField, passwordField, removeButton);
            container.setAlignment(Pos.CENTER_LEFT);
        }
    }

    private final ServerDashboardController controller;
    private final Timeline monitorTimer;

    private final Label statusBadge = new Label("STOPPED");
    private final Label statusDetailLabel = new Label("Configure disposable demo accounts and start the server.");
    private final TextField tcpPortField = new TextField(String.valueOf(ProtocolLimits.TCP_PORT));
    private final TextField udpPortField = new TextField(String.valueOf(ProtocolLimits.UDP_PORT));
    private final CheckBox insecureDemoCheck = new CheckBox("Acknowledge local-only unencrypted demo (--insecure-demo)");
    private final VBox accountsContainer = new VBox(8);
    private final List<AccountRowControls> accountRows = new ArrayList<>();
    private final Button addAccountButton = new Button("+ Add Account");
    private final Button startButton = new Button("Start Server");
    private final Button stopButton = new Button("Stop Server");
    private final Button restartButton = new Button("Restart");
    private final Label errorBannerLabel = new Label();
    private final HBox errorBanner = new HBox(errorBannerLabel);

    private final Label tcpMetricValue = new Label("127.0.0.1:5050");
    private final Label udpMetricValue = new Label("127.0.0.1:5051");
    private final Label accountsMetricValue = new Label("0");
    private final Label sessionsMetricValue = new Label("0");
    private final Label transfersMetricValue = new Label("0");
    private final Label uptimeMetricValue = new Label("00:00:00");

    private final ListView<ServerModels.MonitoredSession> sessionsList = new ListView<>();
    private final ListView<ServerModels.ActivityLogEntry> logList = new ListView<>();

    public ServerDashboardView(ServerDashboardController controller) {
        this.controller = controller;
        getStyleClass().add("app-root");

        setTop(buildTopBar());
        setCenter(buildMainContent());
        setBottom(buildStatusBar());

        addAccountRow("alice");
        addAccountRow("bob");
        addAccountRow("carol");

        configureActions();
        controller.setListener(new ServerDashboardController.Listener() {
            @Override
            public void onSnapshot(ServerModels.ServerSnapshot snapshot) {
                renderSnapshot(snapshot);
            }

            @Override
            public void onLogEntry(ServerModels.ActivityLogEntry entry) {
                logList.getItems().add(entry);
                if (logList.getItems().size() > ServerDashboardController.MAX_LOG_ENTRIES) {
                    logList.getItems().removeFirst();
                }
                logList.scrollTo(logList.getItems().size() - 1);
            }
        });

        monitorTimer = new Timeline(new KeyFrame(Duration.seconds(1), e -> controller.refreshMonitoring()));
        monitorTimer.setCycleCount(Timeline.INDEFINITE);
        monitorTimer.play();
    }

    private HBox buildTopBar() {
        Label brand = new Label("NetSync Server");
        brand.getStyleClass().add("brand-title");
        Label subtitle = new Label("Local Administration Dashboard");
        subtitle.getStyleClass().add("text-secondary");
        VBox titleBox = new VBox(2, brand, subtitle);

        statusBadge.getStyleClass().addAll("badge", "badge-neutral");

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        startButton.getStyleClass().add("btn-success");
        startButton.setMinWidth(Region.USE_PREF_SIZE);
        stopButton.getStyleClass().add("btn-danger");
        stopButton.setMinWidth(Region.USE_PREF_SIZE);
        stopButton.setDisable(true);
        restartButton.setMinWidth(Region.USE_PREF_SIZE);
        restartButton.setDisable(true);

        HBox controls = new HBox(8, startButton, stopButton, restartButton);
        controls.setAlignment(Pos.CENTER_RIGHT);

        HBox bar = new HBox(14, titleBox, statusBadge, spacer, controls);
        bar.setAlignment(Pos.CENTER_LEFT);
        bar.getStyleClass().add("topbar");
        return bar;
    }

    private HBox buildMainContent() {
        VBox leftConfig = buildConfigurationPanel();
        leftConfig.setPrefWidth(435);
        leftConfig.setMinWidth(385);

        VBox rightMonitor = buildMonitoringPanel();
        HBox.setHgrow(rightMonitor, Priority.ALWAYS);

        HBox body = new HBox(16, leftConfig, rightMonitor);
        body.setPadding(new Insets(16));
        return body;
    }

    private VBox buildConfigurationPanel() {
        Label warningText = new Label(NetSyncServerApplication.demoWarning());
        warningText.setWrapText(true);
        warningText.setMaxWidth(Double.MAX_VALUE);
        warningText.setMinHeight(Region.USE_PREF_SIZE);
        VBox warningBanner = new VBox(warningText);
        warningBanner.getStyleClass().add("warning-banner");
        warningBanner.setMinHeight(Region.USE_PREF_SIZE);

        errorBannerLabel.setWrapText(true);
        errorBannerLabel.setMaxWidth(Double.MAX_VALUE);
        errorBannerLabel.setMinHeight(Region.USE_PREF_SIZE);
        HBox.setHgrow(errorBannerLabel, Priority.ALWAYS);
        errorBanner.getStyleClass().add("error-banner");
        errorBanner.setMinHeight(Region.USE_PREF_SIZE);
        errorBanner.setVisible(false);
        errorBanner.setManaged(false);

        Label netHeader = new Label("Network Configuration");
        netHeader.getStyleClass().add("section-title");

        TextField bindField = new TextField(ProtocolLimits.BIND_ADDRESS);
        bindField.setEditable(false);
        bindField.setTooltip(new Tooltip("Fixed loopback-only demonstration address"));

        tcpPortField.setPromptText("5050");
        udpPortField.setPromptText("5051");

        VBox bindBox = labeledField("Bind Address (Fixed Loopback)", bindField);
        VBox tcpBox = labeledField("TCP Port", tcpPortField);
        VBox udpBox = labeledField("UDP Presence Port", udpPortField);
        HBox portRow = new HBox(10, tcpBox, udpBox);
        HBox.setHgrow(tcpBox, Priority.ALWAYS);
        HBox.setHgrow(udpBox, Priority.ALWAYS);

        insecureDemoCheck.setWrapText(true);

        VBox netCard = new VBox(10, netHeader, bindBox, portRow, insecureDemoCheck);
        netCard.getStyleClass().add("card");

        Label accHeader = new Label("Disposable Demo Accounts");
        accHeader.getStyleClass().add("section-title");
        Label accHint = new Label("Enter disposable passwords in memory (never stored on disk).");
        accHint.getStyleClass().add("text-secondary");
        accHint.setWrapText(true);

        addAccountButton.getStyleClass().add("btn-primary");
        addAccountButton.setOnAction(e -> addAccountRow(""));

        accountsContainer.setPadding(new Insets(0, 8, 0, 0));
        ScrollPane accScroll = new ScrollPane(accountsContainer);
        accScroll.setFitToWidth(true);
        accScroll.setPrefViewportHeight(210);
        VBox.setVgrow(accScroll, Priority.ALWAYS);

        VBox accCard = new VBox(10, accHeader, accHint, accScroll, addAccountButton);
        accCard.getStyleClass().add("card");
        VBox.setVgrow(accCard, Priority.ALWAYS);

        return new VBox(12, warningBanner, errorBanner, netCard, accCard);
    }

    private VBox buildMonitoringPanel() {
        FlowPane metricsGrid = new FlowPane(10, 10,
                metricCard("TCP Endpoint", tcpMetricValue),
                metricCard("UDP Presence", udpMetricValue),
                metricCard("Uptime", uptimeMetricValue),
                metricCard("Configured Accounts", accountsMetricValue),
                metricCard("Active Sessions", sessionsMetricValue),
                metricCard("Active Transfers", transfersMetricValue));
        metricsGrid.setPrefWrapLength(520);
        metricsGrid.setMinHeight(Region.USE_PREF_SIZE);

        Label sessionsTitle = new Label("Authenticated Sessions & Presence");
        sessionsTitle.getStyleClass().add("section-title");

        sessionsList.setPlaceholder(new Label("No authenticated clients connected."));
        sessionsList.setCellFactory(lv -> new ListCell<>() {
            @Override
            protected void updateItem(ServerModels.MonitoredSession item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setGraphic(null);
                    setText(null);
                    return;
                }
                Circle dot = new Circle(5);
                dot.getStyleClass().add(item.presenceState() == PresenceState.ONLINE
                        ? "status-dot-online" : "status-dot-offline");
                Label user = new Label(item.username());
                user.getStyleClass().add("text-primary");
                user.setStyle("-fx-font-weight: bold;");
                Label id = new Label("Client ID: " + item.clientId());
                id.getStyleClass().addAll("text-secondary", "text-mono");
                Region spacer = new Region();
                HBox.setHgrow(spacer, Priority.ALWAYS);
                Label stateBadge = new Label(item.presenceState().name());
                stateBadge.getStyleClass().addAll("badge",
                        item.presenceState() == PresenceState.ONLINE ? "badge-success" : "badge-neutral");
                HBox row = new HBox(10, dot, new VBox(2, user, id), spacer, stateBadge);
                row.setAlignment(Pos.CENTER_LEFT);
                row.getStyleClass().add("card-subtle");
                setGraphic(row);
            }
        });
        sessionsList.setPrefHeight(190);
        VBox sessionsCard = new VBox(8, sessionsTitle, sessionsList);
        sessionsCard.getStyleClass().add("card");
        VBox.setVgrow(sessionsList, Priority.ALWAYS);
        VBox.setVgrow(sessionsCard, Priority.ALWAYS);

        Label logTitle = new Label("Server Activity Log (Bounded, Non-Sensitive)");
        logTitle.getStyleClass().add("section-title");
        logList.setPlaceholder(new Label("No server activity yet."));
        logList.setCellFactory(lv -> new ListCell<>() {
            @Override
            protected void updateItem(ServerModels.ActivityLogEntry item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                    setGraphic(null);
                    return;
                }
                Label line = new Label("[" + TIME_FMT.format(item.timestamp()) + "] [" + item.level() + "] " + item.message());
                line.getStyleClass().add("text-mono");
                line.setWrapText(true);
                if ("ERROR".equals(item.level())) {
                    line.setStyle("-fx-text-fill: #FCA5A5;");
                } else {
                    line.setStyle("-fx-text-fill: #CBD5E1;");
                }
                setGraphic(line);
            }
        });
        logList.setPrefHeight(165);
        VBox logCard = new VBox(8, logTitle, logList);
        logCard.getStyleClass().add("card");
        VBox.setVgrow(logList, Priority.ALWAYS);

        return new VBox(12, metricsGrid, sessionsCard, logCard);
    }

    private HBox buildStatusBar() {
        statusDetailLabel.getStyleClass().add("text-secondary");
        Label mode = new Label("Loopback 127.0.0.1 • Unencrypted Local Demo");
        mode.getStyleClass().add("text-secondary");
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox bar = new HBox(12, statusDetailLabel, spacer, mode);
        bar.getStyleClass().add("statusbar");
        return bar;
    }

    private VBox metricCard(String label, Label valueLabel) {
        Label lbl = new Label(label);
        lbl.getStyleClass().add("metric-label");
        lbl.setMinWidth(Region.USE_PREF_SIZE);
        valueLabel.getStyleClass().add("metric-value");
        valueLabel.setMinWidth(Region.USE_PREF_SIZE);
        VBox card = new VBox(4, lbl, valueLabel);
        card.getStyleClass().add("card");
        card.setMinWidth(156);
        card.setPrefWidth(168);
        return card;
    }

    private VBox labeledField(String label, TextField field) {
        Label lbl = new Label(label);
        lbl.getStyleClass().add("subsection-title");
        return new VBox(4, lbl, field);
    }

    private void addAccountRow(String defaultUsername) {
        final AccountRowControls[] holder = new AccountRowControls[1];
        holder[0] = new AccountRowControls(defaultUsername, () -> {
            accountRows.remove(holder[0]);
            accountsContainer.getChildren().remove(holder[0].container);
        });
        accountRows.add(holder[0]);
        accountsContainer.getChildren().add(holder[0].container);
    }

    private void configureActions() {
        startButton.setOnAction(e -> triggerStart(false));
        stopButton.setOnAction(e -> {
            hideError();
            controller.stopServer();
        });
        restartButton.setOnAction(e -> triggerStart(true));
    }

    private void triggerStart(boolean restart) {
        hideError();
        int tcpPort;
        int udpPort;
        try {
            tcpPort = Integer.parseInt(tcpPortField.getText().trim());
            udpPort = Integer.parseInt(udpPortField.getText().trim());
        } catch (NumberFormatException ex) {
            showError("TCP and UDP ports must be integers between 0 and 65535.");
            return;
        }
        List<ServerDashboardController.AccountInput> inputs = new ArrayList<>();
        for (AccountRowControls row : accountRows) {
            String user = row.usernameField.getText() == null ? "" : row.usernameField.getText().trim();
            char[] pass = row.passwordField.getText() == null ? new char[0] : row.passwordField.getText().toCharArray();
            if (!user.isEmpty() || pass.length > 0) {
                inputs.add(new ServerDashboardController.AccountInput(user, pass));
            }
        }
        boolean ack = insecureDemoCheck.isSelected();
        var future = restart
                ? controller.restartServer(ack, tcpPort, udpPort, inputs)
                : controller.startServer(ack, tcpPort, udpPort, inputs);
        // Note: JavaFX PasswordField stores immutable Strings internally; clearing UI fields avoids keeping text on screen.
        future.whenComplete((snap, err) -> {
            if (err != null) {
                Throwable cause = err.getCause() != null ? err.getCause() : err;
                javafx.application.Platform.runLater(() -> showError(cause.getMessage()));
            }
        });
    }

    private void renderSnapshot(ServerModels.ServerSnapshot snap) {
        statusBadge.setText(snap.state().name());
        statusBadge.getStyleClass().removeAll("badge-success", "badge-warning", "badge-error", "badge-neutral");
        switch (snap.state()) {
            case RUNNING -> statusBadge.getStyleClass().add("badge-success");
            case STARTING, STOPPING -> statusBadge.getStyleClass().add("badge-warning");
            case FAILED -> statusBadge.getStyleClass().add("badge-error");
            case STOPPED -> statusBadge.getStyleClass().add("badge-neutral");
        }

        statusDetailLabel.setText(snap.statusMessage());
        tcpMetricValue.setText(snap.bindAddress() + ":" + snap.tcpPort());
        udpMetricValue.setText(snap.bindAddress() + ":" + snap.udpPort());
        accountsMetricValue.setText(String.valueOf(snap.configuredAccountCount()));
        sessionsMetricValue.setText(String.valueOf(snap.sessions().size()));
        transfersMetricValue.setText(String.valueOf(snap.activeTransferCount()));
        uptimeMetricValue.setText(formatUptime(snap.uptimeSeconds()));

        boolean busy = snap.state() == ServerModels.ServerState.STARTING || snap.state() == ServerModels.ServerState.STOPPING;
        boolean running = snap.state() == ServerModels.ServerState.RUNNING;
        startButton.setDisable(busy || running);
        stopButton.setDisable(busy || !running);
        restartButton.setDisable(busy || !running);
        tcpPortField.setDisable(busy || running);
        udpPortField.setDisable(busy || running);
        insecureDemoCheck.setDisable(busy || running);
        addAccountButton.setDisable(busy || running);
        for (AccountRowControls row : accountRows) {
            row.usernameField.setDisable(busy || running);
            row.passwordField.setDisable(busy || running);
            row.removeButton.setDisable(busy || running);
        }

        sessionsList.getItems().setAll(snap.sessions());
    }

    private void showError(String message) {
        errorBannerLabel.setText(message != null ? message : "Operation failed.");
        errorBanner.setVisible(true);
        errorBanner.setManaged(true);
    }

    private void hideError() {
        errorBanner.setVisible(false);
        errorBanner.setManaged(false);
    }

    private static String formatUptime(long totalSeconds) {
        long h = totalSeconds / 3600;
        long m = (totalSeconds % 3600) / 60;
        long s = totalSeconds % 60;
        return String.format("%02d:%02d:%02d", h, m, s);
    }

    public void dispose() {
        monitorTimer.stop();
        for (AccountRowControls row : accountRows) {
            row.passwordField.clear();
        }
    }
}
