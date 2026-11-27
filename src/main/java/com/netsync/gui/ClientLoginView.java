package com.netsync.gui;

import com.netsync.common.ProtocolLimits;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;

import java.util.Arrays;

/** JavaFX Login and Connection Screen for the NetSync desktop client. */
public final class ClientLoginView extends StackPane {
    private final ClientSessionController controller;

    private final TextField hostField = new TextField(ProtocolLimits.BIND_ADDRESS);
    private final TextField tcpPortField = new TextField(String.valueOf(ProtocolLimits.TCP_PORT));
    private final TextField udpPortField = new TextField(String.valueOf(ProtocolLimits.UDP_PORT));
    private final TextField usernameField = new TextField();
    private final PasswordField passwordField = new PasswordField();
    private final Button signInButton = new Button("Connect & Sign In");
    private final ProgressIndicator spinner = new ProgressIndicator();
    private final Label statusLabel = new Label("Ready to connect to local NetSync server.");
    private final Label errorLabel = new Label();
    private final HBox errorBanner = new HBox(errorLabel);

    public ClientLoginView(ClientSessionController controller) {
        this.controller = controller;
        getStyleClass().add("app-root");
        setPadding(new Insets(24));

        VBox card = new VBox(14);
        card.getStyleClass().add("card");
        card.setMaxWidth(440);
        card.setPadding(new Insets(26));

        Label brand = new Label("NetSync");
        brand.getStyleClass().add("brand-title");
        Label subtitle = new Label("Private Messaging & Verified File Transfer");
        subtitle.getStyleClass().add("text-secondary");
        VBox header = new VBox(3, brand, subtitle);

        Label warningText = new Label("Local educational demo: loopback (127.0.0.1) only, no TLS encryption. Use disposable credentials only.");
        warningText.setWrapText(true);
        HBox warningBanner = new HBox(warningText);
        warningBanner.getStyleClass().add("warning-banner");

        errorLabel.setWrapText(true);
        errorBanner.getStyleClass().add("error-banner");
        errorBanner.setVisible(false);
        errorBanner.setManaged(false);

        hostField.setPromptText("127.0.0.1");
        tcpPortField.setPromptText("5050");
        udpPortField.setPromptText("5051");
        usernameField.setPromptText("Username (e.g. alice)");
        passwordField.setPromptText("Disposable password");

        VBox hostBox = labeled("Server Address (Loopback)", hostField);
        VBox tcpBox = labeled("TCP Port", tcpPortField);
        VBox udpBox = labeled("UDP Port", udpPortField);
        HBox portsRow = new HBox(10, tcpBox, udpBox);
        HBox.setHgrow(tcpBox, Priority.ALWAYS);
        HBox.setHgrow(udpBox, Priority.ALWAYS);

        VBox userBox = labeled("Username", usernameField);
        VBox passBox = labeled("Password", passwordField);

        signInButton.getStyleClass().add("btn-primary");
        signInButton.setMaxWidth(Double.MAX_VALUE);
        signInButton.setDefaultButton(true);
        signInButton.setOnAction(e -> submitLogin());
        passwordField.setOnAction(e -> submitLogin());

        spinner.setPrefSize(18, 18);
        spinner.setVisible(false);
        spinner.setManaged(false);
        statusLabel.getStyleClass().add("text-secondary");
        statusLabel.setWrapText(true);
        HBox statusRow = new HBox(8, spinner, statusLabel);
        statusRow.setAlignment(Pos.CENTER_LEFT);

        card.getChildren().addAll(header, warningBanner, errorBanner, hostBox, portsRow, userBox, passBox, signInButton, statusRow);
        getChildren().add(card);
        StackPane.setAlignment(card, Pos.CENTER);
    }

    private VBox labeled(String title, TextField field) {
        Label label = new Label(title);
        label.getStyleClass().add("subsection-title");
        return new VBox(4, label, field);
    }

    private void submitLogin() {
        hideError();
        String host = hostField.getText() == null ? "" : hostField.getText().trim();
        int tcpPort;
        int udpPort;
        try {
            tcpPort = Integer.parseInt(tcpPortField.getText().trim());
            udpPort = Integer.parseInt(udpPortField.getText().trim());
        } catch (NumberFormatException ex) {
            showError("TCP and UDP ports must be valid integers between 1 and 65535.");
            return;
        }
        String username = usernameField.getText() == null ? "" : usernameField.getText().trim();
        String rawPass = passwordField.getText();
        char[] passwordChars = rawPass == null ? new char[0] : rawPass.toCharArray();
        passwordField.clear();

        setBusy(true, "Connecting and signing in...");
        var future = controller.connectAndLogin(host, tcpPort, udpPort, username, passwordChars);
        Arrays.fill(passwordChars, '\0');
        future.whenComplete((session, err) -> {
            if (err != null) {
                Throwable cause = err.getCause() != null ? err.getCause() : err;
                javafx.application.Platform.runLater(() -> {
                    setBusy(false, "Sign-in did not complete.");
                    showError(cause.getMessage());
                });
            }
        });
    }

    public void setStatus(boolean connecting, String statusMessage, String errorMessage) {
        setBusy(connecting, statusMessage);
        if (errorMessage != null && !errorMessage.isBlank()) {
            showError(errorMessage);
        }
    }

    private void setBusy(boolean busy, String status) {
        hostField.setDisable(busy);
        tcpPortField.setDisable(busy);
        udpPortField.setDisable(busy);
        usernameField.setDisable(busy);
        passwordField.setDisable(busy);
        signInButton.setDisable(busy);
        spinner.setVisible(busy);
        spinner.setManaged(busy);
        if (status != null && !status.isBlank()) {
            statusLabel.setText(status);
        }
    }

    private void showError(String msg) {
        errorLabel.setText(msg != null ? msg : "Sign-in failed.");
        errorBanner.setVisible(true);
        errorBanner.setManaged(true);
    }

    private void hideError() {
        errorBanner.setVisible(false);
        errorBanner.setManaged(false);
    }
}
