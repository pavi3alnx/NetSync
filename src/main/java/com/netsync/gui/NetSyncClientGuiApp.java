package com.netsync.gui;

import com.netsync.common.SessionView;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** JavaFX Application entry point for the NetSync Desktop Client. */
public final class NetSyncClientGuiApp extends Application {
    private ClientSessionController controller;
    private ClientLoginView loginView;
    private ClientWorkspaceView workspaceView;
    private StackPane root;
    private Stage primaryStage;

    @Override
    public void start(Stage stage) {
        this.primaryStage = stage;
        controller = new ClientSessionController(Platform::runLater);
        loginView = new ClientLoginView(controller);
        workspaceView = new ClientWorkspaceView(controller);
        workspaceView.setVisible(false);
        workspaceView.setManaged(false);

        root = new StackPane(loginView, workspaceView);
        root.getStyleClass().add("app-root");

        controller.setListener(new ClientSessionController.Listener() {
            @Override
            public void onConnectionPhaseChanged(ClientModels.ConnectionPhase phase, SessionView session, String statusMessage) {
                switch (phase) {
                    case AUTHENTICATED -> {
                        if (session != null) {
                            workspaceView.onSessionEstablished(session);
                            showWorkspace(session.username());
                        }
                    }
                    case CONNECTING -> {
                        showLogin();
                        loginView.setStatus(true, statusMessage, null);
                    }
                    case LOGGING_OUT -> workspaceView.showNotice("WARNING", statusMessage);
                    case DISCONNECTED -> {
                        workspaceView.onSessionDisconnected();
                        showLogin();
                        loginView.setStatus(false, statusMessage, null);
                    }
                }
            }

            @Override
            public void onUsersChanged(List<ClientModels.UserItem> users) {
                workspaceView.onUsersChanged(users);
            }

            @Override
            public void onConversationUpdated(UUID peerClientId, List<ClientModels.ChatMessageItem> messages) {
                workspaceView.onConversationUpdated(peerClientId, messages);
            }

            @Override
            public void onTransfersChanged(List<ClientModels.TransferItem> transfers) {
                workspaceView.onTransfersChanged(transfers);
            }

            @Override
            public void onNotice(String severity, String message) {
                if (controller.phase() == ClientModels.ConnectionPhase.AUTHENTICATED) {
                    workspaceView.showNotice(severity, message);
                } else {
                    loginView.setStatus(false, message, "ERROR".equalsIgnoreCase(severity) ? message : null);
                }
            }
        });

        Scene scene = new Scene(root, 1180, 720);
        scene.getStylesheets().add(Objects.requireNonNull(
                NetSyncClientGuiApp.class.getResource("/com/netsync/gui/netsync-dark.css")).toExternalForm());

        stage.setTitle("NetSync — Sign In");
        stage.setMinWidth(900);
        stage.setMinHeight(620);
        stage.setScene(scene);
        stage.setOnCloseRequest(event -> shutdownAsync());
        stage.show();
    }

    private void showWorkspace(String username) {
        loginView.setVisible(false);
        loginView.setManaged(false);
        workspaceView.setVisible(true);
        workspaceView.setManaged(true);
        primaryStage.setTitle("NetSync — " + username);
    }

    private void showLogin() {
        workspaceView.setVisible(false);
        workspaceView.setManaged(false);
        loginView.setVisible(true);
        loginView.setManaged(true);
        primaryStage.setTitle("NetSync — Sign In");
    }

    @Override
    public void stop() {
        if (workspaceView != null) {
            workspaceView.dispose();
        }
        if (controller != null) {
            controller.close();
        }
    }

    private void shutdownAsync() {
        if (workspaceView != null) {
            workspaceView.dispose();
        }
        ClientSessionController ref = controller;
        if (ref != null) {
            Thread closer = new Thread(ref::close, "netsync-client-gui-close");
            closer.setDaemon(true);
            closer.start();
        }
    }
}
