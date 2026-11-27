package com.netsync.gui;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.util.Objects;

/** JavaFX Application entry point for the NetSync Server Administration Dashboard. */
public final class NetSyncServerGuiApp extends Application {
    private ServerDashboardController controller;
    private ServerDashboardView view;

    @Override
    public void start(Stage primaryStage) {
        controller = new ServerDashboardController(Platform::runLater);
        view = new ServerDashboardView(controller);

        Scene scene = new Scene(view, 1180, 720);
        scene.getStylesheets().add(Objects.requireNonNull(
                NetSyncServerGuiApp.class.getResource("/com/netsync/gui/netsync-dark.css")).toExternalForm());

        primaryStage.setTitle("NetSync — Server Administration Dashboard");
        primaryStage.setMinWidth(960);
        primaryStage.setMinHeight(640);
        primaryStage.setScene(scene);
        primaryStage.setOnCloseRequest(event -> shutdownAsync());
        primaryStage.show();
    }

    @Override
    public void stop() {
        if (view != null) {
            view.dispose();
        }
        if (controller != null) {
            controller.close();
        }
    }

    private void shutdownAsync() {
        if (view != null) {
            view.dispose();
        }
        ServerDashboardController ref = controller;
        if (ref != null) {
            Thread closer = new Thread(ref::close, "netsync-server-gui-close");
            closer.setDaemon(true);
            closer.start();
        }
    }
}
