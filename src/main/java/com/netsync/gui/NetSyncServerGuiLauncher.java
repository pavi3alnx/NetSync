package com.netsync.gui;

import javafx.application.Application;

/**
 * Classpath-safe launcher for the NetSync Server GUI.
 * Invoking a non-Application main class allows launching JavaFX cleanly from standard Maven/batch classpaths.
 */
public final class NetSyncServerGuiLauncher {
    private NetSyncServerGuiLauncher() {}

    public static void main(String[] args) {
        Application.launch(NetSyncServerGuiApp.class, args);
    }
}
