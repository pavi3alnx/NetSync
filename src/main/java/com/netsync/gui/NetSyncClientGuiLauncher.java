package com.netsync.gui;

import javafx.application.Application;

/**
 * Classpath-safe launcher for the NetSync Client GUI.
 * Allows launching JavaFX cleanly from standard Maven/batch classpaths and multiple independent windows.
 */
public final class NetSyncClientGuiLauncher {
    private NetSyncClientGuiLauncher() {}

    public static void main(String[] args) {
        Application.launch(NetSyncClientGuiApp.class, args);
    }
}
