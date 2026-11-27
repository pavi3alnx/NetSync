# NetSync
Java NIO-based real-time communication system with TCP chat, UDP presence, authentication, and file sharing.

## Desktop GUI Quick Start (JavaFX)

1. Build once to package the JAR and JavaFX runtime libraries (`target/lib/`):
   - Windows: double-click `Build-NetSync.bat`
   - CLI: `mvn -B clean package -DskipTests`
2. Launch the server dashboard: double-click `Start-NetSync-Server.bat`
3. Launch one or more client windows: double-click `Start-NetSync-Client.bat`

See [docs/gui-demo.md](docs/gui-demo.md) for complete GUI launch commands, demonstration steps, and known limitations.
