# KiwiScan Installer

Use this page when sharing install instructions with end users.

## Quick install (recommended)

Run in Terminal:

```bash
curl -fsSL https://raw.githubusercontent.com/ristola/KiwiScan/main/tools/install_latest.sh | bash
```

Install to a custom location:

```bash
curl -fsSL https://raw.githubusercontent.com/ristola/KiwiScan/main/tools/install_latest.sh | bash -s -- "$HOME/KiwiScan"
```

## Start the server

Default install path:

```bash
cd /opt/kiwi_scan_prod
./run_server.sh
```

Custom install path example:

```bash
cd "$HOME/KiwiScan"
./run_server.sh
```

## Browser links

- Installer script (raw):
  - https://raw.githubusercontent.com/ristola/KiwiScan/main/tools/install_latest.sh
- macOS helper command file:
  - https://github.com/ristola/KiwiScan/raw/main/tools/install_latest.command

## macOS clickable installer (.pkg)

If you want users to click an installer file instead of running a Terminal command,
build and share the unsigned macOS package:

```bash
cd /opt/ShackMate/kiwi_scan
./tools/build_macos_installer_pkg.sh
```

Output:
- `dist/KiwiScan-Installer-<version>-unsigned.pkg`

Then upload that `.pkg` to a GitHub Release and share the release asset URL.
Users can download and open the package installer from Finder.

## Notes

- Opening the raw installer URL in a browser will show script text (expected).
- Users should run the installer command in Terminal.
- On first run, `run_server.sh` creates `.venv-py3` and installs Python dependencies automatically.
