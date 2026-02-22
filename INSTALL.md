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

## Notes

- Opening the raw installer URL in a browser will show script text (expected).
- Users should run the installer command in Terminal.
- On first run, `run_server.sh` creates `.venv-py3` and installs Python dependencies automatically.
