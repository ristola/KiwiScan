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

## Docker install

Use this when you want to run KiwiScan directly from the published Docker image.

1. Install Docker Desktop or Docker Engine and make sure Docker is running.

  ```bash
  docker version
  ```

2. Start the container.

  ```bash
  docker run -d --name kiwiscan --pull always --restart unless-stopped --platform linux/amd64 \
    -p 4010:4010/tcp \
    -p 4010:4010/udp \
    -p 4020:4020 \
    -v kiwiscan-config:/opt/kiwiscan/config \
    -v kiwiscan-outputs:/opt/kiwiscan/outputs \
    n4ldr/kiwiscan:0.1.9
  ```

3. Open KiwiScan in your browser.

  ```text
  http://localhost:4020
  ```

4. Confirm the container started cleanly.

  ```bash
  docker logs --tail 50 kiwiscan
  ```

5. Confirm the first-run config was saved.

  ```bash
  docker run --rm -v kiwiscan-outputs:/data alpine cat /data/config.json
  ```

If you ran that command from a test folder and the folder stayed empty, that is expected. The documented command uses Docker named volumes, so Docker stores the data outside the current directory.

Use this version instead when you want the files to appear in the folder you launched from:

```bash
mkdir -p config outputs

docker run -d --name kiwiscan --pull always --restart unless-stopped --platform linux/amd64 \
  -p 4010:4010/tcp \
  -p 4010:4010/udp \
  -p 4020:4020 \
  -v "$PWD/config:/opt/kiwiscan/config" \
  -v "$PWD/outputs:/opt/kiwiscan/outputs" \
  n4ldr/kiwiscan:0.1.9
```

With that folder-backed version, the saved config will appear at `./outputs/config.json`.

Ports exposed by the container:

- `4020/tcp`: web UI and HTTP API
- `4010/tcp`: legacy WebSocket decode stream
- `4010/udp`: UDP decode publisher

Notes:

- Docker downloads the image automatically on first run, so a separate `docker pull` step is not required.
- This direct `docker run` flow does not use the repo's `docker-compose.yml`.
- The named Docker volumes preserve config and outputs across container restarts.
- `n4ldr/kiwiscan:0.1.9` is the immutable release image for this version; `n4ldr/kiwiscan:latest` is a rolling tag and may contain newer main-branch fixes.

Useful commands:

```bash
docker ps
docker stop kiwiscan
docker start kiwiscan
docker rm -f kiwiscan
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
