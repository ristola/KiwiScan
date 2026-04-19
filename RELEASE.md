# KiwiScan Release Guide (macOS Installer)

This guide is for maintainers who want a user-clickable installer package.

## 1) Build unsigned package (quick)

```bash
cd /opt/ShackMate/kiwi_scan
./tools/build_macos_installer_pkg.sh
```

Output:
- `dist/KiwiScan-Installer-<version>-unsigned.pkg`

## Version + Git release step (recommended every working update)

Use this helper to ensure each working update bumps version, commits, and pushes:

```bash
cd /opt/ShackMate/kiwi_scan
./tools/release_commit.sh
```

Options:
- explicit version: `./tools/release_commit.sh --version 0.1.2`
- commit only (no push): `./tools/release_commit.sh --no-push`

What this does:
- bumps `pyproject.toml`
- commits staged + tracked changes as `Release v<version>`
- creates annotated tag `v<version>`
- pushes `main` and the new tag unless `--no-push` is used

## Docker image release

The published container lives on Docker Hub:
- `https://hub.docker.com/r/n4ldr/kiwiscan`

Publish the current local image using the version from `pyproject.toml`:

```bash
cd /opt/ShackMate/kiwi_scan
./tools/publish_docker.sh
```

Build first, then publish:

```bash
cd /opt/ShackMate/kiwi_scan
./tools/publish_docker.sh --build
```

What this does:
- tags `kiwiscan-local:latest` as `n4ldr/kiwiscan:<version>`
- pushes the versioned tag
- pushes `n4ldr/kiwiscan:latest` unless `--no-latest` is used

Before any push, log in to Docker Hub on the publishing machine:

```bash
docker login
```

Publish tester-only tags without moving the immutable release tag or `latest`:

```bash
cd /opt/ShackMate/kiwi_scan
./tools/publish_docker.sh --build --no-version --no-latest \
  --tag test-latest \
  --tag test-20260418
```

What this does:
- builds `kiwiscan-local:latest`
- tags it as `n4ldr/kiwiscan:test-latest`
- tags it as `n4ldr/kiwiscan:test-20260418`
- pushes only those tester tags

Tag policy:
- `n4ldr/kiwiscan:<version>` should remain the immutable release snapshot for that version.
- `n4ldr/kiwiscan:latest` may move ahead of the newest numbered release when main-branch follow-up fixes are published.
- `n4ldr/kiwiscan:test-latest` is the rolling tester tag.
- `n4ldr/kiwiscan:test-YYYYMMDD` is a reproducible tester snapshot for a specific build date.

Recommended release sequence:
1. Bump and commit the app version with `./tools/release_commit.sh`
2. Build and publish the Docker image with `./tools/publish_docker.sh --build`
3. Verify the git tag with `git rev-list -n 1 v<version>`
4. Verify the published image with `docker pull n4ldr/kiwiscan:<version>`
5. Update or redeploy Compose consumers pinned to that tag

## 2) Build signed/notarized package (recommended)

Use the release helper script:

```bash
cd /opt/ShackMate/kiwi_scan
SIGN_PKG=1 \
PKG_SIGN_IDENTITY="Developer ID Installer: YOUR NAME (TEAMID)" \
./tools/release_macos_pkg.sh
```

Optional notarization (requires notarytool keychain profile):

```bash
cd /opt/ShackMate/kiwi_scan
SIGN_PKG=1 \
NOTARIZE=1 \
PKG_SIGN_IDENTITY="Developer ID Installer: YOUR NAME (TEAMID)" \
NOTARY_PROFILE="AC_PROFILE" \
./tools/release_macos_pkg.sh
```

## 3) Publish package

Upload the generated `.pkg` from `dist/` to a GitHub Release in:
- `https://github.com/ristola/KiwiScan/releases`

## 4) Share with users

Share either:
- Release asset URL to the `.pkg` file (click/download/install), or
- Install guide URL:
  - `https://github.com/ristola/KiwiScan/blob/main/INSTALL.md`

## Notes

- Unsigned packages may trigger stronger Gatekeeper warnings.
- Signed + notarized packages provide the best click-to-install experience.
