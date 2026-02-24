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
