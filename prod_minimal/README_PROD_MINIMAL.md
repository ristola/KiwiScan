# kiwi_scan production-minimal bundle

This folder contains the minimum runtime set to run the web server safely:

- `src/`
- `vendor/kiwiclient-jks/kiwi/` (vendored Kiwi client package)
- `run_server.sh`
- `requirements.txt`
- `outputs/` (runtime-generated data)
- `.env.example`

## Run

```zsh
cd /opt/ShackMate/kiwi_scan/prod_minimal
python3 -m venv .venv-py3
source .venv-py3/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt
NO_RESTART=1 PORT=4020 ./run_server.sh
```

## Notes

- This bundle intentionally excludes development-only folders such as `tests/` and `tools/`.
- If you need CLI packaging/install mode, include `pyproject.toml` as well.
