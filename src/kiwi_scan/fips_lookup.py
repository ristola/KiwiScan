"""FIPS → county/state name resolution for NOAA SAME/EAS alerts.

SAME location codes are 6-digit PSSCCC strings:
  P   = subdivision (0 = whole county)
  SS  = 2-digit state FIPS
  CCC = 3-digit county FIPS

Cache is at outputs/fips_cache.json (built from Census data on first use).
"""

import json
import logging
import urllib.request
from pathlib import Path

log = logging.getLogger(__name__)

_CENSUS_URL = (
    "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"
)
_CACHE_PATH = Path("outputs/fips_cache.json")

_STATE_ABBR: dict[str, str] = {
    "01":"AL","02":"AK","04":"AZ","05":"AR","06":"CA","08":"CO","09":"CT",
    "10":"DE","11":"DC","12":"FL","13":"GA","15":"HI","16":"ID","17":"IL",
    "18":"IN","19":"IA","20":"KS","21":"KY","22":"LA","23":"ME","24":"MD",
    "25":"MA","26":"MI","27":"MN","28":"MS","29":"MO","30":"MT","31":"NE",
    "32":"NV","33":"NH","34":"NJ","35":"NM","36":"NY","37":"NC","38":"ND",
    "39":"OH","40":"OK","41":"OR","42":"PA","44":"RI","45":"SC","46":"SD",
    "47":"TN","48":"TX","49":"UT","50":"VT","51":"VA","53":"WA","54":"WV",
    "55":"WI","56":"WY","60":"AS","66":"GU","69":"MP","72":"PR","78":"VI",
}

_fips: dict[str, list] = {}
_loaded = False


def _load() -> None:
    global _fips, _loaded
    if _loaded:
        return
    if _CACHE_PATH.exists():
        try:
            _fips = json.loads(_CACHE_PATH.read_text())
            _loaded = True
            log.debug("FIPS cache loaded: %d counties", len(_fips))
            return
        except Exception as exc:
            log.warning("FIPS cache read failed: %s", exc)
    _fetch_and_cache()
    _loaded = True


def _fetch_and_cache() -> None:
    global _fips
    try:
        log.info("Fetching FIPS county data from Census Bureau…")
        resp = urllib.request.urlopen(_CENSUS_URL, timeout=20)
        text = resp.read().decode("latin-1", errors="ignore")
        result: dict[str, list] = {}
        for line in text.splitlines()[1:]:
            parts = line.strip().split("|")
            if len(parts) < 5:
                continue
            state_abbr, state_fp, county_fp = parts[0], parts[1], parts[2]
            county_name = parts[4]
            result[f"{state_fp}{county_fp}"] = [county_name, state_abbr]
        _fips = result
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_PATH.write_text(json.dumps(result, separators=(",", ":")))
        log.info("FIPS county cache built: %d entries → %s", len(result), _CACHE_PATH)
    except Exception as exc:
        log.warning("FIPS cache fetch failed (will use state-only fallback): %s", exc)


def resolve_same_code(code: str) -> str:
    """Convert a 6-digit SAME location code to 'County Name, ST'.

    Returns the raw code if it cannot be resolved.
    """
    _load()
    if len(code) != 6 or not code.isdigit():
        return code
    state_fp  = code[1:3]   # skip P (subdivision) prefix digit
    county_fp = code[3:6]
    entry = _fips.get(f"{state_fp}{county_fp}")
    if entry:
        return f"{entry[0]}, {entry[1]}"
    abbr = _STATE_ABBR.get(state_fp)
    return f"{abbr} county {county_fp}" if abbr else code


def resolve_same_areas(codes: list[str]) -> list[str]:
    """Resolve a list of SAME location codes to human-readable strings."""
    return [resolve_same_code(c) for c in (codes or [])]
