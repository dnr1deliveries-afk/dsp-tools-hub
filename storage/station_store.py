"""
DSP Tools Hub — Station Storage
Per-station webhook storage with GitHub persistence.

Storage layout:
    hub_data/stations/{STATION_CODE}/webhooks.json
    hub_data/stations/{STATION_CODE}/settings.json

User preferences (station choice) stored in session.

v1.5 - Public repo reads don't require token (only writes do)
"""

import os
import json
import base64
import logging
import requests

logger = logging.getLogger('hub.station')

# ── GitHub config from env vars ───────────────────────────────────────────────
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '')
GITHUB_REPO  = os.environ.get('GITHUB_REPO', 'dnr1deliveries-afk/dsp-tools-hub')

LOCAL_BASE = os.path.join(os.path.dirname(__file__), '..', 'hub_data', 'stations')

# ── Default settings ──────────────────────────────────────────────────────────
DEFAULT_SETTINGS = {
    'payload_key': 'message',
}

# ── Default webhooks template ─────────────────────────────────────────────────
DEFAULT_WEBHOOKS = {}  # Empty - each station configures their own


# ── GitHub helpers ────────────────────────────────────────────────────────────

def _gh_headers(auth_required: bool = False):
    """Get headers for GitHub API. Auth only needed for writes."""
    headers = {'Accept': 'application/vnd.github.v3+json'}
    if auth_required and GITHUB_TOKEN:
        headers['Authorization'] = f'token {GITHUB_TOKEN}'
    return headers


def _gh_path(station_code: str, filename: str) -> str:
    """Get GitHub repo path for a station file."""
    return f'hub_data/stations/{station_code.upper()}/{filename}'


def _gh_read(station_code: str, filename: str) -> dict | None:
    """Read JSON file from GitHub for a station. No auth needed for public repos."""
    if not GITHUB_REPO:
        return None
    url = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{_gh_path(station_code, filename)}'
    try:
        r = requests.get(url, headers=_gh_headers(auth_required=False), timeout=10)
        if r.status_code == 200:
            content = base64.b64decode(r.json()['content']).decode('utf-8')
            return json.loads(content)
        elif r.status_code == 404:
            return {}
        else:
            logger.warning(f'GitHub read returned {r.status_code} for {station_code}/{filename}')
    except Exception as e:
        logger.warning(f'GitHub read failed for {station_code}/{filename}: {e}')
    return None


def _gh_write(station_code: str, filename: str, data: dict) -> bool:
    """Write JSON file to GitHub for a station. Auth required."""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        logger.warning(f'GitHub write skipped - missing token or repo config')
        return False
    url = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{_gh_path(station_code, filename)}'
    try:
        # Get current SHA if file exists
        sha = None
        r = requests.get(url, headers=_gh_headers(auth_required=True), timeout=10)
        if r.status_code == 200:
            sha = r.json().get('sha')

        payload = {
            'message': f'hub: update {station_code}/{filename}',
            'content': base64.b64encode(json.dumps(data, indent=2).encode()).decode(),
        }
        if sha:
            payload['sha'] = sha

        r = requests.put(url, headers=_gh_headers(auth_required=True), json=payload, timeout=15)
        if r.status_code in (200, 201):
            return True
        else:
            logger.warning(f'GitHub write returned {r.status_code} for {station_code}/{filename}')
            return False
    except Exception as e:
        logger.warning(f'GitHub write failed for {station_code}/{filename}: {e}')
        return False


# ── Local filesystem helpers ──────────────────────────────────────────────────

def _local_dir(station_code: str) -> str:
    """Get local directory for a station."""
    path = os.path.join(LOCAL_BASE, station_code.upper())
    os.makedirs(path, exist_ok=True)
    return path


def _local_read(station_code: str, filename: str) -> dict | None:
    """Read JSON file from local filesystem."""
    filepath = os.path.join(_local_dir(station_code), filename)
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f'Local read failed for {station_code}/{filename}: {e}')
    return None


def _local_write(station_code: str, filename: str, data: dict) -> bool:
    """Write JSON file to local filesystem."""
    try:
        filepath = os.path.join(_local_dir(station_code), filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        logger.warning(f'Local write failed for {station_code}/{filename}: {e}')
        return False


# ── Public API ────────────────────────────────────────────────────────────────

def load_station_webhooks(station_code: str) -> dict:
    """Load webhooks for a station. GitHub → local → empty."""
    station = station_code.upper()
    
    # Try GitHub first (no auth needed for public repo)
    data = _gh_read(station, 'webhooks.json')
    if data is not None and data:
        # Cache locally
        _local_write(station, 'webhooks.json', data)
        return data
    
    # Try local
    data = _local_read(station, 'webhooks.json')
    if data is not None:
        return data
    
    return dict(DEFAULT_WEBHOOKS)


def save_station_webhooks(station_code: str, webhooks: dict) -> bool:
    """Save webhooks for a station to GitHub and local."""
    station = station_code.upper()
    
    ok = _gh_write(station, 'webhooks.json', webhooks)
    _local_write(station, 'webhooks.json', webhooks)
    
    return ok


def load_station_settings(station_code: str) -> dict:
    """Load settings for a station."""
    station = station_code.upper()
    
    data = _gh_read(station, 'settings.json')
    if data is not None and data:
        _local_write(station, 'settings.json', data)
        settings = dict(DEFAULT_SETTINGS)
        settings.update(data)
        return settings
    
    data = _local_read(station, 'settings.json')
    if data is not None:
        settings = dict(DEFAULT_SETTINGS)
        settings.update(data)
        return settings
    
    return dict(DEFAULT_SETTINGS)


def save_station_settings(station_code: str, settings: dict) -> bool:
    """Save settings for a station."""
    station = station_code.upper()
    
    ok = _gh_write(station, 'settings.json', settings)
    _local_write(station, 'settings.json', settings)
    
    return ok


def get_webhooks_for_channel(station_code: str, channel: str) -> dict:
    """Return {DSP: url} for a given channel at a station."""
    webhooks = load_station_webhooks(station_code)
    key = channel.lower()
    return {
        dsp: urls[key] 
        for dsp, urls in webhooks.items()
        if isinstance(urls, dict) and urls.get(key, '').strip()
    }


def get_payload_key(station_code: str) -> str:
    """Get the payload key for a station."""
    settings = load_station_settings(station_code)
    return settings.get('payload_key', 'message')


def list_stations() -> list:
    """List all stations with saved data."""
    stations = []
    
    # Check local directories
    if os.path.exists(LOCAL_BASE):
        for dirname in os.listdir(LOCAL_BASE):
            dirpath = os.path.join(LOCAL_BASE, dirname)
            if os.path.isdir(dirpath) and dirname == dirname.upper():
                webhooks = load_station_webhooks(dirname)
                stations.append({
                    'code': dirname,
                    'dsp_count': len([k for k in webhooks.keys() if k != '_settings']),
                })
    
    return sorted(stations, key=lambda x: x['code'])
