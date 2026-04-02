"""
DSP Tools Hub — Webhook Storage
Stores dsp_webhooks.json in GitHub repo (primary) with local file fallback.
Pattern mirrors ATLAS Web's user_storage.py / github_storage.py.
"""

import os
import json
import base64
import logging
import requests

logger = logging.getLogger('hub.webhooks')

# ── GitHub config from env vars ───────────────────────────────────────────────
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '')
GITHUB_REPO  = os.environ.get('GITHUB_REPO', '')
WEBHOOK_PATH = 'hub_data/dsp_webhooks.json'   # path inside the repo

LOCAL_PATH   = os.path.join(os.path.dirname(__file__), '..', 'hub_data', 'dsp_webhooks.json')

# ── Default settings ──────────────────────────────────────────────────────────
DEFAULT_SETTINGS = {
    'payload_key': 'message',   # JSON key used in webhook payload: {"message": "..."}
}

# ── Default webhooks (DNR1 DSPs) ──────────────────────────────────────────────
# NOTE: Replace these placeholder URLs with your actual Slack webhook URLs
DEFAULT_WEBHOOKS = {
    "ATAG": {
        "metrics": "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
        "ops":     "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
    },
    "DELL": {
        "metrics": "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
        "ops":     "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
    },
    "DTTD": {
        "metrics": "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
        "ops":     "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
    },
    "DNZN": {
        "metrics": "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
        "ops":     "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
    },
    "DYYL": {
        "metrics": "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
        "ops":     "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
    },
    "HPLM": {
        "metrics": "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
        "ops":     "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
    },
    "KMIL": {
        "metrics": "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
        "ops":     "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
    },
    "MOLI": {
        "metrics": "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
        "ops":     "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
    },
    "SLTD": {
        "metrics": "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
        "ops":     "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
    },
    "ULSL": {
        "metrics": "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
        "ops":     "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
    },
    "VILO": {
        "metrics": "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
        "ops":     "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
    },
    "WACC": {
        "metrics": "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
        "ops":     "https://hooks.slack.com/triggers/YOUR_WORKSPACE/YOUR_TRIGGER_ID/YOUR_SECRET",
    },
}

# ── In-process cache (avoids hammering GitHub API) ────────────────────────────
_cache = None


def _gh_headers():
    return {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
    }


def _gh_read() -> dict | None:
    """Read webhooks.json from GitHub. Returns parsed dict or None on failure."""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return None
    url = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{WEBHOOK_PATH}'
    try:
        r = requests.get(url, headers=_gh_headers(), timeout=10)
        if r.status_code == 200:
            content = base64.b64decode(r.json()['content']).decode('utf-8')
            return json.loads(content)
        elif r.status_code == 404:
            return {}   # file doesn't exist yet — treat as empty
    except Exception as e:
        logger.warning(f'GitHub read failed: {e}')
    return None


def _gh_write(data: dict) -> bool:
    """Write webhooks.json to GitHub. Creates or updates the file."""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return False
    url = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{WEBHOOK_PATH}'
    try:
        # Get current SHA if file exists (needed for update)
        sha = None
        r = requests.get(url, headers=_gh_headers(), timeout=10)
        if r.status_code == 200:
            sha = r.json().get('sha')

        payload = {
            'message': 'hub: update dsp_webhooks.json',
            'content': base64.b64encode(json.dumps(data, indent=2).encode()).decode(),
        }
        if sha:
            payload['sha'] = sha

        r = requests.put(url, headers=_gh_headers(), json=payload, timeout=15)
        return r.status_code in (200, 201)
    except Exception as e:
        logger.warning(f'GitHub write failed: {e}')
        return False


def _local_read() -> dict | None:
    """Read webhooks.json from local filesystem fallback."""
    try:
        if os.path.exists(LOCAL_PATH):
            with open(LOCAL_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f'Local webhook read failed: {e}')
    return None


def _local_write(data: dict) -> bool:
    """Write webhooks.json to local filesystem."""
    try:
        os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)
        with open(LOCAL_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        logger.warning(f'Local webhook write failed: {e}')
        return False


def _load_raw_data() -> dict:
    """Load raw data from storage (GitHub or local), without cache."""
    data = _gh_read()
    if data is None:
        data = _local_read()
    if data is None:
        data = {}
    return data


# ── Public API ────────────────────────────────────────────────────────────────

def load_webhooks() -> dict:
    """
    Load webhook config (DSPs only, excludes _settings).
    Priority: GitHub → local → defaults.
    Merges with defaults so new DSPs always appear even if config is stale.
    Result is cached in-process.
    """
    global _cache
    if _cache is not None:
        return _cache

    data = _load_raw_data()

    # Merge: defaults fill in any missing DSPs but don't overwrite saved values
    merged = {dsp: dict(urls) for dsp, urls in DEFAULT_WEBHOOKS.items()}
    for dsp, urls in data.items():
        if dsp != '_settings':  # Skip settings key
            merged[dsp] = urls

    _cache = merged
    return _cache


def save_webhooks(data: dict) -> bool:
    """Save webhook config to GitHub (primary) and local (backup). Invalidates cache."""
    global _cache
    
    # Preserve existing settings when saving webhooks
    existing = _load_raw_data()
    if '_settings' in existing:
        data['_settings'] = existing['_settings']
    
    ok = _gh_write(data)
    _local_write(data)   # always write locally as backup
    
    # Update cache (excluding _settings)
    _cache = {k: v for k, v in data.items() if k != '_settings'}
    return ok


def get_webhooks_for_channel(channel: str) -> dict:
    """
    Return {DSP: url} for a given channel ('metrics' or 'ops').
    Only includes DSPs that have a non-empty URL for that channel.
    """
    key  = channel.lower()
    data = load_webhooks()
    return {dsp: urls[key] for dsp, urls in data.items()
            if isinstance(urls, dict) and urls.get(key, '').strip()}


def invalidate_cache():
    """Force reload on next load_webhooks() call."""
    global _cache
    _cache = None


# ── Settings API ──────────────────────────────────────────────────────────────

def load_settings() -> dict:
    """Load global settings. Returns defaults merged with saved values."""
    data = _load_raw_data()
    saved_settings = data.get('_settings', {})
    
    # Merge with defaults
    settings = dict(DEFAULT_SETTINGS)
    settings.update(saved_settings)
    return settings


def save_settings(settings: dict) -> bool:
    """Save global settings to storage."""
    data = _load_raw_data()
    data['_settings'] = settings
    
    ok = _gh_write(data)
    _local_write(data)
    return ok


def get_payload_key() -> str:
    """Get the JSON key used for webhook payloads (default: 'message')."""
    settings = load_settings()
    return settings.get('payload_key', 'message')
