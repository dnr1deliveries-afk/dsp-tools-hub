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

# ── Default webhooks (DNR1 DSPs) ──────────────────────────────────────────────
DEFAULT_WEBHOOKS = {
    "ATAG": {
        "metrics": "https://hooks.slack.com/triggers/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",
        "ops":     "https://hooks.slack.com/triggers/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",
    },
    "DELL": {
        "metrics": "https://hooks.slack.com/triggers/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",
        "ops":     "https://hooks.slack.com/triggers/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",
    },
    "DTTD": {
        "metrics": "https://hooks.slack.com/triggers/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",
        "ops":     "https://hooks.slack.com/triggers/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",
    },
    "DNZN": {
        "metrics": "https://hooks.slack.com/triggers/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",
        "ops":     "https://hooks.slack.com/triggers/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",
    },
    "DYYL": {
        "metrics": "https://hooks.slack.com/triggers/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",
        "ops":     "https://hooks.slack.com/triggers/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",
    },
    "HPLM": {
        "metrics": "https://hooks.slack.com/triggers/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",
        "ops":     "https://hooks.slack.com/triggers/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",
    },
    "KMIL": {
        "metrics": "https://hooks.slack.com/triggers/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",
        "ops":     "https://hooks.slack.com/triggers/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",
    },
    "MOLI": {
        "metrics": "https://hooks.slack.com/triggers/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",
        "ops":     "https://hooks.slack.com/triggers/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",
    },
    "SLTD": {
        "metrics": "https://hooks.slack.com/triggers/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",
        "ops":     "https://hooks.slack.com/triggers/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",
    },
    "ULSL": {
        "metrics": "https://hooks.slack.com/triggers/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",
        "ops":     "https://hooks.slack.com/triggers/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",
    },
    "VILO": {
        "metrics": "https://hooks.slack.com/triggers/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",
        "ops":     "https://hooks.slack.com/triggers/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",
    },
    "WACC": {
        "metrics": "https://hooks.slack.com/triggers/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",
        "ops":     "https://hooks.slack.com/triggers/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",
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


# ── Public API ─────────────────────────────────────────────────────────────────

def load_webhooks() -> dict:
    """
    Load webhook config. Priority: GitHub → local → defaults.
    Merges with defaults so new DSPs always appear even if config is stale.
    Result is cached in-process.
    """
    global _cache
    if _cache is not None:
        return _cache

    data = _gh_read()
    if data is None:
        data = _local_read()
    if data is None:
        data = {}

    # Merge: defaults fill in any missing DSPs but don't overwrite saved values
    merged = {dsp: dict(urls) for dsp, urls in DEFAULT_WEBHOOKS.items()}
    for dsp, urls in data.items():
        merged[dsp] = urls

    _cache = merged
    return _cache


def save_webhooks(data: dict) -> bool:
    """Save webhook config to GitHub (primary) and local (backup). Invalidates cache."""
    global _cache
    ok = _gh_write(data)
    _local_write(data)   # always write locally as backup
    _cache = data        # update cache immediately
    return ok


def get_webhooks_for_channel(channel: str) -> dict:
    """
    Return {DSP: url} for a given channel ('metrics' or 'ops').
    Only includes DSPs that have a non-empty URL for that channel.
    """
    key  = channel.lower()
    data = load_webhooks()
    return {dsp: urls[key] for dsp, urls in data.items()
            if urls.get(key, '').strip()}


def invalidate_cache():
    """Force reload on next load_webhooks() call."""
    global _cache
    _cache = None
