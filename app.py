"""
DSP Tools Hub — Web Application v1.0
Flask web interface for the DSP Tools Hub.
Ported from DSP_Tools_Hub.py (desktop tkinter app) v1.6
Deploy: Docker → Render.com
Repo:   dnr1deliveries-afk/dsp-tools-hub
"""

import os
import io
import logging
import requests
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, session, jsonify
)

from processing.dsp_core import (
    generate_chase_messages,
    generate_pickup_messages,
    generate_rostering_messages,
    generate_stc_messages,
    generate_cc_messages,
    generate_pod_messages,
    generate_noa_messages,
)
from storage.webhook_store import (
    load_webhooks, save_webhooks,
    get_webhooks_for_channel, invalidate_cache,
    DEFAULT_WEBHOOKS,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.environ.get('LOG_LEVEL', 'INFO').upper(),
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger('hub')

# ── App config ────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dsp-hub-dev-key-change-in-prod')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024   # 50 MB

VERSION    = '1.0'
BUILD_DATE = '2026-03-26'

# ── Tool registry — single source of truth for all 7 tools ───────────────────
TOOLS = {
    'chase': {
        'name':      'DSP Chase',
        'icon':      'bi-search',
        'emoji':     '🔍',
        'desc':      'Outstanding scrub error shipments per DSP',
        'files':     [{'id': 'csv_file', 'label': 'Scrub Error CSV',
                       'hint': 'OUTSTANDING SCRUB ERROR*.csv', 'required': True}],
        'safe_affected': False,
    },
    'pickups': {
        'name':      'DSP Pickups',
        'icon':      'bi-mailbox',
        'emoji':     '📬',
        'desc':      'Awaiting pickup messages with optional route lookup',
        'files':     [{'id': 'csv_file',    'label': 'Awaiting Pickup CSV',
                       'hint': 'AWAITING PICK UP*.csv',   'required': True},
                      {'id': 'search_file', 'label': 'SearchResults CSV (optional)',
                       'hint': 'SearchResults*.csv — adds Route Codes', 'required': False}],
        'safe_affected': False,
    },
    'rostering': {
        'name':      'Rostering',
        'icon':      'bi-clipboard-check',
        'emoji':     '📋',
        'desc':      'Daily rostering compliance per DSP and service type',
        'files':     [{'id': 'csv_file', 'label': 'Rostering Capacity CSV',
                       'hint': 'Rostering_Capacity_C_*.csv', 'required': True}],
        'safe_affected': False,
    },
    'stc': {
        'name':      'STC',
        'icon':      'bi-truck',
        'emoji':     '🚚',
        'desc':      'Service Type Compliance — D-1 vs D-0 vehicle swaps',
        'files':     [{'id': 'csv_file', 'label': 'STC Deep Dive CSV',
                       'hint': 'Dive Deep Data Service Type Compliance*.csv', 'required': True}],
        'safe_affected': True,
        'safe_note':  'Safe Mode: VINs replaced with anonymised DA-XXXX tokens',
    },
    'cc': {
        'name':      'Contact Compliance',
        'icon':      'bi-telephone-fill',
        'emoji':     '📞',
        'desc':      'Failed deliveries with contact data (excludes NOA rows)',
        'files':     [{'id': 'csv_file', 'label': 'CC Exceptions CSV',
                       'hint': 'Exceptions_Based_Dee_*.csv', 'required': True}],
        'safe_affected': True,
        'safe_note':  'Safe Mode: Transporter IDs replaced with anonymised DA-XXXX tokens',
    },
    'pod': {
        'name':      'POD',
        'icon':      'bi-camera-fill',
        'emoji':     '📷',
        'desc':      'Picture on Delivery opportunities (excludes manual bypasses)',
        'files':     [{'id': 'csv_file', 'label': 'POD Summary CSV',
                       'hint': 'POD_Summary_*.csv', 'required': True}],
        'safe_affected': True,
        'safe_note':  'Safe Mode: DA IDs replaced with anonymised DA-XXXX tokens',
    },
    'noa': {
        'name':      'NOA',
        'icon':      'bi-bell-fill',
        'emoji':     '🔔',
        'desc':      'Notify of Arrival counts per driver',
        'files':     [{'id': 'csv_file', 'label': 'NOA Exceptions CSV',
                       'hint': 'Exceptions_Based_Dee_*.csv (same file as CC)', 'required': True}],
        'safe_affected': True,
        'safe_note':  'Safe Mode: Transporter IDs replaced with anonymised DA-XXXX tokens',
    },
}

# Map tool_id → generate function
GENERATORS = {
    'chase':     generate_chase_messages,
    'pickups':   generate_pickup_messages,
    'rostering': generate_rostering_messages,
    'stc':       generate_stc_messages,
    'cc':        generate_cc_messages,
    'pod':       generate_pod_messages,
    'noa':       generate_noa_messages,
}


# ── Context processor ─────────────────────────────────────────────────────────
@app.context_processor
def inject_globals():
    return {
        'version':   VERSION,
        'tools':     TOOLS,
        'safe_mode': session.get('safe_mode', False),
    }


# ── Session helpers ───────────────────────────────────────────────────────────
def get_messages(tool_id: str) -> dict:
    return session.get(f'messages_{tool_id}', {})


def store_messages(tool_id: str, messages: dict):
    # Flask sessions can't store large dicts directly if they exceed cookie limit.
    # We store in app-level dict keyed by session ID for reliability.
    sid = session.get('sid')
    if sid:
        _msg_store[sid][tool_id] = messages


def get_stored_messages(tool_id: str) -> dict:
    sid = session.get('sid')
    if sid and sid in _msg_store:
        return _msg_store[sid].get(tool_id, {})
    return {}


# In-memory message store (lightweight — messages are ephemeral per session)
import uuid
from collections import defaultdict
_msg_store = defaultdict(dict)


@app.before_request
def ensure_session():
    if 'sid' not in session:
        session['sid'] = str(uuid.uuid4())[:8]
        session.permanent = False


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


# ── Safe Mode toggle (AJAX) ───────────────────────────────────────────────────
@app.route('/set-safe-mode', methods=['POST'])
def set_safe_mode():
    body = request.get_json(silent=True) or {}
    session['safe_mode'] = bool(body.get('safe_mode', False))
    return jsonify({'ok': True, 'safe_mode': session['safe_mode']})


# ── Tool pages ────────────────────────────────────────────────────────────────

@app.route('/tool/<tool_id>', methods=['GET', 'POST'])
def tool(tool_id):
    if tool_id not in TOOLS:
        return render_template('error.html', code=404,
                               title='Tool Not Found',
                               message=f'No tool named "{tool_id}".'), 404

    tool_meta = TOOLS[tool_id]
    messages  = {}
    error     = None

    if request.method == 'POST':
        safe_mode = session.get('safe_mode', False)

        try:
            # Read uploaded files
            csv_file    = request.files.get('csv_file')
            search_file = request.files.get('search_file')

            if not csv_file or not csv_file.filename:
                raise ValueError(f'Please upload the {tool_meta["files"][0]["label"]}.')

            file_bytes   = csv_file.read()
            search_bytes = search_file.read() if search_file and search_file.filename else None

            # Call the right generator
            gen = GENERATORS[tool_id]

            if tool_id == 'pickups':
                messages, _ = gen(file_bytes, search_bytes, safe_mode=safe_mode)
            else:
                messages = gen(file_bytes, safe_mode=safe_mode)

            if not messages:
                flash('No messages generated — check the input file has data for known DSPs.', 'warning')
            else:
                flash(f'✅ Generated {len(messages)} DSP message(s).', 'success')

            # Store messages in server-side store for the send endpoint
            store_messages(tool_id, messages)

        except ValueError as e:
            error = str(e)
            flash(f'⚠️ {error}', 'warning')
        except Exception as e:
            logger.error(f'Tool {tool_id} processing error: {e}', exc_info=True)
            error = str(e)
            flash(f'❌ Processing error: {error}', 'danger')

    else:
        # GET — restore any previously generated messages for this tool
        messages = get_stored_messages(tool_id)

    webhooks      = load_webhooks()
    dsp_list      = sorted(messages.keys())
    first_message = messages.get(dsp_list[0], '') if dsp_list else ''

    return render_template(
        'tool.html',
        tool_id=tool_id,
        tool=tool_meta,
        messages=messages,
        dsp_list=dsp_list,
        first_message=first_message,
        webhooks=webhooks,
        error=error,
    )


# ── Send to Slack (AJAX) ──────────────────────────────────────────────────────

@app.route('/send-slack/<tool_id>', methods=['POST'])
def send_slack(tool_id):
    if tool_id not in TOOLS:
        return jsonify({'ok': False, 'error': f'Unknown tool: {tool_id}'}), 400

    body    = request.get_json(silent=True) or {}
    channel = body.get('channel', 'metrics').lower()
    dsps    = body.get('dsps', [])   # list of DSP codes to send to, empty = all

    messages = get_stored_messages(tool_id)
    if not messages:
        return jsonify({'ok': False, 'error': 'No messages found. Generate messages first.'}), 400

    webhooks = get_webhooks_for_channel(channel)
    if not webhooks:
        return jsonify({'ok': False, 'error': f'No {channel} webhooks configured. Check Setup.'}), 400

    # Filter to requested DSPs (or all if not specified)
    targets = [d for d in (dsps if dsps else sorted(messages.keys())) if d in messages]
    if not targets:
        return jsonify({'ok': False, 'error': 'No matching DSPs to send to.'}), 400

    results = []
    success = 0
    for dsp in targets:
        url = webhooks.get(dsp, '')
        if not url:
            results.append({'dsp': dsp, 'ok': False, 'msg': 'No webhook URL configured'})
            continue
        try:
            r = requests.post(url, json={'message': messages[dsp]}, timeout=10)
            ok = r.status_code in (200, 201, 204)
            if ok:
                success += 1
            results.append({
                'dsp': dsp,
                'ok':  ok,
                'msg': f'HTTP {r.status_code}' if not ok else 'Sent',
            })
        except Exception as e:
            results.append({'dsp': dsp, 'ok': False, 'msg': str(e)})

    return jsonify({
        'ok':      True,
        'sent':    success,
        'total':   len(targets),
        'channel': channel,
        'results': results,
    })


# ── Setup — webhook management ────────────────────────────────────────────────

@app.route('/setup', methods=['GET', 'POST'])
def setup():
    if request.method == 'POST':
        action = request.form.get('action', '')

        if action == 'save':
            # Rebuild webhook dict from posted form fields
            new_data = {}
            raw = request.form.to_dict()
            # fields named: dsp_ATAG_metrics, dsp_ATAG_ops
            for key, val in raw.items():
                if key.startswith('dsp_'):
                    parts = key.split('_', 2)   # ['dsp', 'ATAG', 'metrics']
                    if len(parts) == 3:
                        _, dsp, channel = parts
                        if dsp not in new_data:
                            new_data[dsp] = {'metrics': '', 'ops': ''}
                        new_data[dsp][channel] = val.strip()
            ok = save_webhooks(new_data)
            flash('✅ Webhooks saved.' if ok else
                  '⚠️ Saved locally (GitHub write failed — check GITHUB_TOKEN).', 'success' if ok else 'warning')
            return redirect(url_for('setup'))

        elif action == 'add':
            dsp     = request.form.get('new_dsp', '').strip().upper()
            metrics = request.form.get('new_metrics', '').strip()
            ops     = request.form.get('new_ops', '').strip()
            if not dsp:
                flash('DSP code is required.', 'danger')
            else:
                data = load_webhooks()
                data[dsp] = {'metrics': metrics, 'ops': ops}
                save_webhooks(data)
                invalidate_cache()
                flash(f'✅ {dsp} added.', 'success')
            return redirect(url_for('setup'))

        elif action == 'delete':
            dsp  = request.form.get('dsp', '').strip().upper()
            data = load_webhooks()
            if dsp in data:
                del data[dsp]
                save_webhooks(data)
                invalidate_cache()
                flash(f'🗑️ {dsp} removed.', 'warning')
            return redirect(url_for('setup'))

        elif action == 'reset':
            save_webhooks({dsp: dict(urls) for dsp, urls in DEFAULT_WEBHOOKS.items()})
            invalidate_cache()
            flash('🔄 Webhooks reset to defaults.', 'success')
            return redirect(url_for('setup'))

    webhooks = load_webhooks()
    return render_template('setup.html', webhooks=webhooks)


# ── Health check ──────────────────────────────────────────────────────────────

@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'version': VERSION}), 200


# ── Error handlers ────────────────────────────────────────────────────────────

@app.errorhandler(404)
def not_found(e):
    return render_template('error.html', code=404,
                           title='Page Not Found',
                           message="The page you're looking for doesn't exist."), 404

@app.errorhandler(413)
def file_too_large(e):
    return render_template('error.html', code=413,
                           title='File Too Large',
                           message='Upload exceeds the 50MB limit.'), 413

@app.errorhandler(500)
def server_error(e):
    logger.error(f'500: {e}', exc_info=True)
    return render_template('error.html', code=500,
                           title='Something Went Wrong',
                           message='An unexpected error occurred. Try again.'), 500


# ── Request logging ───────────────────────────────────────────────────────────

@app.after_request
def log_req(response):
    if request.path != '/health':
        logger.info(f'{request.method} {request.path} → {response.status_code}')
    return response


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    port  = int(os.environ.get('PORT', 8080))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    logger.info(f'DSP Tools Hub v{VERSION} starting on port {port}')
    app.run(host='0.0.0.0', port=port, debug=debug)
