"""
DSP Tools Hub — Web Application v2.0 (COMPLIANT)
================================================
Compliant with On-Road DSP Collaboration SOP (Week 21)

COMPLIANCE PRINCIPLES:
1. DSP-level data ONLY — no route-level, no DA/TRID-level
2. Informational language ONLY — no action requests
3. Support-driven — data shared for DSP awareness, not OPS direction

Deploy: Docker → Render.com
Repo:   dnr1deliveries-afk/dsp-tools-hub

v2.0 - Framework-compliant rebuild (Week 21)
       - All tools output DSP-level totals only
       - Removed Nursery Overuse and Ridealong Overuse (no compliant path)
       - Removed action request language from all messages
       - Added compliance footer to all messages
"""

import os
import io
import logging
import uuid
from datetime import datetime
import requests
from collections import defaultdict
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, session, jsonify, Response
)
from processing.dsp_core import (
    generate_chase_messages,
    generate_pickup_messages,
    generate_rostering_messages,
    generate_stc_messages,
    generate_cc_messages,
    generate_pod_messages,
    generate_noa_messages,
    generate_bags_messages,
    generate_carrier_inv_messages,
    generate_vsa_messages,
    generate_tracer_bridge_messages,
    # Removed: generate_nursery_overuse_messages (no compliant path)
    # Removed: generate_ridealong_overuse_messages (no compliant path)
)
from storage.station_store import (
    load_station_webhooks, save_station_webhooks,
    load_station_settings, save_station_settings,
    get_webhooks_for_channel, get_payload_key,
    list_stations,
)
from processing.robl_processor import(
    generate_robl_analysis, format_robl_clipboard,
    format_current_week_clipboard, format_next_week_clipboard, format_changes_clipboard,
    format_dsp_breakdown_clipboard, generate_robl_html_report
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

VERSION    = '2.0'
BUILD_DATE = '2026-05-28'

# ── Tool registry — COMPLIANT v2.0 ────────────────────────────────────────────
# All tools now output DSP-level totals only (no route/DA-level data)
# Nursery Overuse and Ridealong Overuse have been REMOVED (no compliant path)

TOOLS = {
    'chase': {
        'name':      'DSP Chase',
        'icon':      'bi-search',
        'emoji':     '🔍',
        'desc':      'Outstanding scrub error shipments per DSP with route codes',
        'files':     [{'id': 'csv_file',    'label': 'Scrub Error CSV',
                       'hint': 'OUTSTANDING SCRUB ERROR*.csv', 'required': True},
                      {'id': 'search_file', 'label': 'SearchResults CSV',
                       'hint': 'SearchResults*.csv — Route Code lookup', 'required': True}],
        'safe_affected': False,
    },
    'pickups': {
        'name':      'DSP Pickups',
        'icon':      'bi-mailbox',
        'emoji':     '📬',
        'desc':      'Awaiting pickup count per DSP (DSP totals only)',
        'files':     [{'id': 'csv_file',    'label': 'Awaiting Pickup CSV',
                       'hint': 'AWAITING PICK UP*.csv',   'required': True}],
        'safe_affected': False,
        'compliant_note': '✅ Compliant: Shows DSP-level totals only, no route lookup',
    },
    'rostering': {
        'name':      'Rostering',
        'icon':      'bi-clipboard-check',
        'emoji':     '📋',
        'desc':      'Rostering compliance percentage per DSP',
        'files':     [{'id': 'csv_file', 'label': 'Rostering Capacity CSV',
                       'hint': 'Rostering_Capacity_C_*.csv', 'required': True}],
        'safe_affected': False,
        'compliant_note': '✅ Compliant: Shows compliance % only, no slot-level detail',
    },
    'stc': {
        'name':      'STC',
        'icon':      'bi-truck',
        'emoji':     '🚚',
        'desc':      'Fleet service type compliance percentage per DSP',
        'files':     [{'id': 'csv_file', 'label': 'STC Deep Dive CSV',
                       'hint': 'Dive Deep Data Service Type Compliance*.csv', 'required': True}],
        'safe_affected': False,
        'compliant_note': '✅ Compliant: Shows fleet % only, no VIN/route detail',
    },
    'cc': {
        'name':      'Contact Compliance',
        'icon':      'bi-telephone-fill',
        'emoji':     '📞',
        'desc':      'Contact compliance summary per DSP (DSP totals only)',
        'files':     [{'id': 'csv_file', 'label': 'CC Exceptions CSV',
                       'hint': 'Exceptions_Based_Dee_*.csv', 'required': True}],
        'safe_affected': False,
        'compliant_note': '✅ Compliant: Shows DSP-level totals only, no DA/delivery detail',
    },
    'pod': {
        'name':      'POD',
        'icon':      'bi-camera-fill',
        'emoji':     '📷',
        'desc':      'POD opportunity count per DSP (DSP totals only)',
        'files':     [{'id': 'csv_file', 'label': 'POD Summary CSV',
                       'hint': 'POD_Summary_*.csv', 'required': True}],
        'safe_affected': False,
        'compliant_note': '✅ Compliant: Shows DSP-level totals only, no DA detail',
    },
    'noa': {
        'name':      'NOA',
        'icon':      'bi-bell-fill',
        'emoji':     '🔔',
        'desc':      'Notification of Arrival event count per DSP',
        'files':     [{'id': 'csv_file', 'label': 'Exceptions CSV',
                       'hint': 'Exceptions_Based_Dee_*.csv', 'required': True}],
        'safe_affected': False,
        'compliant_note': '✅ Compliant: Shows event count only, no action requests',
    },
    'bags': {
        'name':      'Unreturned Bags',
        'icon':      'bi-bag-fill',
        'emoji':     '👜',
        'desc':      'Unreturned bag count per DSP (DSP totals only)',
        'files':     [{'id': 'csv_file', 'label': 'Unreturned Bags CSV',
                       'hint': 'List_of_not_returned_*.csv', 'required': True}],
        'safe_affected': False,
        'compliant_note': '✅ Compliant: Shows DSP-level totals only, no route detail',
    },
    'carrier_inv': {
        'name':      'Carrier Investigations',
        'icon':      'bi-clipboard-data',
        'emoji':     '🕵️',
        'desc':      'DNR investigation count per DSP — week to date',
        'files':     [{'id': 'csv_file', 'label': 'Carrier Investigations CSV',
                       'hint': 'Carrier_Investigatio_*.csv', 'required': True}],
        'safe_affected': False,
        'compliant_note': '✅ Compliant: Shows investigation count only, no package detail',
    },
    'vsa': {
        'name':      'VSA',
        'icon':      'bi-shield-check',
        'emoji':     '🛡️',
        'desc':      'Vehicles pending inspection per DSP, with VIN/VRN detail',
        'files':     [{'id': 'csv_file', 'label': 'VSA Deep Dive CSV',
                       'hint': 'Dive Deep Data Total Expected VSA Audits (Cycle)*.csv', 'required': True}],
        'safe_affected': False,
        'compliant_note': '⚠️ Contains vehicle-level detail (VIN/VRN) — permitted per PoE policy update 2026-07-12',
    },
    'tracer_bridge': {
        'name':      'Tracer Bridge',
        'icon':      'bi-clipboard2-pulse',
        'emoji':     '📊',
        'desc':      'Not recovered package count per DSP (DSP totals only)',
        'files':     [{'id': 'not_recovered_file', 'label': 'Not Recovered Deep Dive CSV',
                       'hint': 'Not_Recovered_Deep_D_*.csv', 'required': True},
                      {'id': 'search_file', 'label': 'SearchResults CSV',
                       'hint': 'SearchResults*.csv — DSP lookup', 'required': True},
                      {'id': 'bulk_history_file', 'label': 'Bulk History Export (optional)',
                       'hint': 'bulk_history_export*.csv — detects returned packages', 'required': False}],
        'safe_affected': False,
        'compliant_note': '✅ Compliant: Shows DSP-level totals only, no tracking IDs',
    },
    'robl': {
        'name':      'ROBL Offsets',
        'icon':      'bi-graph-up-arrow',
        'emoji':     '📊',
        'desc':      'PvA offset analysis — internal use only',
        'files':     [{'id': 'csv_file', 'label': 'ROBL PvA Export',
                       'hint': '[EU]_ROBL_PvA_Offset_*.csv', 'required': True}],
        'safe_affected': False,
        'compliant_note': '🔒 Internal: Station management tool, not shared with DSPs',
        'internal_only': True,
    },

    # ──────────────────────────────────────────────────────────────────────────
    # REMOVED TOOLS (No compliant path under Week 21 framework)
    # ──────────────────────────────────────────────────────────────────────────
    # 'nursery_overuse' — REMOVED: Directs DSP DA deployment decisions
    # 'ridealong_overuse' — REMOVED: Directs DSP staffing decisions
    # ──────────────────────────────────────────────────────────────────────────
}

# Map tool_id → generate function
GENERATORS = {
    'chase':              generate_chase_messages,
    'pickups':            generate_pickup_messages,
    'rostering':          generate_rostering_messages,
    'stc':                generate_stc_messages,
    'cc':                 generate_cc_messages,
    'pod':                generate_pod_messages,
    'noa':                generate_noa_messages,
    'bags':               generate_bags_messages,
    'carrier_inv':        generate_carrier_inv_messages,
    'vsa':                generate_vsa_messages,
    'tracer_bridge':      generate_tracer_bridge_messages,
    # Removed: nursery_overuse, ridealong_overuse
}


# ── Context processor ─────────────────────────────────────────────────────────
@app.context_processor
def inject_globals():
    station = session.get('station_code', '')
    return {
        'version':   VERSION,
        'tools':     TOOLS,
        'safe_mode': session.get('safe_mode', False),
        'station':   station,
    }


# ── Session helpers ───────────────────────────────────────────────────────────
_msg_store = defaultdict(dict)
_returned_store = defaultdict(dict)
_robl_result_store = {}  # sid -> last generate_robl_analysis() result, for HTML export


def get_station() -> str:
    """Get current station code from session."""
    return session.get('station_code', '').upper()


def store_messages(tool_id: str, messages: dict):
    sid = session.get('sid')
    if sid:
        _msg_store[sid][tool_id] = messages


def get_stored_messages(tool_id: str) -> dict:
    sid = session.get('sid')
    if sid and sid in _msg_store:
        return _msg_store[sid].get(tool_id, {})
    return {}


def store_returned(tool_id: str, returned: dict):
    """Store returned package counts for web display."""
    sid = session.get('sid')
    if sid:
        _returned_store[sid][tool_id] = returned


def get_stored_returned(tool_id: str) -> dict:
    """Get returned package counts for web display."""
    sid = session.get('sid')
    if sid and sid in _returned_store:
        return _returned_store[sid].get(tool_id, {})
    return {}


@app.before_request
def ensure_session():
    if 'sid' not in session:
        session['sid'] = str(uuid.uuid4())[:8]
        session.permanent = False


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/robl', methods=['GET', 'POST'])
def robl():
    """ROBL Offset Analysis - internal use only."""
    results = None
    clipboard_text = ''
    clipboard_current = ''
    clipboard_next = ''
    clipboard_changes = ''
    clipboard_breakdown = ''

    if request.method == 'POST':
        csv_file = request.files.get('csv_file')
        if csv_file and csv_file.filename:
            csv_content = csv_file.read().decode('utf-8-sig')
            results = generate_robl_analysis(csv_content)
            if 'error' in results:
                flash(results['error'], 'danger')
            else:
                clipboard_text = format_robl_clipboard(results)
                clipboard_current = format_current_week_clipboard(results)
                clipboard_next = format_next_week_clipboard(results)
                clipboard_changes = format_changes_clipboard(results)
                clipboard_breakdown = format_dsp_breakdown_clipboard(results)
                sid = session.get('sid')
                if sid:
                    _robl_result_store[sid] = results
                s = results['summary']
                flash(f"ROBL Analysis: Current week {s['current_active_count']} DSPs with offsets "
                      f"(max {s['max_offset_current']} min) | W+1 {s['next_active_count']} DSPs "
                      f"(max {s['max_offset_next']} min)", 'success')
        else:
            flash('Please upload a ROBL CSV file', 'danger')

    return render_template(
        'robl.html', results=results,
        clipboard_text=clipboard_text,
        clipboard_current=clipboard_current,
        clipboard_next=clipboard_next,
        clipboard_changes=clipboard_changes,
        clipboard_breakdown=clipboard_breakdown,
    )


@app.route('/robl/export-html')
def robl_export_html():
    """Download the last ROBL analysis as a standalone, styled HTML report."""
    sid = session.get('sid')
    results = _robl_result_store.get(sid) if sid else None
    if not results or 'error' in results:
        flash('Run a ROBL analysis first, then export.', 'warning')
        return redirect(url_for('robl'))

    station = get_station() or ''
    generated_label = datetime.now().strftime('%d %b %Y %H:%M')
    html = generate_robl_html_report(results, station=station, generated_label=generated_label)

    filename = f"ROBL_Offset_Report_{datetime.now().strftime('%Y-%m-%d')}.html"
    return Response(
        html,
        mimetype='text/html',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )



# ── Station selection ─────────────────────────────────────────────────────────
@app.route('/set-station', methods=['POST'])
def set_station():
    """Set station code from welcome modal."""
    raw_station = request.form.get('station_code', '').strip().upper()
    
    if raw_station:
        session['station_code'] = raw_station
        logger.info(f"Station set to: {raw_station}")
        flash(f'✅ Station set to {raw_station}', 'success')
    
    next_page = request.form.get('next', '/')
    if not next_page.startswith('/'):
        next_page = '/'
    return redirect(next_page)


# ── Safe Mode toggle (AJAX) — Note: Safe Mode no longer needed in v2.0 ────────
@app.route('/set-safe-mode', methods=['POST'])
def set_safe_mode():
    """Safe Mode toggle preserved for backwards compatibility.
    
    Note: In v2.0, Safe Mode is no longer necessary as all tools
    output DSP-level totals only (no DA/route identifiable data).
    """
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

    # ROBL has a dedicated route (bespoke CSV schema + processor signature,
    # not present in GENERATORS) - send any generic /tool/robl hits there.
    if tool_id == 'robl':
        return redirect(url_for('robl'))

    station = get_station()
    if not station:
        flash('⚠️ Please select a station first.', 'warning')
        return redirect(url_for('index'))

    tool_meta = TOOLS[tool_id]
    messages  = {}
    returned_packages = {}
    error     = None

    if request.method == 'POST':
        safe_mode = session.get('safe_mode', False)

        try:
            csv_file     = request.files.get('csv_file')
            search_file  = request.files.get('search_file')
            history_file = request.files.get('history_file')
            tracer_file  = request.files.get('tracer_file')

            # Skip generic csv_file validation for tools with custom file handling
            if tool_id not in ('tracer_bridge',):
                if not csv_file or not csv_file.filename:
                    raise ValueError(f'Please upload the {tool_meta["files"][0]["label"]}.')
                file_bytes = csv_file.read()
                search_bytes = search_file.read() if search_file and search_file.filename else None
                history_bytes = history_file.read() if history_file and history_file.filename else None
                tracer_bytes = tracer_file.read() if tracer_file and tracer_file.filename else None

            gen = GENERATORS[tool_id]

            if tool_id == 'pickups':
                messages, _ = gen(file_bytes, search_bytes, safe_mode=safe_mode)
            elif tool_id == 'chase':
                messages, _ = gen(file_bytes, search_bytes=search_bytes, safe_mode=safe_mode)
            elif tool_id == 'tracer_bridge':
                # Tracer Bridge: Not Recovered + SearchResults + optional Bulk History
                not_recovered_file = request.files.get('not_recovered_file')
                search_file_tb = request.files.get('search_file')
                bulk_history_file = request.files.get('bulk_history_file')
                
                if not not_recovered_file or not not_recovered_file.filename:
                    raise ValueError('Please upload the Not Recovered Deep Dive CSV.')
                if not search_file_tb or not search_file_tb.filename:
                    raise ValueError('Please upload the SearchResults CSV for DSP lookup.')
                
                not_recovered_bytes = not_recovered_file.read()
                search_bytes_tb = search_file_tb.read()
                bulk_history_bytes = bulk_history_file.read() if bulk_history_file and bulk_history_file.filename else None
                
                messages = gen(
                    not_recovered_bytes,
                    search_bytes_tb,
                    bulk_history_bytes=bulk_history_bytes,
                    safe_mode=safe_mode
                )
                
                if bulk_history_bytes:
                    flash('🔍 Bulk history analyzed for returned packages.', 'info')
            else:
                messages = gen(file_bytes, safe_mode=safe_mode)

            if not messages:
                flash('No messages generated — check the input file has data for known DSPs.', 'warning')
            else:
                flash(f'✅ Generated {len(messages)} DSP summary message(s).', 'success')

            store_messages(tool_id, messages)

        except ValueError as e:
            error = str(e)
            flash(f'⚠️ {error}', 'warning')
        except Exception as e:
            logger.error(f'Tool {tool_id} processing error: {e}', exc_info=True)
            error = str(e)
            flash(f'❌ Processing error: {error}', 'danger')

    else:
        messages = get_stored_messages(tool_id)
        returned_packages = get_stored_returned(tool_id)

    webhooks      = load_station_webhooks(station)
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
        returned_packages=returned_packages,
    )


# ── Send to Slack (AJAX) ──────────────────────────────────────────────────────

@app.route('/send-slack/<tool_id>', methods=['POST'])
def send_slack(tool_id):
    if tool_id not in TOOLS:
        return jsonify({'ok': False, 'error': f'Unknown tool: {tool_id}'}), 400

    station = get_station()
    if not station:
        return jsonify({'ok': False, 'error': 'No station selected.'}), 400

    body    = request.get_json(silent=True) or {}
    channel = body.get('channel', 'metrics').lower()
    dsps    = body.get('dsps', [])

    messages = get_stored_messages(tool_id)
    if not messages:
        return jsonify({'ok': False, 'error': 'No messages found. Generate messages first.'}), 400

    webhooks = get_webhooks_for_channel(station, channel)
    if not webhooks:
        return jsonify({'ok': False, 'error': f'No {channel} webhooks configured for {station}. Check Setup.'}), 400

    targets = [d for d in (dsps if dsps else sorted(messages.keys())) if d in messages]
    if not targets:
        return jsonify({'ok': False, 'error': 'No matching DSPs to send to.'}), 400

    payload_key = get_payload_key(station)

    results = []
    success = 0
    for dsp in targets:
        url = webhooks.get(dsp, '')
        if not url:
            results.append({'dsp': dsp, 'ok': False, 'msg': 'No webhook URL configured'})
            continue
        try:
            r = requests.post(url, json={payload_key: messages[dsp]}, timeout=10)
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
        'station': station,
        'results': results,
    })


# ── Setup — webhook management ────────────────────────────────────────────────

@app.route('/setup', methods=['GET', 'POST'])
def setup():
    station = get_station()
    if not station:
        flash('⚠️ Please select a station first.', 'warning')
        return redirect(url_for('index'))

    if request.method == 'POST':
        action = request.form.get('action', '')

        if action == 'save':
            new_data = {}
            raw = request.form.to_dict()
            for key, val in raw.items():
                if key.startswith('dsp_'):
                    parts = key.split('_', 2)
                    if len(parts) == 3:
                        _, dsp, channel = parts
                        if dsp not in new_data:
                            new_data[dsp] = {'metrics': '', 'ops': ''}
                        new_data[dsp][channel] = val.strip()
            ok = save_station_webhooks(station, new_data)
            flash(f'✅ Webhooks saved for {station}.' if ok else
                  '⚠️ Saved locally (GitHub write failed).', 'success' if ok else 'warning')
            return redirect(url_for('setup'))

        elif action == 'add':
            dsp     = request.form.get('new_dsp', '').strip().upper()
            metrics = request.form.get('new_metrics', '').strip()
            ops     = request.form.get('new_ops', '').strip()
            if not dsp:
                flash('DSP code is required.', 'danger')
            else:
                data = load_station_webhooks(station)
                data[dsp] = {'metrics': metrics, 'ops': ops}
                save_station_webhooks(station, data)
                flash(f'✅ {dsp} added.', 'success')
            return redirect(url_for('setup'))

        elif action == 'delete':
            dsp  = request.form.get('dsp', '').strip().upper()
            data = load_station_webhooks(station)
            if dsp in data:
                del data[dsp]
                save_station_webhooks(station, data)
                flash(f'🗑️ {dsp} removed.', 'warning')
            return redirect(url_for('setup'))

        elif action == 'clear_all':
            save_station_webhooks(station, {})
            flash(f'🗑️ All webhooks cleared for {station}.', 'warning')
            return redirect(url_for('setup'))

        elif action == 'save_settings':
            payload_key = request.form.get('payload_key', 'message').strip()
            if not payload_key:
                payload_key = 'message'
            settings = load_station_settings(station)
            settings['payload_key'] = payload_key
            ok = save_station_settings(station, settings)
            flash('✅ Settings saved.' if ok else
                  '⚠️ Saved locally (GitHub write failed).', 'success' if ok else 'warning')
            return redirect(url_for('setup'))

    webhooks = load_station_webhooks(station)
    settings = load_station_settings(station)
    return render_template('setup.html', webhooks=webhooks, settings=settings)


# ── Compliance info page ──────────────────────────────────────────────────────

@app.route('/compliance')
def compliance():
    """Display compliance information about v2.0 changes."""
    return render_template('compliance.html')


# ── Health check ──────────────────────────────────────────────────────────────

@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'version': VERSION, 'compliant': True}), 200


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
    logger.info(f'DSP Tools Hub v{VERSION} (COMPLIANT) starting on port {port}')
    app.run(host='0.0.0.0', port=port, debug=debug)
