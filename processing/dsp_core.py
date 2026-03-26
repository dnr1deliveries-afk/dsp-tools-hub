"""
DSP Tools Hub — Processing Engine
Ported from DSP_Tools_Hub.py (desktop tkinter app) v1.6
All generate_* functions accept safe_mode=False (default).
"""

import csv
import io
import hashlib
import re
import unicodedata
from collections import defaultdict
from datetime import datetime


# ============================================================================
# HELPERS
# ============================================================================

def fmt_date(val) -> str:
    if val is None:
        return ''
    if isinstance(val, datetime):
        return val.strftime('%d/%m/%Y')
    s = str(val)
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y'):
        try:
            return datetime.strptime(s.strip(), fmt).strftime('%d/%m/%Y')
        except ValueError:
            pass
    return s.split(' ')[0]


def fmt_pct(val) -> str:
    if val is None or val == '' or val == 0:
        return '0%'
    try:
        return f'{float(val):.0%}'
    except (ValueError, TypeError):
        return str(val)


def is_zero_row(row_dict: dict) -> bool:
    return all(v in (0, 0.0, '0', None, '') for v in row_dict.values())


def _display_width(text: str) -> int:
    """
    Return the rendered column width of a string in a monospace font.
    Emojis and wide Unicode characters occupy 2 columns; everything else 1.
    Uses unicodedata.east_asian_width — 'W' (Wide) and 'F' (Fullwidth) = 2 cols.
    Variation selectors (U+FE0F etc.) are zero-width — skip them.
    """
    width = 0
    for ch in str(text):
        cp = ord(ch)
        # Skip variation selectors and zero-width joiners
        if 0xFE00 <= cp <= 0xFE0F or cp in (0x200D, 0x20E3):
            continue
        eaw = unicodedata.east_asian_width(ch)
        if eaw in ('W', 'F'):
            width += 2
        else:
            # Emoji outside the EAW 'W' bucket (most modern emoji: U+1F300+)
            # Check Unicode general category — 'So' = Symbol, Other (most emoji)
            if unicodedata.category(ch) in ('So', 'Sm') and cp > 0x2000:
                width += 2
            else:
                width += 1
    return width


def format_table(headers: list, rows: list) -> str:
    """
    Build a padded pipe table aligned for Slack monospace rendering.
    Uses display_width() so emoji and wide chars don't break column alignment.
    Each column is as wide as the widest cell (by rendered width, not byte length).
    """
    all_rows = [headers] + rows

    # Column widths based on rendered display width, not len()
    col_widths = [
        max(_display_width(str(all_rows[r][c])) for r in range(len(all_rows)))
        for c in range(len(headers))
    ]

    def pad_cell(cell, width):
        s      = str(cell)
        filled = _display_width(s)
        # Pad with spaces to reach the target column width
        return s + ' ' * max(0, width - filled)

    def pad_row(row):
        return '| ' + ' | '.join(pad_cell(cell, col_widths[i])
                                  for i, cell in enumerate(row)) + ' |'

    separator = '| ' + ' | '.join('-' * w for w in col_widths) + ' |'
    lines     = [pad_row(headers), separator] + [pad_row(r) for r in rows]
    return '\n'.join(lines)

def mask_id(raw_id: str) -> str:
    """
    Deterministic 4-char hex token derived from the raw ID.
    Same input always produces same token within a run — consistent but not reversible.
    Example: 'AB12CDE' → 'DA-4F2A'
    """
    if not raw_id:
        return 'DA-????'
    h = hashlib.md5(raw_id.encode()).hexdigest()[:4].upper()
    return f'DA-{h}'


def _open_csv(file_bytes: bytes):
    """Return a csv.DictReader from raw bytes, handling UTF-8 BOM."""
    text = file_bytes.decode('utf-8-sig')
    return csv.DictReader(io.StringIO(text))


# ============================================================================
# DSP CHASE
# ============================================================================

def generate_chase_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Input: OUTSTANDING SCRUB ERROR*.csv
    Columns: DSP Name, trackingId, Attempt Reason Code
    Safe mode: no change (no driver IDs in this report).
    """
    dsp_data = defaultdict(lambda: {'chase': set(), 'missing': set()})

    for row in _open_csv(file_bytes):
        dsp    = row.get('DSP Name', '').strip()
        tid    = row.get('trackingId', '').strip()
        reason = row.get('Attempt Reason Code', '').strip()
        if dsp and tid:
            if reason == 'ITEMS_MISSING':
                dsp_data[dsp]['missing'].add(tid)
            else:
                dsp_data[dsp]['chase'].add(tid)

    today    = datetime.now().strftime('%d/%m/%Y')
    messages = {}
    for dsp in sorted(dsp_data.keys()):
        chase   = sorted(dsp_data[dsp]['chase'])
        missing = sorted(dsp_data[dsp]['missing'])
        sections = []
        if chase:
            bullets = '\n'.join(f'• {t}' for t in chase)
            sections.append(f'📦 To Chase ({len(chase)})\n{bullets}')
        if missing:
            bullets = '\n'.join(f'• {t}' for t in missing)
            sections.append(f'🔴 Driver Marked Missing ({len(missing)})\n{bullets}')
        if not sections:
            continue
        messages[dsp] = (
            f'📦 Outstanding Shipments - {dsp}\n'
            f'Last updated: {today}\n\n'
            f'Good morning. Please see below for any shipments that have not been returned to the station.\n\n'
            + '\n\n'.join(sections) +
            f'\n\n⚡ Action Required\n'
            f'1. Contact the driver\n2. Confirm the return\n3. Update this thread\n\n'
            f'Appreciate your support.'
        )
    return messages


# ============================================================================
# DSP PICKUPS
# ============================================================================

def _load_route_lookup(search_bytes: bytes) -> dict:
    routes = {}
    if not search_bytes:
        return routes
    for row in _open_csv(search_bytes):
        tid   = row.get('Tracking ID', '').strip()
        route = row.get('Route Code', '').strip()
        if tid and route:
            routes[tid] = route
    return routes


def _extract_route_number(route_code: str) -> int:
    if not route_code:
        return 9999
    numbers = re.findall(r'\d+', route_code)
    return int(numbers[-1]) if numbers else 9999


def _format_pickup_type(pickup_type: str, route_code: str = '') -> str:
    t = pickup_type.upper().strip()
    if t == 'LOCKER':     base = 'Locker'
    elif t == 'NOREASON': base = 'Counter'
    elif not t:           base = 'Home'
    else:                 base = pickup_type.title()
    return f'{base} - {route_code}' if route_code else base


def generate_pickup_messages(pickup_bytes: bytes, search_bytes: bytes = None,
                              safe_mode: bool = False) -> tuple:
    """
    Input: AWAITING PICK UP*.csv + optional SearchResults*.csv
    Safe mode: no change (no driver IDs in this report).
    Returns: (messages dict, pickup_date string)
    """
    route_lookup = _load_route_lookup(search_bytes) if search_bytes else {}
    dsp_pickups  = defaultdict(list)
    pickup_date  = None

    rows = list(_open_csv(pickup_bytes))
    # Pre-process: fill empty Pick up Type
    for row in rows:
        ptype  = row.get('Pick up Type', '').strip()
        window = row.get('Pick up Start Window', '').strip()
        if not ptype:
            row['Pick up Type'] = '' if '11:00' in window else 'NOREASON'

    for row in rows:
        dsp        = row.get('Dsp', '').strip()
        tid        = row.get('trackingId', '').strip()
        related    = row.get('Related Delivery', '').strip()
        ptype_raw  = row.get('Pick up Type', '').strip()
        route_code = route_lookup.get(related, '')
        ptype      = _format_pickup_type(ptype_raw, route_code)

        if not pickup_date:
            sw = row.get('Pick up Start Window', '')
            pickup_date = sw.split(' ')[0] if sw else datetime.now().strftime('%d/%m/%Y')

        if dsp and tid:
            dsp_pickups[dsp].append({
                'tracking_id':      tid,
                'related_delivery': related or tid,
                'pickup_type':      ptype,
                'route_number':     _extract_route_number(route_code),
            })

    if not pickup_date:
        pickup_date = datetime.now().strftime('%d/%m/%Y')

    messages = {}
    for dsp in sorted(dsp_pickups.keys()):
        pickups   = sorted(dsp_pickups[dsp], key=lambda x: x['route_number'])
        data_rows = [[p['tracking_id'], p['related_delivery'], p['pickup_type']] for p in pickups]
        headers   = ['Tracking ID', 'Related Delivery', 'Pickup Type']
        messages[dsp] = (
            f'## {dsp} Pickups for {pickup_date}\n\n'
            f'{format_table(headers, data_rows)}'
        )

    return messages, pickup_date


# ============================================================================
# ROSTERING ACCURACY
# ============================================================================

def generate_rostering_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Input: Rostering_Capacity_C_*.csv
    Safe mode: no change (no driver IDs in this report).
    """
    dsp_rows   = defaultdict(list)
    first_date = ''

    for row in _open_csv(file_bytes):
        dsp = str(row.get('DSP', '') or '').strip()
        if not dsp:
            continue
        if not first_date:
            first_date = fmt_date(row.get('startdate_local', ''))
        dsp_rows[dsp].append(row)

    if not dsp_rows:
        raise ValueError("No data found. Check the file contains a 'DSP' column.")

    messages = {}
    for dsp in sorted(dsp_rows.keys()):
        data_rows = []
        for r in dsp_rows[dsp]:
            svc      = str(r.get('Service Type', '') or '').strip()
            try:    comp_raw = float(r.get('Rostering Capacity Compliance %', 0) or 0)
            except: comp_raw = 0.0
            flag     = ' [!]' if comp_raw < 0.9 else ''   # text flag — no emoji in table cells
            comp_str = f'{comp_raw:.0%}{flag}'
            r1530    = str(r.get('Rostered routes before 15:30', '') or '').strip()
            r_seq    = str(r.get('Rostered routes before Sequencing', '') or '').strip()
            d1d0     = str(r.get('D-1 15:30 Plan vs D0 requested', '') or '').strip()
            for val in [r1530, r_seq, d1d0]:
                try: val = str(int(float(val)))
                except: pass
            try:    r1530 = str(int(float(r1530)))
            except: pass
            try:    r_seq = str(int(float(r_seq)))
            except: pass
            try:    d1d0  = str(int(float(d1d0)))
            except: pass
            data_rows.append([svc, comp_str, r1530, r_seq, d1d0])

        headers   = ['Service Type', 'Compliance %', 'Routes Before 15:30', 'Routes Before Seq', 'D-1 vs D0']
        messages[dsp] = (
            f'## Daily Deep Dive - Rostering Accuracy - {first_date}\n\n'
            f'Hi team, kindly find below your rostering accuracy, split by each service type. '
            f'Could you please provide us with insight and root cause for all service types '
            f'that have <90% accuracy? [!] = below 90%\n\n'
            f'{format_table(headers, data_rows)}'
        )

    return messages


# ============================================================================
# SERVICE TYPE COMPLIANCE (STC)
# ============================================================================

def generate_stc_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Input: Dive Deep Data Service Type Compliance*.csv
    Safe mode: VIN replaced with deterministic DA-XXXX token.
    """
    # Text labels instead of emoji — keeps table column alignment clean in Slack
    SWAP_ICONS = {
        'plan:small execute:large': 'UP   ',
        'plan:large execute:small': 'DOWN ',
        'plan equal execute':       'SWAP ',
    }
    dsp_rows   = defaultdict(list)
    first_date = ''

    for row in _open_csv(file_bytes):
        if str(row.get('compliant', '1')).strip() != '0':
            continue
        dsp = str(row.get('dsp', '') or '').strip()
        if not dsp:
            continue
        if not first_date:
            first_date = fmt_date(row.get('date', ''))

        raw_vin = str(row.get('vin', '') or '').strip()
        vin     = mask_id(raw_vin) if safe_mode else raw_vin

        dsp_rows[dsp].append({
            'date':  fmt_date(row.get('date', '')),
            'vin':   vin,
            'd1':    str(row.get('day_1_planned_service_type_and_route_service_type', '') or '').strip(),
            'd0':    str(row.get('day0_actual_executed_vehicle_service_type', '') or '').strip(),
            'route': str(row.get('route_id', '') or '').strip(),
            'swap':  SWAP_ICONS.get(str(row.get('not_compliant_type', '') or '').strip(), '—'),
        })

    if not dsp_rows:
        raise ValueError("No non-compliant rows found. Check the file contains a 'compliant' column.")

    messages = {}
    for dsp in sorted(dsp_rows.keys()):
        data_rows = [[r['date'], r['vin'], r['d1'], r['d0'], r['route'], r['swap']]
                     for r in dsp_rows[dsp]]
        headers   = ['Date', 'VIN', 'D-1 Planned', 'D-0 Actual', 'Route', 'Swap']
        messages[dsp] = (
            f'## Daily Deep Dive - Service Type Compliance - {first_date}\n\n'
            f'Hi team, kindly find below the service type swaps conducted on D-0 vs D-1 plan. '
            f'Could you please provide us with some insight as to why the vehicles have been '
            f'swapped for the below routes? Thank you\n\n'
            f'UP = Upgraded to larger  |  DOWN = Downgraded to smaller  |  SWAP = Tier mismatch\n\n'
            f'{format_table(headers, data_rows)}'
        )

    return messages


# ============================================================================
# CONTACT COMPLIANCE (CC)
# ============================================================================

def generate_cc_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Input: Exceptions_Based_Dee_*.csv (excludes NOTIFY_OF_ARRIVAL rows)
    Safe mode: Transporter ID replaced with deterministic DA-XXXX token.
    """
    dsp_rows   = defaultdict(list)
    first_date = ''

    for row in _open_csv(file_bytes):
        if str(row.get('chat_reason_code', '') or '').strip() == 'NOTIFY_OF_ARRIVAL':
            continue
        dsp = str(row.get('DSP', '') or '').strip()
        if not dsp:
            continue
        if not first_date:
            first_date = fmt_date(row.get('Event Date', ''))
        dsp_rows[dsp].append(row)

    if not dsp_rows:
        raise ValueError("No CC rows found. Check the correct Exceptions CSV is selected.")

    messages = {}
    for dsp in sorted(dsp_rows.keys()):
        data_rows = []
        for r in dsp_rows[dsp]:
            date     = fmt_date(r.get('Event Date', ''))
            scan_id  = str(r.get('Scannable ID', '') or '').strip()
            raw_tid  = str(r.get('Transporter ID', '') or '').strip()
            trans_id = mask_id(raw_tid) if safe_mode else raw_tid
            reason   = str(r.get('Shipment Reason', '') or '').strip()
            call_ev  = str(r.get('Call Event', '') or '').strip() or '-'
            text_ev  = str(r.get('Text Event', '') or '').strip() or '-'
            try:
                dur_raw  = float(r.get('Total Call Duration (sec)', 0) or 0)
                call_dur = f'{int(dur_raw)}s' if dur_raw > 0 else '0s'
            except (ValueError, TypeError):
                call_dur = '0s'
            data_rows.append([date, scan_id, trans_id, reason, call_ev, call_dur, text_ev])

        headers   = ['Date', 'Tracking ID', 'Transporter ID', 'Shipment Reason', 'Call Event', 'Duration', 'Text Event']
        messages[dsp] = (
            f'## Daily Deep Dive - Contact Compliance - {first_date}\n\n'
            f'{format_table(headers, data_rows)}'
        )

    return messages


# ============================================================================
# PICTURE ON DELIVERY (POD)
# ============================================================================

def generate_pod_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Input: POD_Summary_*.csv (excludes is_bypassed=Y rows)
    Safe mode: DA ID replaced with deterministic DA-XXXX token.
    """
    dsp_rows   = defaultdict(list)
    first_date = ''

    for row in _open_csv(file_bytes):
        if str(row.get('is_bypassed', '') or '').strip().upper() == 'Y':
            continue
        dsp = str(row.get('DSP', '') or '').strip()
        if not dsp:
            continue
        if not first_date:
            first_date = fmt_date(row.get('event_date', ''))
        dsp_rows[dsp].append(row)

    if not dsp_rows:
        raise ValueError("No POD reject rows found. Check the correct POD Summary CSV is selected.")

    messages = {}
    for dsp in sorted(dsp_rows.keys()):
        data_rows = []
        for r in dsp_rows[dsp]:
            tid       = str(r.get('tracking_id', '') or '').strip()
            raw_da    = str(r.get('DA ID', '') or '').strip()
            da_id     = mask_id(raw_da) if safe_mode else raw_da
            ship_rsn  = str(r.get('shipment_reason', '') or '').strip()
            audit_rsn = str(r.get('audit_state_reason', '') or '').strip() or '-'
            data_rows.append([tid, da_id, ship_rsn, audit_rsn])

        headers   = ['Tracking ID', 'DA ID', 'Shipment Reason', 'Reject Reason']
        messages[dsp] = (
            f'## Daily Deep Dive - POD Opportunities - {first_date}\n\n'
            f'{format_table(headers, data_rows)}'
        )

    return messages


# ============================================================================
# NOTIFY OF ARRIVAL (NOA)
# ============================================================================

def generate_noa_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Input: Exceptions_Based_Dee_*.csv (only NOTIFY_OF_ARRIVAL rows)
    Safe mode: Transporter ID replaced with deterministic DA-XXXX token.
    """
    first_date = ''
    dsp_data   = defaultdict(lambda: defaultdict(int))

    for row in _open_csv(file_bytes):
        if str(row.get('chat_reason_code', '') or '').strip() != 'NOTIFY_OF_ARRIVAL':
            continue
        dsp      = str(row.get('DSP', '') or '').strip()
        trans_id = str(row.get('Transporter ID', '') or '').strip()
        scan_id  = str(row.get('Scannable ID', '') or '').strip()
        if not dsp or not trans_id or not scan_id:
            continue
        if not first_date:
            first_date = fmt_date(row.get('Event Date', ''))
        dsp_data[dsp][trans_id] += 1

    if not dsp_data:
        raise ValueError("No NOTIFY_OF_ARRIVAL rows found. Check the correct Exceptions CSV is selected.")

    intro = (
        'Hi all, as mentioned in the roundtables, Notify of Arrival has proven to have a '
        'positive impact on OTR safety, Concessions, CC, DCR and overall customer experience. '
        'Kindly find below all the drivers that have utilised Notify of Arrival on the date '
        'mentioned in the title.'
    )

    messages = {}
    for dsp in sorted(dsp_data.keys()):
        counts        = dsp_data[dsp]
        sorted_entries = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        grand_total   = sum(counts.values())

        data_rows = []
        for tid, count in sorted_entries:
            display = mask_id(tid) if safe_mode else tid
            data_rows.append([display, str(count)])
        data_rows.append(['Grand Total', str(grand_total)])

        headers   = ['DA ID', 'NOA Count']
        messages[dsp] = (
            f'## Notify of Arrival - {first_date}\n\n'
            f'{intro}\n\n'
            f'{format_table(headers, data_rows)}'
        )

    return messages
