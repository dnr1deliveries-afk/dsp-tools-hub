"""
DSP Tools Hub — Processing Engine
Ported from DSP_Tools_Hub.py (desktop tkinter app) v1.6
All generate_* functions accept safe_mode=False (default).

Message format: pipe-separated columns, each padded to longest value + 5 spaces.
Pipes won't be pixel-perfect in Slack's proportional font but give clear visual
separation between fields.
"""

import csv
import io
import hashlib
import re
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


def mask_id(raw_id: str) -> str:
    """
    Deterministic 4-char hex token. Same input = same token within a run.
    Example: 'AB12CDE' -> 'DA-4F2A'
    """
    if not raw_id:
        return 'DA-????'
    h = hashlib.md5(raw_id.encode()).hexdigest()[:4].upper()
    return f'DA-{h}'


def _open_csv(file_bytes: bytes):
    """Return a csv.DictReader from raw bytes, handling UTF-8 BOM."""
    text = file_bytes.decode('utf-8-sig')
    return csv.DictReader(io.StringIO(text))


def pad_cols(headers: list, rows: list, extra: int = 5) -> str:
    """
    Render a list of rows as pipe-separated text with padded columns.

    Each column width = max(len of all values in that column, len of header) + extra.
    Padding is spaces so pipes land at consistent positions across rows.
    Not pixel-perfect in Slack's proportional font but visually clear.

    headers : list of column header strings
    rows    : list of lists, same length as headers
    extra   : spaces added beyond the longest value (default 5)

    Returns a single string — header row + separator + data rows.
    """
    all_rows = [headers] + rows

    # Column widths: max cell length + extra padding
    col_widths = [
        max(len(str(all_rows[r][c])) for r in range(len(all_rows))) + extra
        for c in range(len(headers))
    ]

    def fmt_row(row):
        return '| ' + '| '.join(
            str(cell).ljust(col_widths[i])
            for i, cell in enumerate(row)
        )

    # Separator uses dashes to width of each column
    separator = '| ' + '| '.join('-' * col_widths[i] for i in range(len(headers)))

    lines = [fmt_row(headers), separator]
    lines += [fmt_row(r) for r in rows]
    return '\n'.join(lines)


# ============================================================================
# DSP CHASE
# ============================================================================

def generate_chase_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Input: OUTSTANDING SCRUB ERROR*.csv
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
            lines = [f'To Chase ({len(chase)}):']
            lines += [f'  {t}' for t in chase]
            sections.append('\n'.join(lines))

        if missing:
            lines = [f'Driver Marked Missing ({len(missing)}):']
            lines += [f'  {t}' for t in missing]
            sections.append('\n'.join(lines))

        if not sections:
            continue

        messages[dsp] = (
            f'Outstanding Shipments — {dsp}\n'
            f'Updated: {today}\n\n'
            f'Good morning. Please see below for any shipments not yet returned to station.\n\n'
            + '\n\n'.join(sections) +
            '\n\nAction Required:\n'
            '1. Contact the driver\n'
            '2. Confirm the return\n'
            '3. Update this thread\n\n'
            'Appreciate your support.'
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
    return f'{base} — {route_code}' if route_code else base


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
        headers   = ['Tracking ID', 'Related Delivery', 'Pickup Type']
        data_rows = [
            [p['tracking_id'], p['related_delivery'], p['pickup_type']]
            for p in pickups
        ]
        messages[dsp] = (
            f'{dsp} Pickups — {pickup_date}\n'
            f'{len(pickups)} awaiting pickup\n\n'
            + pad_cols(headers, data_rows)
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
        headers   = ['Service Type', 'Compliance', 'Routes @15:30', 'Routes @Seq', 'D-1 vs D0']
        data_rows = []

        for r in dsp_rows[dsp]:
            svc = str(r.get('Service Type', '') or '').strip()
            try:    comp_raw = float(r.get('Rostering Capacity Compliance %', 0) or 0)
            except: comp_raw = 0.0
            flag  = ' [!]' if comp_raw < 0.9 else ''
            comp  = f'{comp_raw:.0%}{flag}'
            try:    r1530 = str(int(float(r.get('Rostered routes before 15:30', '') or 0)))
            except: r1530 = '-'
            try:    r_seq = str(int(float(r.get('Rostered routes before Sequencing', '') or 0)))
            except: r_seq = '-'
            try:    d1d0  = str(int(float(r.get('D-1 15:30 Plan vs D0 requested', '') or 0)))
            except: d1d0  = '-'
            data_rows.append([svc, comp, r1530, r_seq, d1d0])

        messages[dsp] = (
            f'Rostering Accuracy — {dsp} — {first_date}\n\n'
            f'Hi team, please find your rostering accuracy below. '
            f'Provide root cause for any service type below 90%. [!] = below 90%\n\n'
            + pad_cols(headers, data_rows)
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
    SWAP_LABELS = {
        'plan:small execute:large': 'UPGRADED',
        'plan:large execute:small': 'DOWNGRADED',
        'plan equal execute':       'TIER MISMATCH',
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
        dsp_rows[dsp].append({
            'date':  fmt_date(row.get('date', '')),
            'vin':   mask_id(raw_vin) if safe_mode else raw_vin,
            'd1':    str(row.get('day_1_planned_service_type_and_route_service_type', '') or '').strip(),
            'd0':    str(row.get('day0_actual_executed_vehicle_service_type', '') or '').strip(),
            'route': str(row.get('route_id', '') or '').strip(),
            'swap':  SWAP_LABELS.get(str(row.get('not_compliant_type', '') or '').strip(), 'UNKNOWN'),
        })

    if not dsp_rows:
        raise ValueError("No non-compliant rows found. Check the file contains a 'compliant' column.")

    messages = {}
    for dsp in sorted(dsp_rows.keys()):
        headers   = ['Route', 'VIN', 'D-1 Planned', 'D-0 Actual', 'Change']
        data_rows = [
            [r['route'], r['vin'], r['d1'], r['d0'], r['swap']]
            for r in dsp_rows[dsp]
        ]
        messages[dsp] = (
            f'Service Type Compliance — {dsp} — {first_date}\n\n'
            f'Hi team, please find below the D-0 vehicle swaps vs D-1 plan. '
            f'Please provide insight on why these vehicles were swapped.\n\n'
            + pad_cols(headers, data_rows)
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
        headers   = ['Tracking ID', 'Driver', 'Reason', 'Call', 'Text']
        data_rows = []

        for r in dsp_rows[dsp]:
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
            call_str = f'{call_ev} ({call_dur})' if call_ev != '-' else f'- ({call_dur})'
            data_rows.append([scan_id, trans_id, reason, call_str, text_ev])

        messages[dsp] = (
            f'Contact Compliance — {dsp} — {first_date}\n'
            f'{len(data_rows)} exception(s)\n\n'
            + pad_cols(headers, data_rows)
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
        headers   = ['Tracking ID', 'DA', 'Shipment Reason', 'Reject Reason']
        data_rows = []

        for r in dsp_rows[dsp]:
            tid       = str(r.get('tracking_id', '') or '').strip()
            raw_da    = str(r.get('DA ID', '') or '').strip()
            da_id     = mask_id(raw_da) if safe_mode else raw_da
            ship_rsn  = str(r.get('shipment_reason', '') or '').strip()
            audit_rsn = str(r.get('audit_state_reason', '') or '').strip() or '-'
            data_rows.append([tid, da_id, ship_rsn, audit_rsn])

        messages[dsp] = (
            f'POD Opportunities — {dsp} — {first_date}\n'
            f'{len(data_rows)} reject(s)\n\n'
            + pad_cols(headers, data_rows)
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
        scan_id  = str(r.get('Scannable ID', '') or '').strip()
        if not dsp or not trans_id or not scan_id:
            continue
        if not first_date:
            first_date = fmt_date(row.get('Event Date', ''))
        dsp_data[dsp][trans_id] += 1

    if not dsp_data:
        raise ValueError("No NOTIFY_OF_ARRIVAL rows found. Check the correct Exceptions CSV is selected.")

    intro = (
        'Hi all — Notify of Arrival has a positive impact on OTR safety, Concessions, '
        'CC, DCR and overall customer experience. '
        'Below are all drivers who utilised NOA on the date shown.'
    )

    messages = {}
    for dsp in sorted(dsp_data.keys()):
        counts         = dsp_data[dsp]
        sorted_entries = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        grand_total    = sum(counts.values())

        headers   = ['Driver', 'NOA Count']
        data_rows = []
        for tid, count in sorted_entries:
            display = mask_id(tid) if safe_mode else tid
            data_rows.append([display, str(count)])
        data_rows.append(['Total', str(grand_total)])

        messages[dsp] = (
            f'Notify of Arrival — {dsp} — {first_date}\n\n'
            f'{intro}\n\n'
            + pad_cols(headers, data_rows)
        )

    return messages


# ============================================================================
# PROCESSING - UNRETURNED BAGS
# ============================================================================

def generate_bags_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Input: List_of_not_returned_*.csv (as bytes)

    Columns used:
        DSP, Route Code, Date, Bag, Transporter_id, Unrecovered

    Groups by DSP -> Date -> Route Code, counts bags per route per date.
    Flags routes with 3+ bags as high priority.
    Date range = min to max date across all rows for that DSP.
    One message per DSP.
    """
    FLAG_THRESHOLD = 3  # routes with this many or more bags get flagged

    # dsp_data[dsp][date_str][route] = list of bag IDs
    dsp_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for row in _open_csv(file_bytes):
        dsp      = str(row.get('DSP', '') or '').strip()
        route    = str(row.get('Route Code', '') or '').strip()
        bag      = str(row.get('Bag', '') or '').strip()
        date_raw = str(row.get('Date', '') or '').strip()

        if not dsp or not route or not bag:
            continue

        # Parse date to DD/MM/YYYY
        date_str = fmt_date(date_raw)
        dsp_data[dsp][date_str][route].append(bag)

    if not dsp_data:
        raise ValueError(
            'No bag data found in CSV.\n'
            'Check the file contains DSP, Route Code, Bag and Date columns.'
        )

    messages = {}
    for dsp in sorted(dsp_data.keys()):
        dates_dict = dsp_data[dsp]
        all_dates  = sorted(
            dates_dict.keys(),
            key=lambda d: datetime.strptime(d, '%d/%m/%Y') if d else datetime.min
        )
        date_from = all_dates[0]  if all_dates else ''
        date_to   = all_dates[-1] if all_dates else ''

        # Only count routes with 2+ bags (singles are excluded from output)
        total_bags = sum(
            len(bags)
            for date_routes in dates_dict.values()
            for bags in date_routes.values()
            if len(bags) >= 2
        )
        flagged_count = sum(
            1
            for date_routes in dates_dict.values()
            for bags in date_routes.values()
            if len(bags) >= FLAG_THRESHOLD
        )

        # Build per-date sections
        sections = []
        for date_str in all_dates:
            routes_dict   = dates_dict[date_str]
            sorted_routes = sorted(routes_dict.keys())

            headers   = ['Route Code', 'Missing Bags']
            data_rows = []
            for route in sorted_routes:
                count = len(routes_dict[route])
                if count < 2:
                    continue  # exclude single-bag routes
                flag = ' [!]' if count >= FLAG_THRESHOLD else ''
                data_rows.append([route, f'{count}{flag}'])

            if not data_rows:
                continue  # skip date entirely if all routes were singles

            sections.append(
                f'{date_str}\n'
                + pad_cols(headers, data_rows)
            )

        flag_line = (
            f'{flagged_count} route(s) with {FLAG_THRESHOLD}+ bags flagged [!]\n'
            if flagged_count else ''
        )

        messages[dsp] = (
            f'Unreturned Bags — {dsp} — {date_from} to {date_to}\n'
            f'Total unreturned: {total_bags} bag(s)\n'
            f'{flag_line}'
            f'\n'
            + '\n\n'.join(sections)
        )

    return messages
