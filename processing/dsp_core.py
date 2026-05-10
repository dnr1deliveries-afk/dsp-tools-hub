"""
DSP Tools Hub — Processing Engine v1.9
Ported from DSP_Tools_Hub.py (desktop tkinter app) v1.6
All generate_* functions accept safe_mode=False (default).

Message format: pipe-separated columns, each padded to longest value + 5 spaces.
Pipes won't be pixel-perfect in Slack's proportional font but give clear visual
separation between fields.

v1.5 - Multi-day support for Rostering and STC tools
v1.6 - Chase tool: optional bulk history for mistaken return detection
       Returns (messages, returned_by_dsp) - returned shown on web only, not in Slack
v1.7 - Chase tool: expanded mistaken return detection
       Condition 1: WRONG_CYCLE_INDUCT on D-1 after 13:00
       Condition 2: INDUCTED + PACKAGE_STATE_UPDATE from 23:30 D-1 to present
v1.8 - Chase tool: Route Code lookup from Tracer file (with Bulk History fallback)
v1.9 - Chase tool: Grouped by Reason Code with Root Cause input fields
v2.0 - NOA tool: Simplified to DSP-level counts (Transporter ID removed from source)
"""
import csv
import io
import re
from collections import defaultdict
from datetime import datetime, timedelta
import io
import re
from collections import defaultdict
from datetime import datetime, timedelta


# ============================================================================
# HELPERS
# ============================================================================

DIVIDER = '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'


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


def parse_date(date_str: str) -> datetime:
    if not date_str:
        return datetime.min
    try:
        return datetime.strptime(date_str, '%d/%m/%Y')
    except ValueError:
        return datetime.min


def format_date_range(dates: list) -> str:
    if not dates:
        return datetime.now().strftime('%d/%m/%Y')
    sorted_dates = sorted(dates, key=parse_date)
    if len(sorted_dates) == 1:
        return sorted_dates[0]
    return f"{sorted_dates[0]} to {sorted_dates[-1]}"


def fmt_pct(val) -> str:
    if val is None or val == '' or val == 0:
        return '0%'
    try:
        return f'{float(val):.0%}'
    except (ValueError, TypeError):
        return str(val)


def mask_id(raw_id: str) -> str:
    if not raw_id:
        return 'DA-????'
    suffix = raw_id.strip()[-4:].upper()
    return f'DA-{suffix}'


def _open_csv(file_bytes: bytes):
    text = file_bytes.decode('utf-8-sig')
    return csv.DictReader(io.StringIO(text))


def pad_cols(headers: list, rows: list, extra: int = 5) -> str:
    all_rows = [headers] + rows
    col_widths = [
        max(len(str(all_rows[r][c])) for r in range(len(all_rows))) + extra
        for c in range(len(headers))
    ]

    def fmt_row(row):
        return '| ' + '| '.join(
            str(cell).ljust(col_widths[i])
            for i, cell in enumerate(row)
        )

    separator = '| ' + '| '.join('-' * col_widths[i] for i in range(len(headers)))
    lines = [fmt_row(headers), separator]
    lines += [fmt_row(r) for r in rows]
    return '\n'.join(lines)


def wrap_message(content: str) -> str:
    return f'{DIVIDER}\n{content}\n{DIVIDER}'


# ============================================================================
# DSP CHASE - BULK HISTORY HELPERS (v1.7)
# ============================================================================

def parse_bulk_history(history_bytes: bytes) -> dict:
    history = defaultdict(list)
    for row in _open_csv(history_bytes):
        tid = row.get('Tracking ID', '').strip()
        if tid:
            history[tid].append({
                'date': row.get('Date', ''),
                'reason': row.get('Reason', ''),
                'operation': row.get('Operation', ''),
                'status': row.get('Current Status', '')
            })
    return history


def is_mistaken_return(tracking_id: str, history: dict, reference_date: datetime = None) -> tuple:
    """
    Check if a tracking ID should be flagged as already returned (exclude from chase).
    
    Returns: (is_returned: bool, event_date: str or None)
    
    Conditions (either triggers a return):
    1. WRONG_CYCLE_INDUCT reason on D-1 after 13:00
    2. INDUCTED status + PACKAGE_STATE_UPDATE operation from 23:30 D-1 to present
    """
    if tracking_id not in history:
        return False, None
    
    ref = reference_date or datetime.now()
    d_minus_1 = (ref - timedelta(days=1)).date()
    
    # Calculate the cutoff: 23:30 on D-1
    cutoff_time = datetime.combine(d_minus_1, datetime.min.time().replace(hour=23, minute=30))
    
    for event in history[tracking_id]:
        try:
            event_dt = datetime.strptime(event['date'], '%d/%m/%Y %H:%M:%S')
        except ValueError:
            continue
        
        # Condition 1: WRONG_CYCLE_INDUCT on D-1 after 13:00
        if event.get('reason') == 'WRONG_CYCLE_INDUCT':
            if event_dt.date() == d_minus_1 and event_dt.hour >= 13:
                return True, event['date']
        
        # Condition 2: INDUCTED + PACKAGE_STATE_UPDATE from 23:30 D-1 to now
        if (event.get('status') == 'INDUCTED' and 
            event.get('operation') == 'PACKAGE_STATE_UPDATE'):
            if cutoff_time <= event_dt <= ref:
                return True, event['date']
    
    return False, None


# ============================================================================
# DSP CHASE - ROUTE CODE HELPERS (v1.8)
# ============================================================================

def parse_tracer_routes(tracer_bytes: bytes) -> dict:
    """
    Parse Tracer file (NDNR Day-1 Raw Data) to extract route codes.
    Returns: {tracking_id: route_code}
    """
    routes = {}
    if not tracer_bytes:
        return routes
    for row in _open_csv(tracer_bytes):
        tid = row.get('trackingId', '').strip()
        route = row.get('RoutePlan.routeCode', '').strip()
        if tid and route:
            routes[tid] = route
    return routes


def parse_bulk_history_routes(history_bytes: bytes) -> dict:
    """
    Parse Bulk History file to extract route codes (fallback).
    Returns: {tracking_id: route_code}
    """
    routes = {}
    if not history_bytes:
        return routes
    for row in _open_csv(history_bytes):
        tid = row.get('Tracking ID', '').strip()
        route = row.get('Route Code', '').strip()
        if tid and route and tid not in routes:
            routes[tid] = route
    return routes


def get_route_code(tracking_id: str, tracer_routes: dict, history_routes: dict) -> str:
    """
    Get route code for a tracking ID.
    Priority: Tracer file first, then Bulk History fallback.
    Returns: route code string or empty string if not found.
    """
    if tracking_id in tracer_routes:
        return tracer_routes[tracking_id]
    if tracking_id in history_routes:
        return history_routes[tracking_id]
    return ''


# ============================================================================
# DSP CHASE
# ============================================================================

def generate_chase_messages(file_bytes: bytes, history_bytes: bytes = None,
                            tracer_bytes: bytes = None,
                            safe_mode: bool = False) -> tuple:
    """
    Returns: (messages_dict, returned_by_dsp_dict)
        - messages_dict: {dsp: message_text} for Slack (excludes returned)
        - returned_by_dsp_dict: {dsp: [list of tracking IDs]} for web display only
    
    v1.8: Route Code lookup from Tracer (with Bulk History fallback)
    v1.9: Grouped by Reason Code with Root Cause input fields
    """
    history = parse_bulk_history(history_bytes) if history_bytes else {}
    
    # Build route code lookups
    tracer_routes = parse_tracer_routes(tracer_bytes) if tracer_bytes else {}
    history_routes = parse_bulk_history_routes(history_bytes) if history_bytes else {}
    
    # v1.9: Group by DSP -> Reason Code -> list of {tid, route}
    dsp_data = defaultdict(lambda: {'by_reason': defaultdict(list), 'returned': []})

    for row in _open_csv(file_bytes):
        dsp    = row.get('DSP Name', '').strip()
        tid    = row.get('trackingId', '').strip()
        reason = row.get('Attempt Reason Code', '').strip() or 'UNKNOWN'
        
        if not (dsp and tid):
            continue
        
        # Get route code for this tracking ID
        route_code = get_route_code(tid, tracer_routes, history_routes)
        
        if history:
            is_mistaken, _ = is_mistaken_return(tid, history)
            if is_mistaken:
                dsp_data[dsp]['returned'].append({'tid': tid, 'route': route_code, 'reason': reason})
                continue
        
        # v1.9: Group by actual reason code
        dsp_data[dsp]['by_reason'][reason].append({'tid': tid, 'route': route_code})

    today    = datetime.now().strftime('%d/%m/%Y')
    messages = {}
    returned_by_dsp = {}
    
    for dsp in sorted(dsp_data.keys()):
        # Sort helper
        def sort_key(item):
            route_num = _extract_route_number(item['route']) if item['route'] else 9999
            return (route_num, item['tid'])
        
        by_reason = dsp_data[dsp]['by_reason']
        returned  = sorted(dsp_data[dsp]['returned'], key=sort_key)
        
        if returned:
            returned_by_dsp[dsp] = [r['tid'] for r in returned]
        
        if not by_reason:
            continue
        
        # v1.9: Build sections grouped by reason code
        sections = []
        total_packages = 0
        
        # Sort reasons alphabetically for consistent output
        for reason in sorted(by_reason.keys()):
            items = sorted(by_reason[reason], key=sort_key)
            total_packages += len(items)
            
            # Format reason code for display (replace underscores with spaces)
            reason_display = reason.replace('_', ' ').title()
            
            lines = [f'{reason_display} ({len(items)}):']
            for item in items:
                route_display = f' [{item["route"]}]' if item['route'] else ''
                lines.append(f'  {item["tid"]}{route_display}')
            
            # Add Root Cause input field after each reason section
            lines.append('')
            lines.append('Root Cause: _______________')
            
            sections.append('\n'.join(lines))

        content = (
            f'Outstanding Shipments — {dsp}\n'
            f'Updated: {today}\n'
            f'Total Packages: {total_packages}\n\n'
            f'Good morning. Please see below for any shipments not yet returned to station.\n\n'
            + '\n\n'.join(sections) +
            '\n\n' + DIVIDER + '\n\n'
            'Actions:\n'
            '1. Contact the driver to locate package\n'
            '2. Confirm return ETA to station\n'
            '3. Update this thread with status\n'
            '4. Escalate if no response within 2 hours\n\n'
            'Appreciate your support.'
        )
        messages[dsp] = wrap_message(content)
    
    return messages, returned_by_dsp

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
    route_lookup = _load_route_lookup(search_bytes) if search_bytes else {}
    dsp_pickups  = defaultdict(list)
    pickup_date  = None

    rows = list(_open_csv(pickup_bytes))
    for row in rows:
        ptype  = row.get('Pick up Type', '').strip()
        window = row.get('Pick up Start Window', '').strip()
        if not ptype:
            row['Pick up Type'] = 'NOREASON' if '00:00' in window else ''

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
        content = (
            f'{dsp} Pickups — {pickup_date}\n'
            f'{len(pickups)} awaiting pickup\n\n'
            + pad_cols(headers, data_rows)
        )
        messages[dsp] = wrap_message(content)

    return messages, pickup_date


# ============================================================================
# ROSTERING ACCURACY
# ============================================================================

def generate_rostering_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    dsp_data = defaultdict(lambda: defaultdict(list))
    all_dates = set()

    for row in _open_csv(file_bytes):
        dsp = str(row.get('DSP', '') or '').strip()
        if not dsp:
            continue
        date_str = fmt_date(row.get('startdate_local', ''))
        if date_str:
            all_dates.add(date_str)
        dsp_data[dsp][date_str].append(row)

    if not dsp_data:
        raise ValueError("No data found. Check the file contains a 'DSP' column.")

    sorted_dates = sorted(all_dates, key=parse_date)
    is_multi_day = len(sorted_dates) > 1
    date_range = format_date_range(sorted_dates)

    messages = {}
    for dsp in sorted(dsp_data.keys()):
        dates_dict = dsp_data[dsp]
        
        if is_multi_day:
            sections = []
            total_below_90 = 0
            total_service_types = 0
            
            for date_str in sorted(dates_dict.keys(), key=parse_date):
                rows = dates_dict[date_str]
                headers = ['Service Type', 'Compliance', 'Routes @15:30', 'Routes @Seq', 'D-1 vs D0']
                data_rows = []
                
                for r in rows:
                    svc = str(r.get('Service Type', '') or '').strip()
                    try:
                        comp_raw = float(r.get('Rostering Capacity Compliance %', 0) or 0)
                    except:
                        comp_raw = 0.0
                    flag = ' [!]' if comp_raw < 0.9 else ''
                    if comp_raw < 0.9:
                        total_below_90 += 1
                    total_service_types += 1
                    comp = f'{comp_raw:.0%}{flag}'
                    try:
                        r1530 = str(int(float(r.get('Rostered routes before 15:30', '') or 0)))
                    except:
                        r1530 = '-'
                    try:
                        r_seq = str(int(float(r.get('Rostered routes before Sequencing', '') or 0)))
                    except:
                        r_seq = '-'
                    try:
                        d1d0 = str(int(float(r.get('D-1 15:30 Plan vs D0 requested', '') or 0)))
                    except:
                        d1d0 = '-'
                    data_rows.append([svc, comp, r1530, r_seq, d1d0])
                
                if data_rows:
                    sections.append(f'📅 {date_str}\n' + pad_cols(headers, data_rows))
            
            summary_line = ''
            if total_below_90 > 0:
                summary_line = f'\n⚠️ {total_below_90}/{total_service_types} service type(s) below 90% across all dates\n'
            
            content = (
                f'Rostering Accuracy — {dsp} — {date_range}\n\n'
                f'Hi team, please find your rostering accuracy below for the period shown. '
                f'Provide root cause for any service type below 90%. [!] = below 90%\n'
                f'{summary_line}\n'
                + '\n\n'.join(sections)
            )
        else:
            first_date = sorted_dates[0] if sorted_dates else ''
            all_rows = []
            for date_rows in dates_dict.values():
                all_rows.extend(date_rows)
            
            headers = ['Service Type', 'Compliance', 'Routes @15:30', 'Routes @Seq', 'D-1 vs D0']
            data_rows = []

            for r in all_rows:
                svc = str(r.get('Service Type', '') or '').strip()
                try:
                    comp_raw = float(r.get('Rostering Capacity Compliance %', 0) or 0)
                except:
                    comp_raw = 0.0
                flag = ' [!]' if comp_raw < 0.9 else ''
                comp = f'{comp_raw:.0%}{flag}'
                try:
                    r1530 = str(int(float(r.get('Rostered routes before 15:30', '') or 0)))
                except:
                    r1530 = '-'
                try:
                    r_seq = str(int(float(r.get('Rostered routes before Sequencing', '') or 0)))
                except:
                    r_seq = '-'
                try:
                    d1d0 = str(int(float(r.get('D-1 15:30 Plan vs D0 requested', '') or 0)))
                except:
                    d1d0 = '-'
                data_rows.append([svc, comp, r1530, r_seq, d1d0])

            content = (
                f'Rostering Accuracy — {dsp} — {first_date}\n\n'
                f'Hi team, please find your rostering accuracy below. '
                f'Provide root cause for any service type below 90%. [!] = below 90%\n\n'
                + pad_cols(headers, data_rows)
            )
        
        messages[dsp] = wrap_message(content)

    return messages


# ============================================================================
# SERVICE TYPE COMPLIANCE (STC)
# ============================================================================

def generate_stc_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    SWAP_LABELS = {
        'plan:small execute:large': 'UPGRADED',
        'plan:large execute:small': 'DOWNGRADED',
        'plan equal execute':       'TIER MISMATCH',
    }
    
    dsp_data = defaultdict(lambda: defaultdict(list))
    all_dates = set()

    for row in _open_csv(file_bytes):
        if str(row.get('compliant', '1')).strip() != '0':
            continue
        dsp = str(row.get('dsp', '') or '').strip()
        if not dsp:
            continue
        
        date_str = fmt_date(row.get('date', ''))
        if date_str:
            all_dates.add(date_str)

        raw_vin = str(row.get('vin', '') or '').strip()
        dsp_data[dsp][date_str].append({
            'date':  date_str,
            'vin':   mask_id(raw_vin) if safe_mode else raw_vin,
            'd1':    str(row.get('day_1_planned_service_type_and_route_service_type', '') or '').strip(),
            'd0':    str(row.get('day0_actual_executed_vehicle_service_type', '') or '').strip(),
            'route': str(row.get('route_id', '') or '').strip(),
            'swap':  SWAP_LABELS.get(str(row.get('not_compliant_type', '') or '').strip(), 'UNKNOWN'),
        })

    if not dsp_data:
        raise ValueError("No non-compliant rows found. Check the file contains a 'compliant' column.")

    sorted_dates = sorted(all_dates, key=parse_date)
    is_multi_day = len(sorted_dates) > 1
    date_range = format_date_range(sorted_dates)

    messages = {}
    for dsp in sorted(dsp_data.keys()):
        dates_dict = dsp_data[dsp]
        
        if is_multi_day:
            sections = []
            total_swaps = 0
            swap_counts = defaultdict(int)
            
            for date_str in sorted(dates_dict.keys(), key=parse_date):
                rows = dates_dict[date_str]
                total_swaps += len(rows)
                
                headers = ['Route', 'VIN', 'D-1 Planned', 'D-0 Actual', 'Change']
                data_rows = []
                
                for r in rows:
                    swap_counts[r['swap']] += 1
                    data_rows.append([r['route'], r['vin'], r['d1'], r['d0'], r['swap']])
                
                if data_rows:
                    sections.append(
                        f'📅 {date_str} ({len(rows)} swap{"s" if len(rows) != 1 else ""})\n'
                        + pad_cols(headers, data_rows)
                    )
            
            swap_summary = ', '.join([f'{count} {swap_type}' for swap_type, count in sorted(swap_counts.items())])
            
            content = (
                f'Service Type Compliance — {dsp} — {date_range}\n\n'
                f'Hi team, please find below the D-0 vehicle swaps vs D-1 plan for the period shown. '
                f'Please provide insight on why these vehicles were swapped.\n\n'
                f'📊 Summary: {total_swaps} total swap(s) — {swap_summary}\n\n'
                + '\n\n'.join(sections)
            )
        else:
            first_date = sorted_dates[0] if sorted_dates else ''
            all_rows = []
            for date_rows in dates_dict.values():
                all_rows.extend(date_rows)
            
            headers = ['Route', 'VIN', 'D-1 Planned', 'D-0 Actual', 'Change']
            data_rows = [
                [r['route'], r['vin'], r['d1'], r['d0'], r['swap']]
                for r in all_rows
            ]
            content = (
                f'Service Type Compliance — {dsp} — {first_date}\n\n'
                f'Hi team, please find below the D-0 vehicle swaps vs D-1 plan. '
                f'Please provide insight on why these vehicles were swapped.\n\n'
                + pad_cols(headers, data_rows)
            )
        
        messages[dsp] = wrap_message(content)

    return messages


# ============================================================================
# CONTACT COMPLIANCE (CC)
# ============================================================================

def generate_cc_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
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

        content = (
            f'Contact Compliance — {dsp} — {first_date}\n'
            f'{len(data_rows)} exception(s)\n\n'
            + pad_cols(headers, data_rows)
        )
        messages[dsp] = wrap_message(content)

    return messages


# ============================================================================
# PICTURE ON DELIVERY (POD)
# ============================================================================

def generate_pod_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
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

        content = (
            f'POD Opportunities — {dsp} — {first_date}\n'
            f'{len(data_rows)} reject(s)\n\n'
            + pad_cols(headers, data_rows)
        )
        messages[dsp] = wrap_message(content)

    return messages

# ============================================================================
# NOTIFY OF ARRIVAL (NOA) - v2.0
# ============================================================================

def generate_noa_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Generate NOA summary messages per DSP.
    
    v2.0: Simplified to DSP-level counts only.
          Transporter ID column was removed from source file.
    """
    dates_seen = set()
    dsp_counts = defaultdict(int)

    for row in _open_csv(file_bytes):
        if str(row.get('chat_reason_code', '') or '').strip() != 'NOTIFY_OF_ARRIVAL':
            continue
        dsp     = str(row.get('DSP', '') or '').strip()
        scan_id = str(row.get('Scannable ID', '') or '').strip()
        if not dsp or not scan_id:
            continue
        
        event_date = fmt_date(row.get('Event Date', ''))
        if event_date:
            dates_seen.add(event_date)
        
        dsp_counts[dsp] += 1

    if not dsp_counts:
        raise ValueError("No NOTIFY_OF_ARRIVAL rows found. Check the correct Exceptions CSV is selected.")

    date_range = format_date_range(list(dates_seen)) if dates_seen else datetime.now().strftime('%d/%m/%Y')
    
    intro = (
        'Hi all — Notify of Arrival has a positive impact on OTR safety, Concessions, '
        'CC, DCR and overall customer experience. '
        'Below is the NOA summary for your DSP.'
    )

    messages = {}
    for dsp in sorted(dsp_counts.keys()):
        count = dsp_counts[dsp]
        
        content = (
            f'Notify of Arrival — {dsp} — {date_range}\n\n'
            f'{intro}\n\n'
            f'Total NOA Events: {count}\n\n'
            'Keep up the great work! 🎉'
        )
        messages[dsp] = wrap_message(content)

    return messages


# ============================================================================
# UNRETURNED BAGS
    return messages


# ============================================================================
# UNRETURNED BAGS
# ============================================================================

def generate_bags_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    FLAG_THRESHOLD = 3

    dsp_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for row in _open_csv(file_bytes):
        dsp      = str(row.get('DSP', '') or '').strip()
        route    = str(row.get('Route Code', '') or '').strip()
        bag      = str(row.get('Bag', '') or '').strip()
        date_raw = str(row.get('Date', '') or '').strip()

        if not dsp or not route or not bag:
            continue

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

        sections = []
        for date_str in all_dates:
            routes_dict   = dates_dict[date_str]
            sorted_routes = sorted(routes_dict.keys())

            headers   = ['Route Code', 'Missing Bags']
            data_rows = []
            for route in sorted_routes:
                count = len(routes_dict[route])
                if count < 2:
                    continue
                flag = ' [!]' if count >= FLAG_THRESHOLD else ''
                data_rows.append([route, f'{count}{flag}'])

            if not data_rows:
                continue

            sections.append(
                f'{date_str}\n'
                + pad_cols(headers, data_rows)
            )

        flag_line = (
            f'{flagged_count} route(s) with {FLAG_THRESHOLD}+ bags flagged [!]\n'
            if flagged_count else ''
        )

        content = (
            f'Unreturned Bags — {dsp} — {date_from} to {date_to}\n'
            f'Total unreturned: {total_bags} bag(s)\n'
            f'{flag_line}'
            f'\n'
            + '\n\n'.join(sections)
        )
        messages[dsp] = wrap_message(content)

    return messages


# ============================================================================
# CARRIER INVESTIGATIONS
# ============================================================================

def generate_carrier_inv_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    dsp_data = {}

    for row in _open_csv(file_bytes):
        dsp = str(row.get('dsp_shortcode', '') or '').strip()
        if not dsp:
            continue

        try:
            investigations = int(float(row.get('Carrier Investigations', 0) or 0))
        except (ValueError, TypeError):
            investigations = 0

        try:
            responses = int(float(row.get('DSP Responses', 0) or 0))
        except (ValueError, TypeError):
            responses = 0

        if investigations > 0:
            dsp_data[dsp] = {
                'investigations': investigations,
                'responses': responses,
            }

    if not dsp_data:
        raise ValueError(
            "No carrier investigation data found.\n"
            "Check the file contains 'dsp_shortcode' and 'Carrier Investigations' columns."
        )

    messages = {}
    for dsp in sorted(dsp_data.keys()):
        data = dsp_data[dsp]

        headers = ['Carrier Investigations', 'DSP Responses']
        data_rows = [[str(data['investigations']), str(data['responses'])]]

        content = (
            f'DNR Carrier Investigations — {dsp}\n\n'
            f'Please see below current performance week to date.\n\n'
            + pad_cols(headers, data_rows)
        )
        messages[dsp] = wrap_message(content)

    return messages


# ============================================================================
# VSA
# ============================================================================

def mask_vin(raw_vin: str) -> str:
    if not raw_vin:
        return 'VIN-????'
    suffix = raw_vin.strip()[-4:].upper()
    return f'VIN-{suffix}'


def generate_vsa_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    dsp_data = defaultdict(list)

    for row in _open_csv(file_bytes):
        inspection = str(row.get('inspection_passed', '') or '').strip().upper()
        if inspection != 'N':
            continue
        
        dsp = str(row.get('dsp', '') or '').strip().upper()
        if not dsp:
            continue
        
        raw_vin = str(row.get('vin', '') or '').strip()
        vrn = str(row.get('vrns', '') or '').strip()
        
        dsp_data[dsp].append({
            'vin': mask_vin(raw_vin) if safe_mode else raw_vin,
            'vrn': vrn,
        })

    if not dsp_data:
        raise ValueError(
            "No vehicles pending VSA found (inspection_passed = 'N').\n"
            "Check the file contains 'dsp', 'vin', and 'inspection_passed' columns."
        )

    messages = {}
    for dsp in sorted(dsp_data.keys()):
        vehicles = dsp_data[dsp]
        vehicles_sorted = sorted(vehicles, key=lambda x: x['vrn'])
        
        headers = ['DSP Name', 'VRN', 'VIN']
        data_rows = [[dsp, v['vrn'], v['vin']] for v in vehicles_sorted]

        intro = (
            'Hi team,\n'
            'The following list of Vans are still pending a VSA for this cycle.\n'
            'Please reply to this message and let us know if or when the Van will be next used.\n'
            'Thank you'
        )

        content = (
            f'@here\n'
            f'## Bi-Weekly VSA Audits ##\n\n'
            f'{intro}\n\n'
            f'{dsp} — {len(vehicles)} vehicle(s) pending\n\n'
            + pad_cols(headers, data_rows)
        )
        messages[dsp] = wrap_message(content)

    return messages



# ============================================================================
# NURSERY OVERUSE
# ============================================================================

def generate_nursery_overuse_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Input: Overused_Nursery_Rou_*.csv
    
    Identifies routes where a Standard DA was assigned to a Nursery-level route.
    Groups by DSP -> Date, shows planned vs actual DA tenure level.
    
    Safe mode: no change (no driver IDs in this report).
    
    Columns used:
        DSP, Route ID, Date, Actual Level Tenure DA, Planned Level Tenure DA
    
    Output: One message per DSP with date-grouped route violations.
    """
    # dsp_data[dsp][date_str] = list of route dicts
    dsp_data = defaultdict(lambda: defaultdict(list))
    level_counts = defaultdict(lambda: defaultdict(int))  # dsp -> level -> count

    for row in _open_csv(file_bytes):
        dsp = str(row.get('DSP', '') or '').strip().upper()
        if not dsp:
            continue
        
        route_id = str(row.get('Route ID', '') or '').strip()
        date_raw = str(row.get('Date', '') or '').strip()
        actual = str(row.get('Actual Level Tenure DA', '') or '').strip()
        planned = str(row.get('Planned Level Tenure DA', '') or '').strip()
        
        if not route_id or not planned:
            continue
        
        date_str = fmt_date(date_raw)
        
        dsp_data[dsp][date_str].append({
            'route': route_id,
            'planned': planned,
            'actual': actual,
        })
        
        # Count by nursery level for summary
        level_counts[dsp][planned] += 1

    if not dsp_data:
        raise ValueError(
            "No nursery overuse data found.\n"
            "Check the file contains 'DSP', 'Route ID', 'Date', "
            "'Actual Level Tenure DA', and 'Planned Level Tenure DA' columns."
        )

    messages = {}
    for dsp in sorted(dsp_data.keys()):
        dates_dict = dsp_data[dsp]
        all_dates = sorted(
            dates_dict.keys(),
            key=lambda d: datetime.strptime(d, '%d/%m/%Y') if d else datetime.min
        )
        date_from = all_dates[0] if all_dates else ''
        date_to = all_dates[-1] if all_dates else ''
        
        # Total routes overused
        total_routes = sum(len(routes) for routes in dates_dict.values())
        
        # Build summary by level
        levels = level_counts[dsp]
        level_summary = ', '.join(
            f'{count} {level.replace("Nursery Route ", "")}'
            for level, count in sorted(levels.items())
        )
        
        # Build per-date sections
        sections = []
        for date_str in all_dates:
            routes = dates_dict[date_str]
            
            headers = ['Route', 'Planned DA Level', 'Actual DA Level']
            data_rows = [
                [r['route'], r['planned'], r['actual']]
                for r in sorted(routes, key=lambda x: x['route'])
            ]
            
            sections.append(
                f':date: {date_str} ({len(routes)} route{"s" if len(routes) != 1 else ""})\n'
                + pad_cols(headers, data_rows)
            )
        
        content = (
            f'Nursery Route Overuse — {dsp} — {date_from} to {date_to}\n\n'
            f'Hi team, please find below the Nursery Route assignments where a Standard DA '
            f'was assigned to a route planned for a Nursery-level DA. '
            f'Please provide insight on why these assignments occurred.\n\n'
            f':bar_chart: Summary: {total_routes} total overuse(s) — {level_summary}\n\n'
            + '\n\n'.join(sections)
        )
        messages[dsp] = wrap_message(content)

    return messages



# ============================================================================
# RIDEALONG OVERUSE
# ============================================================================

def generate_ridealong_overuse_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Input: Raw_DataOnly_showing_*.csv
    
    Identifies Ridealong routes flagged for overuse (DA Count <2 or Two tenured DAs).
    Groups by DSP -> Date, shows route details and overuse reason.
    
    Safe mode: no change (no driver IDs in this report).
    
    Columns used:
        DSP, Route ID, Date, Executed Service Type, Overuse Reason, DA 1 Roster
    
    Output: One message per DSP with date-grouped route violations.
    """
    # dsp_data[dsp][date_str] = list of route dicts
    dsp_data = defaultdict(lambda: defaultdict(list))
    reason_counts = defaultdict(lambda: defaultdict(int))  # dsp -> reason -> count

    for row in _open_csv(file_bytes):
        dsp = str(row.get('DSP', '') or '').strip().upper()
        if not dsp:
            continue
        
        route_id = str(row.get('Route ID', '') or '').strip()
        date_raw = str(row.get('Date', '') or '').strip()
        exec_svc = str(row.get('Executed Service Type', '') or '').strip()
        overuse_reason = str(row.get('Overuse Reason', '') or '').strip()
        da1_roster = str(row.get('DA 1 Roster', '') or '').strip()
        
        if not route_id:
            continue
        
        date_str = fmt_date(date_raw)
        
        dsp_data[dsp][date_str].append({
            'route': route_id,
            'exec_svc': exec_svc,
            'reason': overuse_reason,
            'da1_role': da1_roster,
        })
        
        # Count by overuse reason for summary
        reason_counts[dsp][overuse_reason] += 1

    if not dsp_data:
        raise ValueError(
            "No ridealong overuse data found.\n"
            "Check the file contains 'DSP', 'Route ID', 'Date', "
            "'Executed Service Type', and 'Overuse Reason' columns."
        )

    messages = {}
    for dsp in sorted(dsp_data.keys()):
        dates_dict = dsp_data[dsp]
        all_dates = sorted(
            dates_dict.keys(),
            key=lambda d: datetime.strptime(d, '%d/%m/%Y') if d else datetime.min
        )
        date_from = all_dates[0] if all_dates else ''
        date_to = all_dates[-1] if all_dates else ''
        
        # Total routes overused
        total_routes = sum(len(routes) for routes in dates_dict.values())
        
        # Build summary by reason
        reasons = reason_counts[dsp]
        reason_summary = ', '.join(
            f'{count} {reason}'
            for reason, count in sorted(reasons.items())
        )
        
        # Build per-date sections
        sections = []
        for date_str in all_dates:
            routes = dates_dict[date_str]
            
            headers = ['Route', 'Executed Service Type', 'Overuse Reason', 'DA 1 Role']
            data_rows = [
                [r['route'], r['exec_svc'], r['reason'], r['da1_role']]
                for r in sorted(routes, key=lambda x: x['route'])
            ]
            
            sections.append(
                f':date: {date_str} ({len(routes)} route{"s" if len(routes) != 1 else ""})\n'
                + pad_cols(headers, data_rows)
            )
        
        content = (
            f'Ridealong Overuse — {dsp} — {date_from} to {date_to}\n\n'
            f'Hi team, please find below Ridealong routes where the route was flagged for overuse. '
            f'Please provide insight on why these assignments occurred.\n\n'
            f':bar_chart: Summary: {total_routes} total overuse(s) — {reason_summary}\n\n'
            + '\n\n'.join(sections)
        )
        messages[dsp] = wrap_message(content)

    return messages



# ============================================================================
# ============================================================================
# TRACER BRIDGE
# ============================================================================

# DSP Name mapping (full name -> short code)
DSP_NAME_MAP = {
    'hero parcel logistics limited': 'HPLM',
    'deliverwize ltd': 'DELL',
    'dtt deliveries ltd': 'DTTD',
    'dtt deliveries ltd ': 'DTTD',  # trailing space variant
    'v1 logistics': 'VILO',
    'universal courier logistical services limited': 'ULSL',
    'kmi logistics ltd': 'KMIL',
    'wac couriers ltd': 'WACC',
    'danzen logistics ltd': 'DNZN',
    'dyy ltd': 'DYYL',
    'molina express ltd': 'MOLI',
    'alkaia ltd': 'AKTD',
    'greythorn services': 'GSSL',
    'csp_company_name': 'CSP',
}


def _normalize_dsp_name(dsp_name: str) -> str:
    """Convert full DSP name to short code."""
    if not dsp_name:
        return 'UNKNOWN'
    normalized = dsp_name.strip().lower()
    return DSP_NAME_MAP.get(normalized, dsp_name.strip().upper()[:4])


def _parse_bulk_history_returns(history_bytes: bytes) -> dict:
    """
    Parse bulk history to find returned packages (WRONG_CYCLE_INDUCT).
    Returns dict: {tracking_id: return_date}
    """
    returns = {}
    if not history_bytes:
        return returns
    
    for row in _open_csv(history_bytes):
        tid = row.get('Tracking ID', '').strip()
        reason = row.get('Reason', '').strip()
        date_str = row.get('Date', '').strip()
        
        if reason == 'WRONG_CYCLE_INDUCT' and tid:
            if tid not in returns:
                returns[tid] = fmt_date(date_str)
    
    return returns


def generate_tracer_bridge_messages(not_recovered_bytes: bytes, search_bytes: bytes,
                                     bulk_history_bytes: bytes = None,
                                     safe_mode: bool = False) -> dict:
    """
    Input files:
        - Not_Recovered_Deep_D_*.csv (required) — Package list with reasons
        - SearchResults*.csv (required) — DSP lookup via Tracking ID
        - bulk_history_export_*.csv (optional) — Return detection (WRONG_CYCLE_INDUCT)
    
    Generates a tracer bridge showing not recovered packages grouped by DSP,
    with returned packages separated out.
    
    Safe mode: no change (no driver IDs in this report).
    
    Output: Single station-level message with DSP breakdown.
    """
    # Build DSP lookup from SearchResults
    dsp_lookup = {}
    for row in _open_csv(search_bytes):
        tid = str(row.get('Tracking ID', '') or '').strip()
        dsp_full = str(row.get('DSP Name', '') or '').strip()
        if tid and dsp_full:
            dsp_lookup[tid] = _normalize_dsp_name(dsp_full)
    
    # Parse bulk history for returns
    returns = _parse_bulk_history_returns(bulk_history_bytes)
    
    # Parse Not Recovered file
    packages = []
    station = ''
    
    for row in _open_csv(not_recovered_bytes):
        tid = str(row.get('TrackingID', '') or '').strip()
        reason = str(row.get('reason_before_missing', '') or '').strip()
        is_rejected = str(row.get('is_rejected', '') or '').strip().upper() == 'Y'
        
        if not station:
            station = str(row.get('parent_location', '') or '').strip()
        
        # Get shipment value
        try:
            value = float(row.get('Shipment Value', 0) or 0)
        except (ValueError, TypeError):
            value = 0.0
        
        # Normalize reason
        if not reason or reason == 'NONE':
            reason = 'NO_REASON'
        
        # Get DSP from lookup
        dsp = dsp_lookup.get(tid, 'UNKNOWN')
        
        # Check if returned
        return_date = returns.get(tid)
        
        packages.append({
            'tid': tid,
            'dsp': dsp,
            'reason': reason,
            'value': value,
            'is_rejected': is_rejected,
            'returned': return_date is not None,
            'return_date': return_date,
        })
    
    if not packages:
        raise ValueError(
            "No tracer data found.\n"
            "Check the Not Recovered file contains 'TrackingID' and 'reason_before_missing' columns,\n"
            "and the SearchResults file contains 'Tracking ID' and 'DSP Name' columns."
        )
    
    # Separate into categories
    returned_packages = [p for p in packages if p['returned']]
    rejected_packages = [p for p in packages if p['is_rejected'] and not p['returned']]
    outstanding_packages = [p for p in packages if not p['returned'] and not p['is_rejected']]
    
    # Calculate totals
    total = len(packages)
    total_returned = len(returned_packages)
    total_rejected = len(rejected_packages)
    total_outstanding = len(outstanding_packages)
    total_value = sum(p['value'] for p in outstanding_packages)
    # Group outstanding by DSP - track units and value per DSP
    dsp_outstanding = defaultdict(lambda: defaultdict(list))
    dsp_values = defaultdict(float)
    for p in outstanding_packages:
        dsp_outstanding[p['dsp']][p['reason']].append(p['tid'])
        dsp_values[p['dsp']] += p['value']
    
    # Group returned by DSP
    dsp_returned = defaultdict(list)
    for p in returned_packages:
        dsp_returned[p['dsp']].append(p)
    
    # Group rejected by DSP
    dsp_rejected = defaultdict(int)
    for p in rejected_packages:
        dsp_rejected[p['dsp']] += 1
    
    # Count by reason across all outstanding
    reason_totals = defaultdict(int)
    for p in outstanding_packages:
        reason_totals[p['reason']] += 1
    
    # Build reason summary line
    reason_summary = ' | '.join(
        f'{reason}: {count}'
        for reason, count in sorted(reason_totals.items(), key=lambda x: -x[1])
    )
    
    # Build DSP breakdown for outstanding - now includes shipments and value
    dsp_lines = []
    for dsp in sorted(dsp_outstanding.keys(), key=lambda d: -sum(len(t) for t in dsp_outstanding[d].values())):
        reasons = dsp_outstanding[dsp]
        dsp_shipments = sum(len(tids) for tids in reasons.values())
        dsp_value = dsp_values[dsp]
        
        reason_parts = []
        for reason, tids in sorted(reasons.items(), key=lambda x: -len(x[1])):
            reason_parts.append(f'{reason}: {len(tids)}')
        
        dsp_lines.append(f'{dsp} — {dsp_shipments} shipments (£{dsp_value:,.2f}) — {", ".join(reason_parts)}')
    
    # Get current date for header
    today = datetime.now()
    week_num = today.isocalendar()[1]
    # Build the bridge message
    lines = []
    lines.append(f':bar_chart: {station} TRACER BRIDGE — Week {week_num} | {today.strftime("%d/%m/%Y")}')
    lines.append(DIVIDER)
    lines.append('')
    lines.append(':clipboard: SUMMARY')
    lines.append(f'Total Shipments: {total}')
    lines.append(f'  • Returned to Station: {total_returned} shipments')
    lines.append(f'  • Customer Rejected: {total_rejected} shipments')
    lines.append(f'  • Still Outstanding: {total_outstanding} shipments (£{total_value:,.2f})')
    lines.append('')
    
    # Outstanding by DSP
    if outstanding_packages:
        lines.append(f':red_circle: NOT RECOVERED BY DSP ({total_outstanding} shipments)')
        lines.append(DIVIDER)
        lines.extend(dsp_lines)
        lines.append('')
        lines.append('')
    
    # Reason breakdown
    if reason_totals:
        lines.append(':mag: REASON BREAKDOWN')
        lines.append(reason_summary)
        lines.append('')
    
    # Returned packages
    if returned_packages:
        lines.append(f':large_green_circle: RETURNED TO STATION ({total_returned} shipments)')
        lines.append(DIVIDER)
        for dsp in sorted(dsp_returned.keys()):
            pkgs = dsp_returned[dsp]
            for p in pkgs:
                lines.append(f'{p["tid"]} — {dsp} — Returned {p["return_date"]}')
        lines.append('')
    
    # Rejected packages
    if rejected_packages:
        lines.append(f':large_yellow_circle: CUSTOMER REJECTED ({total_rejected} shipments)')
        lines.append(DIVIDER)
        rejected_str = ' | '.join(
            f'{dsp}: {count}'
            for dsp, count in sorted(dsp_rejected.items(), key=lambda x: -x[1])
        )
        lines.append(rejected_str)
    
    content = '\n'.join(lines)
    messages = {station or 'STATION': wrap_message(content)}
    
    return messages
