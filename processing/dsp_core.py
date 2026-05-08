"""
DSP Tools Hub — Processing Engine v1.8
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
       Packages now display with their assigned Route Code in messages
"""
import csv
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
    
    v1.8: Now includes Route Code lookup from Tracer (with Bulk History fallback)
    """
    history = parse_bulk_history(history_bytes) if history_bytes else {}
    
    # v1.8: Build route code lookups
    tracer_routes = parse_tracer_routes(tracer_bytes) if tracer_bytes else {}
    history_routes = parse_bulk_history_routes(history_bytes) if history_bytes else {}
    
    dsp_data = defaultdict(lambda: {'chase': [], 'missing': [], 'returned': []})

    for row in _open_csv(file_bytes):
        dsp    = row.get('DSP Name', '').strip()
        tid    = row.get('trackingId', '').strip()
        reason = row.get('Attempt Reason Code', '').strip()
        
        if not (dsp and tid):
            continue
        
        # v1.8: Get route code for this tracking ID
        route_code = get_route_code(tid, tracer_routes, history_routes)
        
        if history:
            is_mistaken, _ = is_mistaken_return(tid, history)
            if is_mistaken:
                dsp_data[dsp]['returned'].append({'tid': tid, 'route': route_code})
                continue
        
        if reason == 'ITEMS_MISSING':
            dsp_data[dsp]['missing'].append({'tid': tid, 'route': route_code})
        else:
            dsp_data[dsp]['chase'].append({'tid': tid, 'route': route_code})

    today    = datetime.now().strftime('%d/%m/%Y')
    messages = {}
    returned_by_dsp = {}
    
    for dsp in sorted(dsp_data.keys()):
        # Sort by route code (numeric extraction) then by tracking ID
        def sort_key(item):
            route_num = _extract_route_number(item['route']) if item['route'] else 9999
            return (route_num, item['tid'])
        
        chase    = sorted(dsp_data[dsp]['chase'], key=sort_key)
        missing  = sorted(dsp_data[dsp]['missing'], key=sort_key)
        returned = sorted(dsp_data[dsp]['returned'], key=sort_key)
        
        if returned:
            returned_by_dsp[dsp] = [r['tid'] for r in returned]
        
        sections = []
        if chase:
            # v1.8: Include route code in output
            lines = [f'To Chase ({len(chase)}):']
            for item in chase:
                route_display = f' [{item["route"]}]' if item['route'] else ''
                lines.append(f'  {item["tid"]}{route_display}')
            sections.append('\n'.join(lines))

        if missing:
            # v1.8: Include route code in output
            lines = [f'Driver Marked Missing ({len(missing)}):']
            for item in missing:
                route_display = f' [{item["route"]}]' if item['route'] else ''
                lines.append(f'  {item["tid"]}{route_display}')
            sections.append('\n'.join(lines))

        if not sections:
            continue

        content = (
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
# NOTIFY OF ARRIVAL (NOA)
# ============================================================================

def generate_noa_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
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

        content = (
            f'Notify of Arrival — {dsp} — {first_date}\n\n'
            f'{intro}\n\n'
            + pad_cols(headers, data_rows)
        )
        messages[dsp] = wrap_message(content)

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
