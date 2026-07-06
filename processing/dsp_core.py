"""
DSP Tools Hub — Processing Engine v2.0 (COMPLIANT)
==================================================
Compliant with On-Road DSP Collaboration SOP (Week 21)

COMPLIANCE PRINCIPLES:
1. DSP-level data ONLY — no route-level, no DA/TRID-level
2. Informational language ONLY — no action requests
3. Support-driven — data shared for DSP awareness, not OPS direction

REMOVED TOOLS (no compliant path):
- Nursery Overuse — directs DA deployment decisions
- Ridealong Overuse — directs DA deployment decisions

MODIFIED TOOLS (DSP totals only):
- Chase: Total scrub errors per DSP
- Pickups: Total awaiting pickup per DSP
- Rostering: Compliance % per DSP
- STC: Fleet compliance % per DSP
- CC: Contact compliance % per DSP
- POD: POD compliance % per DSP
- NOA: Total NOA events per DSP
- Bags: Total unreturned bags per DSP
- Carrier Inv: Total investigations per DSP
- VSA: Total vehicles pending per DSP
- Tracer: Total not recovered per DSP

v2.0 - Framework-compliant rebuild (Week 21)
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


def _open_csv(file_bytes: bytes):
    text = file_bytes.decode('utf-8-sig')
    return csv.DictReader(io.StringIO(text))


def wrap_message(content: str) -> str:
    return f'{DIVIDER}\n{content}\n{DIVIDER}'


# ============================================================================
# COMPLIANT MESSAGE FOOTER
# ============================================================================

COMPLIANT_FOOTER = (
    '\n\n'
    'ℹ️ This is DSP-level summary data for your awareness.\n'
    'For detailed breakdown, we advise that you refer to your DSP tools.\n'
    '📞 Need support? Contact OPS during DDM / DORM windows.'
)


# ============================================================================
# DSP CHASE
# ============================================================================

def generate_chase_messages(file_bytes: bytes, search_bytes: bytes = None,
                            safe_mode: bool = False) -> tuple:
    """
    DSP Chase — route number + shipment ID output.

    Inputs:
        file_bytes   : OUTSTANDING SCRUB ERROR*.csv
        search_bytes : SearchResults*.csv  (Tracking ID -> Route Code lookup)

    Returns: (messages_dict, {})
        - messages_dict: {dsp: message_text}
        - empty dict (second value kept for backwards-compatible call signature)
    """
    # Build route code lookup from SearchResults
    route_lookup = {}
    if search_bytes:
        for row in _open_csv(search_bytes):
            tid   = row.get('Tracking ID', '').strip()
            route = row.get('Route Code', '').strip()
            if tid and route:
                route_lookup[tid] = route

    # Group by DSP -> (Status Code, Reason Code) -> list of {tid, route}
    dsp_data = defaultdict(lambda: {'by_reason': defaultdict(list)})

    # Internal status → display label mapping (used when attempt fields are blank)
    INTERNAL_LABEL_MAP = {
        ('MISSING', 'SHIPMENT_RECEIVED'): ('Delivery Failed', 'Damaged'),
    }

    for row in _open_csv(file_bytes):
        dsp    = row.get('DSP Name', '').strip()
        tid    = row.get('trackingId', '').strip()
        status = row.get('Attempt Status Code', '').strip()
        reason = row.get('Attempt Reason Code', '').strip()

        if not (dsp and tid):
            continue

        if not status and not reason:
            # Fall back to internal fields for display label
            int_status = row.get('InternalStatusCode', '').strip()
            int_reason = row.get('InternalReasonCode', '').strip()
            status, reason = INTERNAL_LABEL_MAP.get(
                (int_status, int_reason),
                (int_status or 'UNKNOWN', int_reason or 'UNKNOWN')
            )
        
        route_code = route_lookup.get(tid, '')
        group_key  = (status, reason)
        dsp_data[dsp]['by_reason'][group_key].append({'tid': tid, 'route': route_code})

    today    = datetime.now().strftime('%d/%m/%Y')
    messages = {}

    for dsp in sorted(dsp_data.keys()):

        def sort_key(item):
            if item['route']:
                nums = re.findall(r'\d+', item['route'])
                return (int(nums[-1]) if nums else 9999, item['tid'])
            return (9999, item['tid'])

        by_reason = dsp_data[dsp]['by_reason']
        if not by_reason:
            continue

        sections       = []
        total_packages = 0

        for group_key in sorted(by_reason.keys()):
            items = sorted(by_reason[group_key], key=sort_key)
            total_packages += len(items)

            status_raw, reason_raw = group_key
            status_display = status_raw.replace('_', ' ').title()
            reason_display = reason_raw.replace('_', ' ').title()
            group_display  = f'{status_display} — {reason_display}'
            lines = [f'{group_display} ({len(items)}):']
            for item in items:
                route_display = f' [{item["route"]}]' if item['route'] else ''
                lines.append(f'  {item["tid"]}{route_display}')

            sections.append('\n'.join(lines))

        content = (
            f'Outstanding Shipments \u2014 {dsp}\n'
            f'Updated: {today}\n'
            f'Total Packages: {total_packages}\n\n'
            f'Good morning. Please see below for any shipments not yet returned to station.\n\n'
            + '\n\n'.join(sections)
            + '\n\n\n'
            + DIVIDER
        )
        messages[dsp] = wrap_message(content)

    return messages, {}



# ============================================================================
# DSP PICKUPS — COMPLIANT v2.0
# ============================================================================

def generate_pickup_messages(pickup_bytes: bytes, search_bytes: bytes = None,
                              safe_mode: bool = False) -> tuple:
    """
    COMPLIANT v2.0: DSP-level totals only.
    
    Output: "Your DSP has X packages awaiting pickup"
    
    NO route lookup, NO tracking IDs, NO action requests.
    """
    dsp_counts = defaultdict(lambda: {'total': 0, 'by_type': defaultdict(int)})
    pickup_date = None
    
    for row in _open_csv(pickup_bytes):
        dsp = row.get('Dsp', '').strip()
        tid = row.get('trackingId', '').strip()
        ptype = row.get('Pick up Type', '').strip().upper()
        
        if not pickup_date:
            sw = row.get('Pick up Start Window', '')
            pickup_date = sw.split(' ')[0] if sw else datetime.now().strftime('%d/%m/%Y')
        
        if not (dsp and tid):
            continue
        
        # Categorise pickup type
        if ptype == 'LOCKER':
            category = 'Locker'
        elif ptype == 'NOREASON' or '00:00' in row.get('Pick up Start Window', ''):
            category = 'Counter'
        elif not ptype:
            category = 'Home'
        else:
            category = ptype.title()
        
        dsp_counts[dsp]['total'] += 1
        dsp_counts[dsp]['by_type'][category] += 1
    
    if not pickup_date:
        pickup_date = datetime.now().strftime('%d/%m/%Y')
    
    messages = {}
    for dsp in sorted(dsp_counts.keys()):
        data = dsp_counts[dsp]
        total = data['total']
        
        if total == 0:
            continue
        
        # Build type breakdown
        type_lines = []
        for ptype, count in sorted(data['by_type'].items(), key=lambda x: -x[1]):
            type_lines.append(f'  • {ptype}: {count}')
        
        type_summary = '\n'.join(type_lines) if type_lines else '  • No breakdown available'
        
        content = (
            f'📬 Pickup Summary — {dsp}\n'
            f'Date: {pickup_date}\n\n'
            f'Your DSP has {total} package(s) awaiting pickup.\n\n'
            f'Breakdown by type:\n'
            f'{type_summary}'
            f'{COMPLIANT_FOOTER}'
        )
        messages[dsp] = wrap_message(content)
    
    return messages, pickup_date


# ============================================================================
# ROSTERING ACCURACY — COMPLIANT v2.0
# ============================================================================

def generate_rostering_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    COMPLIANT v2.0: DSP-level compliance percentages only.
    
    Output: "Your DSP rostering compliance: X%"
    
    NO route-level slot detail, NO action requests.
    """
    dsp_data = defaultdict(lambda: {
        'total_compliance': [],
        'by_service_type': defaultdict(list),
        'dates': set()
    })
    
    for row in _open_csv(file_bytes):
        dsp = str(row.get('DSP', '') or '').strip()
        if not dsp:
            continue
        
        date_str = fmt_date(row.get('startdate_local', ''))
        if date_str:
            dsp_data[dsp]['dates'].add(date_str)
        
        svc_type = str(row.get('Service Type', '') or '').strip()
        
        try:
            comp = float(row.get('Rostering Capacity Compliance %', 0) or 0)
        except:
            comp = 0.0
        
        dsp_data[dsp]['total_compliance'].append(comp)
        if svc_type:
            dsp_data[dsp]['by_service_type'][svc_type].append(comp)
    
    if not dsp_data:
        raise ValueError("No data found. Check the file contains a 'DSP' column.")
    
    messages = {}
    for dsp in sorted(dsp_data.keys()):
        data = dsp_data[dsp]
        
        # Calculate overall average compliance
        all_comp = data['total_compliance']
        avg_compliance = sum(all_comp) / len(all_comp) if all_comp else 0
        
        # Calculate per-service-type averages
        svc_lines = []
        below_threshold = 0
        for svc_type, comps in sorted(data['by_service_type'].items()):
            svc_avg = sum(comps) / len(comps) if comps else 0
            flag = ' ⚠️' if svc_avg < 0.9 else ''
            if svc_avg < 0.9:
                below_threshold += 1
            svc_lines.append(f'  • {svc_type}: {svc_avg:.0%}{flag}')
        
        svc_summary = '\n'.join(svc_lines) if svc_lines else '  • No service type breakdown'
        
        date_range = format_date_range(list(data['dates']))
        
        # Status indicator
        status = '✅ On track' if avg_compliance >= 0.9 else '⚠️ Below 90% threshold'
        
        content = (
            f'📋 Rostering Summary — {dsp}\n'
            f'Period: {date_range}\n\n'
            f'Overall Compliance: {avg_compliance:.0%} {status}\n\n'
            f'By Service Type:\n'
            f'{svc_summary}'
            f'{COMPLIANT_FOOTER}'
        )
        messages[dsp] = wrap_message(content)
    
    return messages


# ============================================================================
# SERVICE TYPE COMPLIANCE (STC) — COMPLIANT v2.0
# ============================================================================

def generate_stc_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    COMPLIANT v2.0: Fleet compliance percentage only.
    
    Output: "Your DSP fleet STC compliance: X%"
    
    NO VINs, NO route IDs, NO vehicle-level detail.
    """
    dsp_data = defaultdict(lambda: {
        'total_routes': 0,
        'non_compliant': 0,
        'swap_types': defaultdict(int),
        'dates': set()
    })
    
    for row in _open_csv(file_bytes):
        dsp = str(row.get('dsp', '') or '').strip()
        if not dsp:
            continue
        
        date_str = fmt_date(row.get('date', ''))
        if date_str:
            dsp_data[dsp]['dates'].add(date_str)
        
        dsp_data[dsp]['total_routes'] += 1
        
        compliant = str(row.get('compliant', '1')).strip()
        if compliant == '0':
            dsp_data[dsp]['non_compliant'] += 1
            swap_type = str(row.get('not_compliant_type', '') or '').strip()
            if swap_type:
                dsp_data[dsp]['swap_types'][swap_type] += 1
    
    if not dsp_data:
        raise ValueError("No data found. Check the file contains a 'dsp' column.")
    
    messages = {}
    for dsp in sorted(dsp_data.keys()):
        data = dsp_data[dsp]
        
        total = data['total_routes']
        non_comp = data['non_compliant']
        compliant_count = total - non_comp
        compliance_pct = (compliant_count / total * 100) if total > 0 else 100
        
        date_range = format_date_range(list(data['dates']))
        
        # Swap type summary (counts only)
        swap_lines = []
        for swap_type, count in sorted(data['swap_types'].items(), key=lambda x: -x[1]):
            swap_display = swap_type.replace('plan:', '').replace('execute:', '→').replace(' ', ' ').title()
            swap_lines.append(f'  • {swap_display}: {count}')
        
        swap_summary = '\n'.join(swap_lines) if swap_lines else '  • No swaps recorded'
        
        status = '✅ Compliant' if compliance_pct >= 95 else '⚠️ Below target'
        
        content = (
            f'🚚 Fleet STC Summary — {dsp}\n'
            f'Period: {date_range}\n\n'
            f'Fleet Compliance: {compliance_pct:.1f}% {status}\n'
            f'({compliant_count}/{total} routes compliant)\n\n'
            f'Non-compliant breakdown:\n'
            f'{swap_summary}'
            f'{COMPLIANT_FOOTER}'
        )
        messages[dsp] = wrap_message(content)
    
    return messages


# ============================================================================
# CONTACT COMPLIANCE (CC) — COMPLIANT v2.0
# ============================================================================

def generate_cc_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    COMPLIANT v2.0: DSP-level contact compliance percentage.
    
    Output: "Your DSP contact compliance: X%"
    
    NO DA IDs, NO tracking IDs, NO individual delivery detail.
    """
    dsp_data = defaultdict(lambda: {
        'total': 0,
        'with_call': 0,
        'with_text': 0,
        'by_reason': defaultdict(int),
        'dates': set()
    })
    
    for row in _open_csv(file_bytes):
        # Skip NOA rows
        if str(row.get('chat_reason_code', '') or '').strip() == 'NOTIFY_OF_ARRIVAL':
            continue
        
        dsp = str(row.get('DSP', '') or '').strip()
        if not dsp:
            continue
        
        date_str = fmt_date(row.get('Event Date', ''))
        if date_str:
            dsp_data[dsp]['dates'].add(date_str)
        
        dsp_data[dsp]['total'] += 1
        
        call_event = str(row.get('Call Event', '') or '').strip()
        text_event = str(row.get('Text Event', '') or '').strip()
        reason = str(row.get('Shipment Reason', '') or '').strip()
        
        if call_event and call_event != '-':
            dsp_data[dsp]['with_call'] += 1
        if text_event and text_event != '-':
            dsp_data[dsp]['with_text'] += 1
        if reason:
            dsp_data[dsp]['by_reason'][reason] += 1
    
    if not dsp_data:
        raise ValueError("No CC rows found. Check the correct Exceptions CSV is selected.")
    
    messages = {}
    for dsp in sorted(dsp_data.keys()):
        data = dsp_data[dsp]
        
        total = data['total']
        with_call = data['with_call']
        with_text = data['with_text']
        
        call_pct = (with_call / total * 100) if total > 0 else 0
        text_pct = (with_text / total * 100) if total > 0 else 0
        
        date_range = format_date_range(list(data['dates']))
        
        # Reason breakdown (counts only)
        reason_lines = []
        for reason, count in sorted(data['by_reason'].items(), key=lambda x: -x[1])[:5]:
            reason_lines.append(f'  • {reason}: {count}')
        
        reason_summary = '\n'.join(reason_lines) if reason_lines else '  • No breakdown available'
        
        content = (
            f'📞 Contact Compliance Summary — {dsp}\n'
            f'Period: {date_range}\n\n'
            f'Total Exceptions: {total}\n'
            f'Call Attempts: {with_call} ({call_pct:.0f}%)\n'
            f'Text Attempts: {with_text} ({text_pct:.0f}%)\n\n'
            f'Top reasons:\n'
            f'{reason_summary}'
            f'{COMPLIANT_FOOTER}'
        )
        messages[dsp] = wrap_message(content)
    
    return messages


# ============================================================================
# PICTURE ON DELIVERY (POD) — COMPLIANT v2.0
# ============================================================================

def generate_pod_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    COMPLIANT v2.0: DSP-level POD compliance percentage.
    
    Output: "Your DSP POD compliance: X%"
    
    NO DA IDs, NO tracking IDs, NO individual delivery detail.
    """
    dsp_data = defaultdict(lambda: {
        'total_deliveries': 0,
        'pod_opportunities': 0,
        'by_reason': defaultdict(int),
        'dates': set()
    })
    
    for row in _open_csv(file_bytes):
        # Skip manual bypasses
        if str(row.get('is_bypassed', '') or '').strip().upper() == 'Y':
            continue
        
        dsp = str(row.get('DSP', '') or '').strip()
        if not dsp:
            continue
        
        date_str = fmt_date(row.get('event_date', ''))
        if date_str:
            dsp_data[dsp]['dates'].add(date_str)
        
        dsp_data[dsp]['pod_opportunities'] += 1
        
        ship_reason = str(row.get('shipment_reason', '') or '').strip()
        if ship_reason:
            dsp_data[dsp]['by_reason'][ship_reason] += 1
    
    if not dsp_data:
        raise ValueError("No POD data found. Check the correct POD Summary CSV is selected.")
    
    messages = {}
    for dsp in sorted(dsp_data.keys()):
        data = dsp_data[dsp]
        
        opportunities = data['pod_opportunities']
        date_range = format_date_range(list(data['dates']))
        
        # Reason breakdown
        reason_lines = []
        for reason, count in sorted(data['by_reason'].items(), key=lambda x: -x[1])[:5]:
            reason_lines.append(f'  • {reason}: {count}')
        
        reason_summary = '\n'.join(reason_lines) if reason_lines else '  • No breakdown available'
        
        content = (
            f'📷 POD Summary — {dsp}\n'
            f'Period: {date_range}\n\n'
            f'POD Opportunities: {opportunities}\n\n'
            f'By shipment reason:\n'
            f'{reason_summary}'
            f'{COMPLIANT_FOOTER}'
        )
        messages[dsp] = wrap_message(content)
    
    return messages


# ============================================================================
# NOTIFY OF ARRIVAL (NOA) — COMPLIANT v2.0
# ============================================================================

def generate_noa_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    COMPLIANT v2.0: DSP-level NOA event count.
    
    Output: "Your DSP had X NOA events"
    
    NO DA IDs, NO tracking IDs, NO action requests.
    """
    dsp_counts = defaultdict(int)
    dates_seen = set()
    
    for row in _open_csv(file_bytes):
        if str(row.get('chat_reason_code', '') or '').strip() != 'NOTIFY_OF_ARRIVAL':
            continue
        
        dsp = str(row.get('DSP', '') or '').strip()
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
    
    messages = {}
    for dsp in sorted(dsp_counts.keys()):
        count = dsp_counts[dsp]
        
        content = (
            f'🔔 NOA Summary — {dsp}\n'
            f'Period: {date_range}\n\n'
            f'Total NOA Events: {count}\n\n'
            f'Notify of Arrival supports OTR safety, concessions, '
            f'customer contact, and overall delivery experience.'
            f'{COMPLIANT_FOOTER}'
        )
        messages[dsp] = wrap_message(content)
    
    return messages


# ============================================================================
# UNRETURNED BAGS — COMPLIANT v2.0
# ============================================================================

def generate_bags_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    COMPLIANT v2.0: DSP-level unreturned bag totals.
    
    Output: "Your DSP has X unreturned bags"
    
    NO route-level breakdown, NO route flags.
    """
    dsp_data = defaultdict(lambda: {
        'total_bags': 0,
        'oldest_date': None,
        'newest_date': None,
        'dates': set()
    })
    
    for row in _open_csv(file_bytes):
        dsp = str(row.get('DSP', '') or '').strip()
        bag = str(row.get('Bag', '') or '').strip()
        date_raw = str(row.get('Date', '') or '').strip()
        
        if not dsp or not bag:
            continue
        
        date_str = fmt_date(date_raw)
        if date_str:
            dsp_data[dsp]['dates'].add(date_str)
        
        dsp_data[dsp]['total_bags'] += 1
    
    if not dsp_data:
        raise ValueError("No bag data found. Check the file contains DSP, Bag and Date columns.")
    
    messages = {}
    for dsp in sorted(dsp_data.keys()):
        data = dsp_data[dsp]
        
        total = data['total_bags']
        if total == 0:
            continue
        
        # Calculate date range and oldest
        dates = sorted(data['dates'], key=parse_date)
        oldest = dates[0] if dates else 'Unknown'
        date_range = format_date_range(dates)
        
        # Calculate age of oldest
        age_note = ''
        if dates:
            try:
                oldest_dt = datetime.strptime(dates[0], '%d/%m/%Y')
                days_old = (datetime.now() - oldest_dt).days
                age_note = f' (oldest: {days_old} day{"s" if days_old != 1 else ""} ago)'
            except:
                pass
        
        content = (
            f'👜 Unreturned Bags Summary — {dsp}\n'
            f'Period: {date_range}\n\n'
            f'Total Unreturned Bags: {total}{age_note}'
            f'{COMPLIANT_FOOTER}'
        )
        messages[dsp] = wrap_message(content)
    
    return messages


# ============================================================================
# CARRIER INVESTIGATIONS — COMPLIANT v2.0
# ============================================================================

def generate_carrier_inv_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    COMPLIANT v2.0: DSP-level investigation totals.
    
    Output: "Your DSP has X open DNR investigations"
    
    NO package detail, NO tracking IDs.
    """
    dsp_data = {}
    
    for row in _open_csv(file_bytes):
        dsp = str(row.get('dsp_shortcode', '') or '').strip()
        if not dsp:
            continue
        
        try:
            investigations = int(float(row.get('Carrier Investigations', 0) or 0))
        except:
            investigations = 0
        
        try:
            responses = int(float(row.get('DSP Responses', 0) or 0))
        except:
            responses = 0
        
        if investigations > 0:
            dsp_data[dsp] = {
                'investigations': investigations,
                'responses': responses,
            }
    
    if not dsp_data:
        raise ValueError("No carrier investigation data found.")
    
    messages = {}
    for dsp in sorted(dsp_data.keys()):
        data = dsp_data[dsp]
        inv = data['investigations']
    messages = {}
    for dsp in sorted(dsp_data.keys()):
        data = dsp_data[dsp]
        inv = data['investigations']
        resp = data['responses']
        prevention_rate = (resp / inv * 100) if inv > 0 else 0
        
        content = (
            f'🕵️ DNR Investigation Summary — {dsp}\n'
            f'Week to Date\n\n'
            f'Open Investigations: {inv}\n'
            f'DSP Responses: {resp}\n'
            f'% Prevention (Response): {prevention_rate:.0f}%'
            f'{COMPLIANT_FOOTER}'
        )
        messages[dsp] = wrap_message(content)
    
    return messages


# ============================================================================
# VSA — COMPLIANT v2.0
# ============================================================================

def generate_vsa_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    COMPLIANT v2.0: DSP-level vehicle pending count.
    
    Output: "Your DSP has X vehicles pending VSA inspection"
    
    NO VINs, NO VRNs, NO vehicle-level detail.
    """
    dsp_counts = defaultdict(int)
    
    for row in _open_csv(file_bytes):
        inspection = str(row.get('inspection_passed', '') or '').strip().upper()
        if inspection != 'N':
            continue
        
        dsp = str(row.get('dsp', '') or '').strip().upper()
        if not dsp:
            continue
        
        dsp_counts[dsp] += 1
    
    if not dsp_counts:
        raise ValueError("No vehicles pending VSA found (inspection_passed = 'N').")
    
    messages = {}
    for dsp in sorted(dsp_counts.keys()):
        count = dsp_counts[dsp]
        
        content = (
            f'🛡️ VSA Summary — {dsp}\n'
            f'Current Cycle\n\n'
            f'Vehicles Pending Inspection: {count}\n\n'
            f'Vehicle Safety Audits are part of the bi-weekly inspection cycle.'
            f'{COMPLIANT_FOOTER}'
        )
        messages[dsp] = wrap_message(content)
    
    return messages


# ============================================================================
# TRACER BRIDGE — COMPLIANT v2.0
# ============================================================================

# DSP Name mapping (full name -> short code)
DSP_NAME_MAP = {
    'hero parcel logistics limited': 'HPLM',
    'deliverwize ltd': 'DELL',
    'dtt deliveries ltd': 'DTTD',
    'dtt deliveries ltd ': 'DTTD',
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
    if not dsp_name:
        return 'UNKNOWN'
    normalized = dsp_name.strip().lower()
    return DSP_NAME_MAP.get(normalized, dsp_name.strip().upper()[:4])


def generate_tracer_bridge_messages(not_recovered_bytes: bytes, search_bytes: bytes,
                                     bulk_history_bytes: bytes = None,
                                     safe_mode: bool = False) -> dict:
    """
    COMPLIANT v2.0: DSP-level not recovered totals.
    
    Output: "Your DSP has X not recovered packages"
    
    NO tracking IDs, NO package-level detail.
    """
    # Build DSP lookup from SearchResults
    dsp_lookup = {}
    for row in _open_csv(search_bytes):
        tid = str(row.get('Tracking ID', '') or '').strip()
        dsp_full = str(row.get('DSP Name', '') or '').strip()
        if tid and dsp_full:
            dsp_lookup[tid] = _normalize_dsp_name(dsp_full)
    
    # Parse bulk history for returns
    returns = set()
    if bulk_history_bytes:
        for row in _open_csv(bulk_history_bytes):
            tid = row.get('Tracking ID', '').strip()
            reason = row.get('Reason', '').strip()
            if reason == 'WRONG_CYCLE_INDUCT' and tid:
                returns.add(tid)
    
    # Parse Not Recovered file
    dsp_data = defaultdict(lambda: {
        'not_recovered': 0,
        'returned': 0,
        'total_value': 0.0,
        'by_reason': defaultdict(int)
    })
    station = ''
    
    for row in _open_csv(not_recovered_bytes):
        tid = str(row.get('TrackingID', '') or '').strip()
        reason = str(row.get('reason_before_missing', '') or '').strip() or 'NO_REASON'
        
        if not station:
            station = str(row.get('parent_location', '') or '').strip()
        
        try:
            value = float(row.get('Shipment Value', 0) or 0)
        except:
            value = 0.0
        
        dsp = dsp_lookup.get(tid, 'UNKNOWN')
        
        if tid in returns:
            dsp_data[dsp]['returned'] += 1
        else:
            dsp_data[dsp]['not_recovered'] += 1
            dsp_data[dsp]['total_value'] += value
            dsp_data[dsp]['by_reason'][reason] += 1
    
    if not dsp_data:
        raise ValueError("No data found. Check the Not Recovered and SearchResults files.")
    
    # Generate per-DSP messages
    messages = {}
    now = datetime.now()
    week_num = now.isocalendar()[1]
    
    for dsp in sorted(dsp_data.keys()):
        if dsp == 'UNKNOWN':
            continue
        
        data = dsp_data[dsp]
        not_rec = data['not_recovered']
        returned = data['returned']
        value = data['total_value']
        
        if not_rec == 0 and returned == 0:
            continue
        
        # Reason breakdown (counts only)
        reason_lines = []
        for reason, count in sorted(data['by_reason'].items(), key=lambda x: -x[1])[:5]:
            reason_display = reason.replace('_', ' ').title()
            reason_lines.append(f'  • {reason_display}: {count}')
        
        reason_summary = '\n'.join(reason_lines) if reason_lines else '  • No breakdown available'
        
        content = (
            f'📊 Tracer Summary — {dsp}\n'
            f'Week {week_num} | {now.strftime("%d/%m/%Y")}\n\n'
            f'Not Recovered: {not_rec} package(s) (£{value:,.2f})\n'
            f'Returned to Station: {returned} package(s)\n\n'
            f'By reason:\n'
            f'{reason_summary}'
            f'{COMPLIANT_FOOTER}'
        )
        messages[dsp] = wrap_message(content)
    
    return messages


# ============================================================================
# REMOVED TOOLS — NO COMPLIANT PATH
# ============================================================================

def generate_nursery_overuse_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    ❌ REMOVED — NO COMPLIANT PATH
    
    This tool directed DSP DA deployment decisions, which violates the
    On-Road DSP Collaboration SOP. DSPs manage their own employees.
    
    Raises an error explaining why this tool is no longer available.
    """
    raise ValueError(
        "🚫 Nursery Overuse tool has been removed.\n\n"
        "This tool is not compliant with the On-Road DSP Collaboration SOP (Week 21).\n"
        "Reason: Directs DSP decisions regarding DA deployment.\n\n"
        "DSPs are independent business owners who manage their own employees.\n"
        "OPS cannot direct which DAs are assigned to which route types."
    )


def generate_ridealong_overuse_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    ❌ REMOVED — NO COMPLIANT PATH
    
    This tool directed DSP staffing decisions, which violates the
    On-Road DSP Collaboration SOP. DSPs manage their own employees.
    
    Raises an error explaining why this tool is no longer available.
    """
    raise ValueError(
        "🚫 Ridealong Overuse tool has been removed.\n\n"
        "This tool is not compliant with the On-Road DSP Collaboration SOP (Week 21).\n"
        "Reason: Directs DSP staffing and DA deployment decisions.\n\n"
        "DSPs are independent business owners who manage their own employees.\n"
        "OPS cannot flag or direct how DSPs staff their routes."
    )

