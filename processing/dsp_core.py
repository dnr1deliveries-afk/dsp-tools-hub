"""
DSP Tools Hub — Processing Engine v2.1
=======================================
Compliant with On-Road DSP Collaboration SOP (Week 21)

COMPLIANCE PRINCIPLES:
1. DSP-level data ONLY — no route-level, no DA/TRID-level
2. Informational language ONLY — no action requests
3. Support-driven — data shared for DSP awareness, not OPS direction

REMOVED TOOLS (no compliant path):
- Nursery Overuse — directs DA deployment decisions
- Ridealong Overuse — directs DA deployment decisions

REMOVED TOOLS (Oct 2026 — scope reduction):
- Rostering — removed
- STC — removed
- Contact Compliance — removed
- POD — removed
- NOA — removed
- Tracer Bridge — removed

ACTIVE TOOLS:
- Chase: D-1 to D-4 outstanding shipments per DSP
- Pickups: Awaiting pickup per DSP with shipment ID detail (v2.1)
- Bags: Unreturned bags per DSP
- Carrier Inv: Total investigations per DSP
- VSA: Vehicles pending inspection per DSP with VIN/VRN detail
- Abortive Routes: Cut routes per DSP with cycle/service type/depart time
- Siphon Review: FRT miss flags per DSP per cycle

v2.1 - Scope reduction (Oct 2026)
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

def generate_chase_messages(d1_bytes: bytes, d2_bytes: bytes = None,
                            d3_bytes: bytes = None, d4_bytes: bytes = None,
                            station: str = None, safe_mode: bool = False) -> tuple:
    """
    DSP Chase — D-1 to D-4 not-returned shipments per DSP (v3.1, 2026-09-22).

    Replaces the legacy Scrub Error + SearchResults join. Each day's file
    now uses the LastMilePickupInformationV2 export schema and already
    carries the route code, so no separate SearchResults lookup is needed.

    v3.1: Files are now downloaded UK-wide (all sites) from Mercury —
    NOT pre-filtered to a single station. Rows are filtered here to the
    signed-in station's SITE code before grouping by DSP, so the user
    doesn't need to filter/export a station-specific file themselves.

    No bucket split (Driver Marked Missing removed 2026-09-22) — every
    outstanding row for the day is listed with its
    LastMilePickupInformationV2.InternalStatusCode shown as a 3rd column.

    Inputs:
        d1_bytes : D-1 not-returned CSV (required)
        d2_bytes : D-2 not-returned CSV (optional)
        d3_bytes : D-3 not-returned CSV (optional)
        d4_bytes : D-4 not-returned CSV (optional)
        station  : station code to filter SITE column to (e.g. 'DNR1').
                   If blank/None, no site filtering is applied (all sites kept).

        Expected columns per file: trackingId, SITE, RoutePlan.routeCode,
        LastMilePickupInformationV2.InternalStatusCode,
        LMTRUpdates.PTRAS.dspShortCode

    Returns: (messages_dict, {})
        - messages_dict: {dsp: message_text}
        - empty dict (second value kept for backwards-compatible call signature)
    """
    day_files   = [('D-1', d1_bytes), ('D-2', d2_bytes), ('D-3', d3_bytes), ('D-4', d4_bytes)]
    station_up  = (station or '').strip().upper()

    # dsp -> day_label -> list of {tid, route, status}
    dsp_data = defaultdict(lambda: defaultdict(list))

    for day_label, file_bytes in day_files:
        if not file_bytes:
            continue
        for row in _open_csv(file_bytes):
            site = (row.get('SITE') or '').strip().upper()
            if station_up and site != station_up:
                continue

            dsp = (row.get('LMTRUpdates.PTRAS.dspShortCode') or '').strip().upper()
            tid = (row.get('trackingId') or '').strip()

            if not (dsp and tid):
                continue

            route  = (row.get('RoutePlan.routeCode') or '').strip()
            status = (row.get('LastMilePickupInformationV2.InternalStatusCode') or '').strip().upper() or '—'
            dsp_data[dsp][day_label].append({'tid': tid, 'route': route, 'status': status})

    today    = datetime.now().strftime('%d/%m/%Y')
    messages = {}

    def sort_key(item):
        if item['route']:
            nums = re.findall(r'\d+', item['route'])
            return (int(nums[-1]) if nums else 9999, item['tid'])
        return (9999, item['tid'])

    def build_day_table(items):
        items = sorted(items, key=sort_key)
        tid_w    = max([len(i['tid']) for i in items] + [len('Tracking ID')])
        route_w  = max([len(i['route']) for i in items] + [len('Route')])
        status_w = max([len(i['status']) for i in items] + [len('Status')])

        lines = [
            '```',
            f"| {'Tracking ID':<{tid_w}} | {'Route':<{route_w}} | {'Status':<{status_w}} |",
        ]
        for i in items:
            lines.append(
                f"| {i['tid']:<{tid_w}} | {i['route']:<{route_w}} | {i['status']:<{status_w}} |"
            )
        lines.append('```')
        return '\n'.join(lines)

    for dsp in sorted(dsp_data.keys()):
        day_sections = []
        for day_label, _ in day_files:
            items = dsp_data[dsp].get(day_label, [])
            if not items:
                continue
            day_sections.append(f'{day_label} ({len(items)}):\n{build_day_table(items)}')

        if not day_sections:
            continue

        content = (
            f'Outstanding Shipments \u2014 {dsp}\n'
            f'Updated: {today}\n\n'
            f'Good morning. Please see below for any shipments not yet returned to station.\n\n'
            + '\n\n'.join(day_sections)
            + '\n\nAppreciate your support.'
        )
        messages[dsp] = content

    return messages, {}



# ============================================================================
# DSP PICKUPS — v2.1 (shipment ID detail added per DSP request 2026-08-13)
# ============================================================================

def generate_pickup_messages(pickup_bytes: bytes, search_bytes: bytes = None,
                              safe_mode: bool = False) -> tuple:
    """
    v2.1: DSP-level totals WITH per-shipment tracking ID detail, grouped
    by pickup type.

    Shipment IDs are package-level identifiers (not DA/personnel-level),
    consistent with the tracking ID listing already used in the Chase
    tool and the VIN/VRN vehicle-level detail restored in VSA v2.1
    (PoE policy update 2026-07-12). NO route lookup, NO DA-level data.

    Output per DSP:
        "Your DSP has X packages awaiting pickup"
        followed by shipment IDs listed under each pickup-type category.
    """
    dsp_counts = defaultdict(lambda: {'total': 0, 'by_type': defaultdict(list)})
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
        dsp_counts[dsp]['by_type'][category].append(tid)

    if not pickup_date:
        pickup_date = datetime.now().strftime('%d/%m/%Y')

    messages = {}
    for dsp in sorted(dsp_counts.keys()):
        data = dsp_counts[dsp]
        total = data['total']

        if total == 0:
            continue

        # Build type breakdown with shipment IDs nested under each category
        type_sections = []
        for ptype, tids in sorted(data['by_type'].items(), key=lambda x: -len(x[1])):
            tid_lines = '\n'.join(f'      {tid}' for tid in tids)
            type_sections.append(f'  \u2022 {ptype} ({len(tids)}):\n{tid_lines}')

        type_summary = '\n'.join(type_sections) if type_sections else '  • No breakdown available'

        content = (
            f'📬 Pickup Summary — {dsp}\n'
            f'Date: {pickup_date}\n\n'
            f'Your DSP has {total} package(s) awaiting pickup.\n\n'
            f'Breakdown by type (shipment IDs):\n'
            f'{type_summary}'
            f'{COMPLIANT_FOOTER}'
        )
        messages[dsp] = wrap_message(content)

    return messages, pickup_date

def generate_bags_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    v2.2: Bags grouped by date, then route, then individual bag IDs.

    Output per DSP: header → total → per-day blocks → per-route with bag IDs.
    """
    # dsp -> date_str -> route_code -> list of bag IDs
    dsp_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for row in _open_csv(file_bytes):
        dsp = str(row.get('DSP', '') or '').strip()
        bag = str(row.get('Bag', '') or '').strip()
        route = str(row.get('Route Code', '') or '').strip()
        date_raw = str(row.get('Date', '') or '').strip()

        if not dsp or not bag:
            continue

        date_str = fmt_date(date_raw) or 'Unknown'
        dsp_data[dsp][date_str][route].append(bag)

    if not dsp_data:
        raise ValueError("No bag data found. Check the file contains DSP, Route Code, Bag and Date columns.")

    messages = {}
    for dsp in sorted(dsp_data.keys()):
        days = dsp_data[dsp]
        total = sum(
            len(bags)
            for routes in days.values()
            for bags in routes.values()
        )
        if total == 0:
            continue

        all_dates = sorted([d for d in days.keys() if d != 'Unknown'], key=parse_date)
        if 'Unknown' in days:
            all_dates.append('Unknown')

        date_range = format_date_range([d for d in all_dates if d != 'Unknown'])

        age_note = ''
        if all_dates and all_dates[0] != 'Unknown':
            try:
                oldest_dt = datetime.strptime(all_dates[0], '%d/%m/%Y')
                days_old = (datetime.now() - oldest_dt).days
                age_note = f' (oldest: {days_old} day{"s" if days_old != 1 else ""} ago)'
            except:
                pass

        lines = [
            f'\U0001f9fa Unreturned Bags \u2014 {dsp}',
            f'Period: {date_range}',
            f'Total Unreturned: {total}{age_note}',
        ]

        for date_str in all_dates:
            routes = days[date_str]
            day_total = sum(len(b) for b in routes.values())
            lines.append('')
            lines.append(f'\U0001f4c5 {date_str} \u2014 {day_total} bag{"s" if day_total != 1 else ""}')
            for route in sorted(routes.keys()):
                bag_list = routes[route]
                lines.append(f'({route}) \u2014 {len(bag_list)} bag{"s" if len(bag_list) != 1 else ""}')
                for b in bag_list:
                    lines.append(f'  \u2022 {b}')

        content = '\n'.join(lines) + COMPLIANT_FOOTER
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
# VSA - v2.1 (VIN/VRN detail restored per PoE policy update 2026-07-12)
# ============================================================================

def generate_vsa_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    v2.1: DSP-level vehicle pending count WITH per-vehicle VIN/VRN detail.

    Policy note (2026-07-12): PoE (Principles of Engagement) rules were
    updated to permit sharing VIN/VRN vehicle-level detail with DSPs for
    VSA pending-inspection tracking. This reverses the v2.0 "COMPLIANT"
    rebuild (Week 21) restriction, which had stripped VIN/VRN from this
    message deliberately under the prior PoE reading. If PoE guidance
    changes again, re-apply the count-only restriction (see git history
    around Week 21 for the previous implementation).

    Output per DSP:
        "VSA Summary - {DSP}
         Vehicles Pending Inspection: {count}
         VIN: ...  VRN: ...
         VIN: ...  VRN: ...
         ..."
    """
    dsp_vehicles = defaultdict(list)

    for row in _open_csv(file_bytes):
        # Authoritative pending signal: inspection_date is blank => no audit
        # conducted yet on this vehicle (confirmed against real export
        # 2026-07-12: inspection_date blank correlates 1:1 with
        # inspection_passed == 'N' in current data, but inspection_date is
        # the field that actually carries the "has this been audited" fact).
        inspection_date = str(row.get('inspection_date', '') or '').strip().strip('"')
        if inspection_date:
            continue  # audit already conducted on this vehicle

        dsp = str(row.get('dsp', '') or '').strip().upper()
        if not dsp:
            continue

        vin = str(row.get('vin', '') or '').strip().upper()
        # Real exports use 'vrns'; also accept 'vrn' as a fallback variant.
        vrn = str(row.get('vrns', '') or row.get('vrn', '') or '').strip().upper()

        dsp_vehicles[dsp].append({'vin': vin or '-', 'vrn': vrn or '-'})

    if not dsp_vehicles:
        raise ValueError("No vehicles pending VSA found (inspection_date is blank for none of the rows).")

    messages = {}
    for dsp in sorted(dsp_vehicles.keys()):
        vehicles = dsp_vehicles[dsp]
        count = len(vehicles)

        vehicle_lines = '\n'.join(
            f'  \u2022 VIN: {v["vin"]}   VRN: {v["vrn"]}' for v in vehicles
        )

        content = (
            f'\U0001F6E1\uFE0F VSA Summary - {dsp}\n'
            f'Current Cycle\n\n'
            f'Vehicles Pending Inspection: {count}\n\n'
            f'{vehicle_lines}\n\n'
            f'Vehicle Safety Audits are part of the bi-weekly inspection cycle.'
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



# ============================================================================
# ABORTIVE ROUTES
# ============================================================================

def _open_delimited(file_bytes: bytes):
    """Open a CSV or TSV export, auto-detecting the delimiter."""
    text = file_bytes.decode('utf-8-sig')
    sample = text[:2000]
    delimiter = '\t' if sample.count('\t') >= sample.count(',') else ','
    return csv.DictReader(io.StringIO(text), delimiter=delimiter)


def generate_abortive_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Abortive Routes — per-DSP breakdown of routes aborted/cut today.

    Input columns: Day, Month, Year, Service Type, Cycle, Route Length,
                    DSP, Cuts, Depart Time

    Returns: {dsp: message_text}
    """
    reader = _open_delimited(file_bytes)

    by_dsp = defaultdict(list)
    date_label = ''

    for row in reader:
        dsp = (row.get('DSP') or '').strip().upper()
        if not dsp:
            continue

        day   = (row.get('Day') or '').strip()
        month = (row.get('Month') or '').strip()
        year  = (row.get('Year') or '').strip()
        if day and month and year and not date_label:
            date_label = f'{day} {month} {year}'

        by_dsp[dsp].append({
            'service_type': (row.get('Service Type') or '').strip(),
            'cycle':        (row.get('Cycle') or '').strip(),
            'length':       (row.get('Route Length') or '').strip(),
            'depart':       (row.get('Depart Time') or '').strip(),
            'cuts':         (row.get('Cuts') or '1').strip(),
        })

    if not date_label:
        date_label = datetime.now().strftime('%d %B %Y')

    messages = {}
    for dsp, routes in by_dsp.items():
        routes.sort(key=lambda r: r['depart'])

        total_cuts = 0
        for r in routes:
            try:
                total_cuts += int(float(r['cuts'] or 1))
            except ValueError:
                total_cuts += 1

        # Dynamic column widths -- sized to actual content (and header
        # labels) so long Service Type strings (e.g. LEV variants) never
        # break alignment. Route Length dropped from the table per
        # 2026-09-15 format change.
        cycle_w  = max([len(r['cycle']) for r in routes] + [len('Cycle')])
        svc_w    = max([len(r['service_type']) for r in routes] + [len('Service Type')])
        depart_w = max([len(r['depart']) for r in routes] + [len('Depart Time')])
        cuts_w   = max([len(str(r['cuts'])) for r in routes] + [len('Cuts')])

        lines = [
            'Good morning',
            '',
            'Please see the below abortive routes for today:',
            '',
            '```',
            f"| {'Cycle':<{cycle_w}} | {'Service Type':<{svc_w}} | {'Depart Time':<{depart_w}} | {'Cuts':<{cuts_w}} |",
        ]
        for r in routes:
            lines.append(
                f"| {r['cycle']:<{cycle_w}} | {r['service_type']:<{svc_w}} | "
                f"{r['depart']:<{depart_w}} | {r['cuts']:<{cuts_w}} |"
            )
        lines.append('```')
        lines.append('')
        lines.append(f"Total abortive routes: {total_cuts}")
        lines.append('')
        lines.append('Appreciate your support.')

        messages[dsp] = '\n'.join(lines)

    return messages



# ============================================================================
# SIPHON REVIEW — FRT capacity gap alerts to MDO channels
# ============================================================================

DAY_NAMES = {
    0: 'Monday',
    1: 'Tuesday',
    2: 'Wednesday',
    3: 'Thursday',
    4: 'Friday',
    5: 'Saturday',
    6: 'Sunday',
}


def generate_siphon_review_messages(file_bytes: bytes, safe_mode: bool = False) -> dict:
    """
    Siphon Review — flag DSPs under FRT (Flex-up Route Target) per day.

    Input CSV columns: Week, Station, Cycle, DSP, Date, SUI, Okami, FRT,
                       Delta_Okami, Delta_FRT, Meets_Okami, Meets_FRT, Service_Types

    Logic:
        - Rows where Meets_FRT == 'No' are flagged
        - Grouped per DSP → per Week → per Cycle (separate sections)
        - Delta_FRT is negative when under target; abs(Delta_FRT) = routes short
        - Cycles with zero flags for a DSP are omitted
        - Rows where FRT == 0 (empty cycles) are skipped entirely

    Returns: {dsp: message_text}
    """
    reader = _open_csv(file_bytes)

    # Structure: {dsp: {week: {cycle: [(date_obj, day_name, date_str, routes_under)]}}}
    flags = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    warnings = []

    for row in reader:
        dsp = (row.get('DSP') or '').strip().upper()
        if not dsp:
            continue

        meets_frt = (row.get('Meets_FRT') or '').strip()
        if meets_frt != 'No':
            continue

        # Skip zero-FRT rows (empty cycle placeholders)
        try:
            frt_val = int(float(row.get('FRT') or '0'))
        except ValueError:
            frt_val = 0
        if frt_val == 0:
            continue

        week  = (row.get('Week') or '').strip()
        cycle = (row.get('Cycle') or '').strip()
        date_str = (row.get('Date') or '').strip()

        try:
            delta = int(float(row.get('Delta_FRT') or '0'))
        except ValueError:
            delta = 0
        routes_under = abs(delta)

        # Parse date for day name and display format
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            day_name = DAY_NAMES[date_obj.weekday()]
            display_date = date_obj.strftime('%d/%m')
        except (ValueError, KeyError):
            day_name = ''
            display_date = date_str
            date_obj = datetime.min

        flags[dsp][week][cycle].append((date_obj, day_name, display_date, routes_under))

    if not flags:
        return {}

    # Build one message per DSP
    messages = {}
    for dsp in sorted(flags.keys()):
        weeks = flags[dsp]
        sections = []

        for week in sorted(weeks.keys()):
            cycles = weeks[week]
            cycle_sections = []

            for cycle in sorted(cycles.keys()):
                days = cycles[cycle]
                if not days:
                    continue

                # Sort by date
                days.sort(key=lambda d: d[0])

                # Compute alignment padding for day names
                max_day_len = max(len(d[1]) for d in days)

                day_lines = []
                for date_obj, day_name, display_date, routes_under in days:
                    route_word = 'route' if routes_under == 1 else 'routes'
                    padded_day = f"{day_name},{' ' * (max_day_len - len(day_name))}"
                    day_lines.append(
                        f"{padded_day}  {display_date}:  {routes_under} {route_word} under target"
                    )

                cycle_block = f"*{cycle}*\n" + '\n'.join(day_lines)
                cycle_sections.append(cycle_block)

            if cycle_sections:
                week_block = f"*{week}*\n\n" + '\n\n'.join(cycle_sections)
                sections.append(week_block)

        if not sections:
            continue

        body = '\n\n'.join(sections)

        msg = (
            f"Hi,\n"
            f"\n"
            f"We are currently reviewing OTR capacity and your ECP capacity "
            f"looks under FRT (flex up target) on the following days:\n"
            f"\n"
            f"{body}\n"
            f"\n"
            f"Could you please review as soon as possible and let me know "
            f"if you can increase to match your FRT target please?"
        )

        messages[dsp] = msg

    return messages
