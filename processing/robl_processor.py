"""
ROBL PvA Offset Analysis — Internal Use Only
============================================
Analyses ROBL offset data for station management.
NOT shared with DSPs — no Slack send functionality.

Expects an export covering the CURRENT week + W+1 (next week), e.g.
[EU]_ROBL_PvA_Offset_*.csv. The analysis anchors on "today" (or an
optional reference_date) to split the file into:
    - Current Week snapshot (latest known day in the current ISO week)
    - W+1 Preview snapshot   (latest known day in the next ISO week)
    - Week-on-Week Changes   (Current Week -> W+1, full list, no truncation)

Any rows outside those two ISO weeks (e.g. stray prior-week rows) are
excluded from the analysis but reported in the summary for transparency.
"""

import pandas as pd
from io import StringIO


def _iso_week(ts: pd.Timestamp):
    iso = ts.isocalendar()
    return iso.year, iso.week


def _snapshot(week_df: pd.DataFrame) -> pd.DataFrame:
    """Latest-date value per DSP/type within a single week's slice."""
    if week_df.empty:
        return pd.DataFrame(columns=['_offset', '_final_input', '_total_reduction', 'Modified By', 'reason_change'])
    latest = week_df['OFD'].max()
    snap = week_df[week_df['OFD'] == latest]
    return snap.groupby(['DSP', '_type']).agg({
        '_offset': 'max',
        '_final_input': 'min',
        '_total_reduction': 'max',
        'Modified By': 'first',
        'reason_change': 'first',
    })


def _week_label(week_df: pd.DataFrame) -> str:
    if week_df.empty:
        return 'No data'
    lo = week_df['OFD'].min().strftime('%d %b')
    hi = week_df['OFD'].max().strftime('%d %b')
    return f"{lo} \u2192 {hi}"


def generate_robl_analysis(csv_content: str, reference_date=None) -> dict:
    """
    Process a ROBL PvA Offset CSV (current week + W+1) and return an
    analysis dict.

    Args:
        csv_content: raw CSV text (utf-8-sig export)
        reference_date: optional date/str to anchor "today" for testing;
                         defaults to real current date.

    Returns dict with:
        - summary: overall stats + week date ranges
        - current_week: list of active-offset rows for the current week
        - next_week: list of active-offset rows (W+1 preview)
        - changes: full week-on-week change list (current -> W+1), no truncation
        - internal_only: True (no DSP send)
    """
    # Parse CSV
    df = pd.read_csv(StringIO(csv_content), encoding='utf-8-sig')

    # Map actual export headers -> internal names used below.
    # [EU]_ROBL_PvA_Offset_*.csv ships lowercase/snake_case headers that
    # don't match this module's original column names - rename here so the
    # rest of the function is untouched.
    df = df.rename(columns={
        'ofd_date':            'OFD',
        'service_type':        'Service Type',
        'company_code':        'DSP',
        'modified_by':         'Modified By',
        'Final Input Minutes': 'final_input_minutes',
        'Total Reduction':     'total_reduction',
    })

    if df.empty or 'OFD' not in df.columns:
        return {'error': 'No valid rows found in file'}

    df['OFD'] = pd.to_datetime(df['OFD'])
    df['_type'] = df['Service Type'].apply(
        lambda x: 'LEV' if 'LOW EMISSION' in str(x) else 'Standard'
    )
    df['_offset'] = pd.to_numeric(df['final_pva_robl_offset'], errors='coerce').fillna(0)
    df['_final_input'] = pd.to_numeric(df['final_input_minutes'], errors='coerce').fillna(0)
    df['_total_reduction'] = pd.to_numeric(df['total_reduction'], errors='coerce').fillna(0)

    ref = pd.Timestamp(reference_date) if reference_date else pd.Timestamp.now()
    cur_year, cur_week = _iso_week(ref)
    nxt_year, nxt_week = _iso_week(ref + pd.Timedelta(days=7))

    iso = df['OFD'].dt.isocalendar()
    cur_mask = (iso['year'] == cur_year) & (iso['week'] == cur_week)
    nxt_mask = (iso['year'] == nxt_year) & (iso['week'] == nxt_week)

    cur_df = df[cur_mask].copy()
    nxt_df = df[nxt_mask].copy()
    excluded_df = df[~(cur_mask | nxt_mask)].copy()

    if cur_df.empty and nxt_df.empty:
        return {'error': 'No rows found for the current week or W+1 in this file. '
                          'Upload a ROBL export covering current week + next week.'}

    cur_snap = _snapshot(cur_df)
    nxt_snap = _snapshot(nxt_df)

    def build_rows(snap: pd.DataFrame) -> list:
        rows = []
        for (dsp, dtype), row in snap.sort_values('_offset', ascending=False).iterrows():
            if row['_offset'] <= 0:
                continue
            rows.append({
                'dsp': dsp,
                'type': dtype,
                'offset': int(row['_offset']),
                'final_input': int(row['_final_input']),
                'total_reduction': int(row['_total_reduction']),
                'modified_by': row['Modified By'] if pd.notna(row['Modified By']) else '-',
                'reason': row['reason_change'] if pd.notna(row['reason_change']) else '-',
            })
        return rows

    current_week_rows = build_rows(cur_snap)
    next_week_rows = build_rows(nxt_snap)

    # Full week-on-week change list — current week -> W+1, no truncation
    all_keys = sorted(set(cur_snap.index.tolist()) | set(nxt_snap.index.tolist()))
    changes = []
    for dsp, dtype in all_keys:
        cur_val = int(cur_snap.loc[(dsp, dtype), '_offset']) if (dsp, dtype) in cur_snap.index else 0
        nxt_val = int(nxt_snap.loc[(dsp, dtype), '_offset']) if (dsp, dtype) in nxt_snap.index else 0
        if cur_val > 0 or nxt_val > 0:
            changes.append({
                'dsp': dsp,
                'type': dtype,
                'current_week': cur_val,
                'next_week': nxt_val,
                'change': nxt_val - cur_val,
            })
    changes.sort(key=lambda x: abs(x['change']), reverse=True)

    clear_dsps = sorted(set(df['DSP'].unique()) - {r['dsp'] for r in current_week_rows} - {r['dsp'] for r in next_week_rows})

    summary = {
        'current_week_range': _week_label(cur_df),
        'next_week_range': _week_label(nxt_df),
        'current_active_count': len(current_week_rows),
        'next_active_count': len(next_week_rows),
        'max_offset_current': current_week_rows[0]['offset'] if current_week_rows else 0,
        'max_offset_current_dsp': current_week_rows[0]['dsp'] if current_week_rows else '-',
        'max_offset_next': next_week_rows[0]['offset'] if next_week_rows else 0,
        'max_offset_next_dsp': next_week_rows[0]['dsp'] if next_week_rows else '-',
        'clear_count': len(clear_dsps),
        'clear_dsps': clear_dsps,
        'excluded_rows': int(len(excluded_df)),
        'excluded_range': _week_label(excluded_df) if not excluded_df.empty else None,
    }

    return {
        'summary': summary,
        'current_week': current_week_rows,
        'next_week': next_week_rows,
        'changes': changes,
        'internal_only': True,
    }


def _format_table(title: str, rows: list, value_key: str = 'offset') -> list:
    lines = [title]
    if not rows:
        lines.append('  (none)')
        return lines
    for r in rows:
        lines.append(f"  \u2022 {r['dsp']} ({r['type']}): {r[value_key]} min \u2192 {r['final_input']} final input "
                      f"[{r['modified_by']}]")
    return lines


def format_current_week_clipboard(result: dict) -> str:
    """Full current-week active offsets, plain text."""
    if 'error' in result:
        return f"Error: {result['error']}"
    s = result['summary']
    lines = [f"📊 Current Week Active Offsets \u2014 {s['current_week_range']}", '']
    lines += _format_table('Active Offsets:', result['current_week'])
    return '\n'.join(lines)


def format_next_week_clipboard(result: dict) -> str:
    """Full W+1 preview offsets, plain text."""
    if 'error' in result:
        return f"Error: {result['error']}"
    s = result['summary']
    lines = [f"📅 W+1 Preview \u2014 {s['next_week_range']}", '']
    lines += _format_table('Active Offsets:', result['next_week'])
    return '\n'.join(lines)


def format_changes_clipboard(result: dict) -> str:
    """Full week-on-week change list, plain text, no truncation."""
    if 'error' in result:
        return f"Error: {result['error']}"
    s = result['summary']
    lines = [f"🔁 Week-on-Week Changes — Current Week ({s['current_week_range']}) vs W+1 ({s['next_week_range']})", '']
    if not result['changes']:
        lines.append('  (no changes)')
    for c in result['changes']:
        arrow = '\u2b06\ufe0f' if c['change'] > 0 else '\u2b07\ufe0f' if c['change'] < 0 else '\u27a1\ufe0f'
        sign = '+' if c['change'] > 0 else ''
        lines.append(f"  \u2022 {c['dsp']} ({c['type']}): {c['current_week']} \u2192 {c['next_week']} "
                      f"({arrow} {sign}{c['change']})")
    return '\n'.join(lines)


def format_robl_clipboard(result: dict) -> str:
    """Full combined report — every section, nothing truncated. Single copy-all block."""
    if 'error' in result:
        return f"Error: {result['error']}"

    s = result['summary']
    lines = [
        '📊 ROBL PvA Offset Analysis \u2014 Internal Use Only',
        f"Current Week: {s['current_week_range']}  |  W+1: {s['next_week_range']}",
        '',
        f"Current Week \u2014 Active Offsets: {s['current_active_count']} DSPs "
        f"(max {s['max_offset_current']} min, {s['max_offset_current_dsp']})",
        f"W+1 Preview  \u2014 Active Offsets: {s['next_active_count']} DSPs "
        f"(max {s['max_offset_next']} min, {s['max_offset_next_dsp']})",
        f"Clear (both weeks): {s['clear_count']} DSPs",
    ]
    if s.get('excluded_rows'):
        lines.append(f"\u26a0\ufe0f {s['excluded_rows']} row(s) outside current/W+1 excluded ({s['excluded_range']})")
    lines.append('')
    lines += _format_table(f"Current Week Active Offsets ({s['current_week_range']}):", result['current_week'])
    lines.append('')
    lines += _format_table(f"W+1 Preview Active Offsets ({s['next_week_range']}):", result['next_week'])
    lines.append('')
    lines.append(f"Week-on-Week Changes — Current Week ({s['current_week_range']}) vs W+1 ({s['next_week_range']}):")
    if not result['changes']:
        lines.append('  (no changes)')
    for c in result['changes']:
        arrow = '\u2b06\ufe0f' if c['change'] > 0 else '\u2b07\ufe0f' if c['change'] < 0 else '\u27a1\ufe0f'
        sign = '+' if c['change'] > 0 else ''
        lines.append(f"  \u2022 {c['dsp']} ({c['type']}): {c['current_week']} \u2192 {c['next_week']} "
                      f"({arrow} {sign}{c['change']})")

    return '\n'.join(lines)
