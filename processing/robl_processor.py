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


def _dsp_breakdown(week_df: pd.DataFrame) -> dict:
    """
    Per-DSP granular breakdown within a single week's snapshot (latest date):
        - avg_offset:  mean offset across ALL service types the DSP runs
                        (includes 0-offset service types, so this reflects
                        the DSP's true average burden, not just active ones)
        - service_count: how many distinct service types the DSP runs
        - top_services: top 3 service types by offset (descending)

    Returns: {dsp: {avg_offset, service_count, top_services: [...]}}
    """
    if week_df.empty:
        return {}
    latest = week_df['OFD'].max()
    snap = week_df[week_df['OFD'] == latest]

    by_svc = snap.groupby(['DSP', 'Service Type']).agg({
        '_offset': 'max',
        '_final_input': 'min',
    }).reset_index()

    breakdown = {}
    for dsp, grp in by_svc.groupby('DSP'):
        grp_sorted = grp.sort_values('_offset', ascending=False)
        top3 = [
            {
                'service_type': row['Service Type'],
                'offset': int(row['_offset']),
                'final_input': int(row['_final_input']),
            }
            for _, row in grp_sorted.head(3).iterrows()
        ]
        breakdown[dsp] = {
            'avg_offset': round(float(grp['_offset'].mean()), 1),
            'service_count': int(len(grp)),
            'top_services': top3,
        }
    return breakdown


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

    cur_breakdown = _dsp_breakdown(cur_df)
    nxt_breakdown = _dsp_breakdown(nxt_df)

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
        'dsp_breakdown': {
            'current': cur_breakdown,
            'next': nxt_breakdown,
        },
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


def _format_dsp_breakdown_section(title: str, breakdown: dict) -> list:
    lines = [title]
    if not breakdown:
        lines.append('  (none)')
        return lines
    for dsp in sorted(breakdown.keys(), key=lambda d: breakdown[d]['avg_offset'], reverse=True):
        b = breakdown[dsp]
        lines.append(f"  • {dsp} — Avg Offset: {b['avg_offset']} min across {b['service_count']} service type(s)")
        for svc in b['top_services']:
            lines.append(f"      - {svc['service_type']}: {svc['offset']} min → {svc['final_input']} final input")
    return lines


def format_dsp_breakdown_clipboard(result: dict) -> str:
    """Per-DSP average offset + top 3 service types, both weeks, plain text."""
    if 'error' in result:
        return f"Error: {result['error']}"
    s = result['summary']
    bd = result.get('dsp_breakdown', {})
    lines = ['📋 DSP Breakdown — Avg Offset & Top 3 Service Types', '']
    lines += _format_dsp_breakdown_section(f"Current Week ({s['current_week_range']}):", bd.get('current', {}))
    lines.append('')
    lines += _format_dsp_breakdown_section(f"W+1 Preview ({s['next_week_range']}):", bd.get('next', {}))
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

    bd = result.get('dsp_breakdown', {})
    lines.append('')
    lines += _format_dsp_breakdown_section(f"DSP Breakdown — Current Week ({s['current_week_range']}):", bd.get('current', {}))
    lines.append('')
    lines += _format_dsp_breakdown_section(f"DSP Breakdown — W+1 Preview ({s['next_week_range']}):", bd.get('next', {}))

    return '\n'.join(lines)


_HTML_CSS = """
<style>
  body { font-family: 'Segoe UI', Arial, sans-serif; color: #212529; margin: 0; padding: 28px; background: #fff; font-size: 16px; }
  h1 { font-size: 30px; margin-bottom: 4px; }
  .subtitle { color: #6c757d; margin-bottom: 24px; font-size: 16px; }
  .cards { display: flex; gap: 14px; margin-bottom: 28px; flex-wrap: wrap; }
  .card { border: 1px solid #dee2e6; border-radius: 6px; padding: 18px 20px; flex: 1; min-width: 200px; text-align: center; }
  .card.warning { border-color: #ffc107; }
  .card.info { border-color: #0dcaf0; }
  .card.danger { border-color: #dc3545; }
  .card.success { border-color: #198754; }
  .card .num { font-size: 38px; font-weight: 700; margin: 0; }
  .card.warning .num { color: #b38600; }
  .card.info .num { color: #0aa2c0; }
  .card.danger .num { color: #dc3545; }
  .card.success .num { color: #198754; }
  .card .label { font-size: 15px; color: #495057; margin: 6px 0 0; }
  .card .range { font-size: 14px; color: #6c757d; }
  .section { margin-bottom: 32px; }
  .section-header { background: #212529; color: #fff; padding: 12px 18px; font-size: 18px; font-weight: 600;
                     border-radius: 6px 6px 0 0; display: flex; justify-content: space-between; align-items: center; }
  .badge { background: #6c757d; color: #fff; border-radius: 10px; padding: 4px 14px; font-size: 15px; }
  table { width: 100%; border-collapse: collapse; font-size: 16px; }
  table th { background: #343a40; color: #fff; padding: 10px 14px; text-align: left; }
  table td { padding: 10px 14px; border-bottom: 1px solid #e9ecef; }
  table tr:nth-child(even) { background: #f8f9fa; }
  tr.high { background: #f8d7da !important; }
  tr.med { background: #fff3cd !important; }
  .type-badge { border-radius: 8px; padding: 3px 12px; font-size: 14px; color: #fff; }
  .type-lev { background: #0dcaf0; }
  .type-std { background: #6c757d; }
  .up { color: #dc3545; font-weight: 600; }
  .down { color: #198754; font-weight: 600; }
  .flat { color: #6c757d; }
  .warn-banner { background: #fff3cd; border: 1px solid #ffc107; border-radius: 6px; padding: 14px 18px;
                 font-size: 16px; margin-bottom: 22px; }
  .bd-grid { display: flex; gap: 28px; }
  .bd-col { flex: 1; }
  .bd-col h4 { font-size: 17px; color: #6c757d; margin-bottom: 8px; }
  .svc-line { font-size: 14.5px; color: #495057; margin: 2px 0; }
  .footer-note { font-size: 14px; color: #6c757d; margin-top: 34px; border-top: 1px solid #dee2e6; padding-top: 12px; }
  @media print { .card, table { break-inside: avoid; } .section { break-inside: avoid; } }
</style>
"""



def _html_type_badge(t: str) -> str:
    cls = 'type-lev' if t == 'LEV' else 'type-std'
    return f'<span class="type-badge {cls}">{t}</span>'


def _html_row_class(offset: int) -> str:
    if offset >= 20:
        return 'high'
    if offset >= 10:
        return 'med'
    return ''


def _html_table_section(title: str, badge: str, rows: list) -> str:
    out = [f'<div class="section"><div class="section-header"><span>{title}</span><span class="badge">{badge}</span></div>']
    out.append('<table><thead><tr><th>DSP</th><th>Type</th><th>Offset (min)</th><th>Final Input</th>'
                '<th>Total Reduction</th><th>Modified By</th><th>Reason</th></tr></thead><tbody>')
    if not rows:
        out.append('<tr><td colspan="7" style="text-align:center;color:#6c757d;">No active offsets</td></tr>')
    for row in rows:
        out.append(
            f'<tr class="{_html_row_class(row["offset"])}">'
            f'<td><strong>{row["dsp"]}</strong></td>'
            f'<td>{_html_type_badge(row["type"])}</td>'
            f'<td><strong>{row["offset"]}</strong></td>'
            f'<td>{row["final_input"]}</td>'
            f'<td>{row["total_reduction"]}</td>'
            f'<td>{row["modified_by"]}</td>'
            f'<td>{row["reason"]}</td>'
            f'</tr>'
        )
    out.append('</tbody></table></div>')
    return ''.join(out)


def _html_breakdown_col(label: str, week_bd: dict) -> str:
    out = [f'<div class="bd-col"><h4>{label}</h4><table><thead><tr><th>DSP</th><th>Avg Offset</th>'
           '<th>Top Service Types</th></tr></thead><tbody>']
    if not week_bd:
        out.append('<tr><td colspan="3" style="text-align:center;color:#6c757d;">No data</td></tr>')
    for dsp, b in sorted(week_bd.items(), key=lambda kv: kv[1]['avg_offset'], reverse=True):
        svc_lines = ''.join(
            f'<div class="svc-line">{sv["service_type"]}: <strong>{sv["offset"]}</strong> min</div>'
            for sv in b['top_services']
        )
        out.append(f'<tr><td><strong>{dsp}</strong></td><td>{b["avg_offset"]} min</td><td>{svc_lines}</td></tr>')
    out.append('</tbody></table></div>')
    return ''.join(out)


def generate_robl_html_report(result: dict, station: str = '', generated_label: str = '') -> str:
    """
    Render the full ROBL analysis as a single self-contained HTML document
    (inline CSS, no external dependencies) preserving the same visual
    formatting as the web app — summary cards, colour-coded severity rows,
    LEV/Standard type badges, trend arrows, and the DSP breakdown grid.

    Suitable for direct download/open in a browser, or conversion to PDF.
    """
    if 'error' in result:
        return f"<html><body><h1>ROBL Analysis Error</h1><p>{result['error']}</p></body></html>"

    s = result['summary']
    station_line = f"{station} &mdash; " if station else ''

    html = ['<!DOCTYPE html><html><head><meta charset="utf-8"><title>ROBL PvA Offset Analysis</title>',
            _HTML_CSS, '</head><body>']

    html.append('<h1>ROBL PvA Offset Analysis</h1>')
    html.append(f'<p class="subtitle">{station_line}Internal use only, not shared with DSPs'
                f'{" &nbsp;|&nbsp; Generated " + generated_label if generated_label else ""}</p>')

    if s.get('excluded_rows'):
        html.append(f'<div class="warn-banner">&#9888; {s["excluded_rows"]} row(s) outside the current week / '
                     f'W+1 window were excluded ({s["excluded_range"]}).</div>')

    html.append('<div class="cards">')
    html.append(f'<div class="card warning"><p class="num">{s["current_active_count"]}</p>'
                f'<p class="label">Current Week &mdash; DSPs w/ Offsets</p>'
                f'<p class="range">{s["current_week_range"]}</p></div>')
    html.append(f'<div class="card info"><p class="num">{s["next_active_count"]}</p>'
                f'<p class="label">W+1 &mdash; DSPs w/ Offsets</p>'
                f'<p class="range">{s["next_week_range"]}</p></div>')
    html.append(f'<div class="card danger"><p class="num">{s["max_offset_next"]}</p>'
                f'<p class="label">Max W+1 Offset (min) &mdash; {s["max_offset_next_dsp"]}</p></div>')
    html.append(f'<div class="card success"><p class="num">{s["clear_count"]}</p>'
                f'<p class="label">DSPs Clear (both weeks)</p></div>')
    html.append('</div>')

    html.append(_html_table_section('Current Week Active Offsets', s['current_week_range'], result['current_week']))
    html.append(_html_table_section('W+1 Preview Active Offsets', s['next_week_range'], result['next_week']))

    html.append(f'<div class="section"><div class="section-header"><span>Week-on-Week Changes</span>'
                f'<span class="badge">{s["current_week_range"]} &rarr; {s["next_week_range"]}</span></div>')
    html.append('<table><thead><tr><th>DSP</th><th>Type</th><th>Current Week</th><th>W+1</th>'
                '<th>Change</th><th>Trend</th></tr></thead><tbody>')
    if not result['changes']:
        html.append('<tr><td colspan="6" style="text-align:center;color:#6c757d;">No changes between weeks</td></tr>')
    for c in result['changes']:
        if c['change'] > 0:
            cls, arrow, sign = 'up', '&#8593;', '+'
        elif c['change'] < 0:
            cls, arrow, sign = 'down', '&#8595;', ''
        else:
            cls, arrow, sign = 'flat', '&#8594;', ''
        html.append(
            f'<tr><td><strong>{c["dsp"]}</strong></td><td>{_html_type_badge(c["type"])}</td>'
            f'<td>{c["current_week"]}</td><td>{c["next_week"]}</td>'
            f'<td class="{cls}">{sign}{c["change"]}</td><td class="{cls}">{arrow}</td></tr>'
        )
    html.append('</tbody></table></div>')

    bd = result.get('dsp_breakdown', {})
    html.append('<div class="section"><div class="section-header">'
                 '<span>DSP Breakdown &mdash; Avg Offset &amp; Top 3 Service Types</span></div>')
    html.append('<div class="bd-grid">')
    html.append(_html_breakdown_col(f'Current Week ({s["current_week_range"]})', bd.get('current', {})))
    html.append(_html_breakdown_col(f'W+1 Preview ({s["next_week_range"]})', bd.get('next', {})))
    html.append('</div></div>')

    html.append('<p class="footer-note">DSP Tools Hub &mdash; ROBL PvA Offset Analysis. '
                 'Internal station management tool. Not for external distribution.</p>')
    html.append('</body></html>')

    return ''.join(html)
