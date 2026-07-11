"""
ROBL PvA Offset Analysis — Internal Use Only
============================================
Analyses ROBL offset data for station management.
NOT shared with DSPs — no Slack send functionality.
"""

import pandas as pd
from io import StringIO


def generate_robl_analysis(csv_content: str) -> dict:
    """
    Process ROBL PvA Offset CSV and return analysis summary.
    
    Returns dict with:
        - summary: Overall stats
        - active_offsets: DSPs with offsets (sorted by offset desc)
        - trends: 7-day trend data
        - changes: Week-on-week changes
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
    
    # Parse dates
    df['OFD'] = pd.to_datetime(df['OFD'])
    df['_type'] = df['Service Type'].apply(
        lambda x: 'LEV' if 'LOW EMISSION' in str(x) else 'Standard'
    )
    df['_offset'] = pd.to_numeric(df['final_pva_robl_offset'], errors='coerce').fillna(0)
    df['_final_input'] = pd.to_numeric(df['final_input_minutes'], errors='coerce').fillna(0)
    df['_total_reduction'] = pd.to_numeric(df['total_reduction'], errors='coerce').fillna(0)
    
    # Sort and get date range
    df = df.sort_values('OFD', ascending=False)
    dates = sorted(df['OFD'].dt.strftime('%Y-%m-%d').unique())
    latest_date = dates[-1] if dates else None
    
    if not latest_date:
        return {'error': 'No valid dates found in file'}
    
    # Filter to latest date
    latest = df[df['OFD'].dt.strftime('%Y-%m-%d') == latest_date].copy()
    
    # Aggregate by DSP/Type (take max offset per combo)
    agg = latest.groupby(['DSP', '_type']).agg({
        '_offset': 'max',
        '_final_input': 'min',
        '_total_reduction': 'max',
        'Modified By': 'first',
        'reason_change': 'first'
    }).reset_index()
    
    # Split into with/without offsets
    with_offsets = agg[agg['_offset'] > 0].sort_values('_offset', ascending=False)
    clear_dsps = agg[agg['_offset'] == 0]['DSP'].unique().tolist()
    
    # Build active offsets list
    active_offsets = []
    for _, row in with_offsets.iterrows():
        active_offsets.append({
            'dsp': row['DSP'],
            'type': row['_type'],
            'offset': int(row['_offset']),
            'final_input': int(row['_final_input']),
            'total_reduction': int(row['_total_reduction']),
            'modified_by': row['Modified By'] if pd.notna(row['Modified By']) else '-',
            'reason': row['reason_change'] if pd.notna(row['reason_change']) else '-',
        })
    
    # Build 7-day trend for top DSPs
    trends = {}
    for dsp_row in active_offsets[:6]:
        dsp, dtype = dsp_row['dsp'], dsp_row['type']
        trend_data = []
        for date in dates:
            match = df[(df['DSP'] == dsp) & 
                       (df['_type'] == dtype) & 
                       (df['OFD'].dt.strftime('%Y-%m-%d') == date)]
            val = int(match['_offset'].max()) if len(match) > 0 and pd.notna(match['_offset'].max()) else 0
            trend_data.append({'date': date, 'offset': val})
        trends[f"{dsp}_{dtype}"] = trend_data
    
    # Week-on-week changes
    changes = []
    if len(dates) >= 2:
        start_date, end_date = dates[0], dates[-1]
        start_df = df[df['OFD'].dt.strftime('%Y-%m-%d') == start_date]
        end_df = df[df['OFD'].dt.strftime('%Y-%m-%d') == end_date]
        
        all_keys = set(
            list(zip(start_df['DSP'], start_df['_type'])) +
            list(zip(end_df['DSP'], end_df['_type']))
        )
        
        for dsp, dtype in all_keys:
            start_match = start_df[(start_df['DSP'] == dsp) & (start_df['_type'] == dtype)]
            end_match = end_df[(end_df['DSP'] == dsp) & (end_df['_type'] == dtype)]
            
            start_val = int(start_match['_offset'].max()) if len(start_match) > 0 and pd.notna(start_match['_offset'].max()) else 0
            end_val = int(end_match['_offset'].max()) if len(end_match) > 0 and pd.notna(end_match['_offset'].max()) else 0
            
            if start_val > 0 or end_val > 0:
                changes.append({
                    'dsp': dsp,
                    'type': dtype,
                    'start': start_val,
                    'end': end_val,
                    'change': end_val - start_val
                })
        
        changes.sort(key=lambda x: abs(x['change']), reverse=True)
    
    # Build summary
    summary = {
        'date_range': f"{dates[0]} → {dates[-1]}" if len(dates) > 1 else dates[0],
        'latest_date': latest_date,
        'active_count': len(active_offsets),
        'max_offset': active_offsets[0]['offset'] if active_offsets else 0,
        'max_offset_dsp': active_offsets[0]['dsp'] if active_offsets else '-',
        'clear_count': len(clear_dsps),
        'clear_dsps': clear_dsps,
    }
    
    return {
        'summary': summary,
        'active_offsets': active_offsets,
        'trends': trends,
        'changes': changes,
        'dates': dates,
        'internal_only': True,
    }


def format_robl_clipboard(result: dict) -> str:
    """Format ROBL analysis as plain text for clipboard."""
    if 'error' in result:
        return f"Error: {result['error']}"
    
    s = result['summary']
    lines = [
        f"📊 ROBL Offset Summary — {s['latest_date']}",
        f"Date Range: {s['date_range']}",
        "",
        f"Active Offsets: {s['active_count']} DSPs",
        f"Max Offset: {s['max_offset']} min ({s['max_offset_dsp']})",
        f"Clear: {s['clear_count']} DSPs",
        "",
        "Active Offsets:",
    ]
    
    for r in result['active_offsets']:
        lines.append(f"  • {r['dsp']} ({r['type']}): {r['offset']} min → {r['final_input']} final input")
    
    if result['changes']:
        lines.append("")
        lines.append("Week Changes:")
        for c in result['changes'][:5]:
            arrow = '↑' if c['change'] > 0 else '↓' if c['change'] < 0 else '→'
            sign = '+' if c['change'] > 0 else ''
            lines.append(f"  • {c['dsp']} ({c['type']}): {c['start']} → {c['end']} ({arrow}{sign}{c['change']})")
    
    return '\n'.join(lines)
