# DSP Tools Hub v2.0 — Compliance Changelog

## Summary

This document details all changes made to transform DSP Tools Hub from v1.x (non-compliant) to v2.0 (compliant with On-Road DSP Collaboration SOP Week 21).

---

## Files Modified

### 1. `processing/dsp_core.py` — COMPLETE REWRITE

**Original:** 53,274 bytes (route/DA-level detail)  
**New:** 31,594 bytes (DSP-level totals only)

#### Changes by Function:

| Function | v1.x Output | v2.0 Output | Change |
|----------|-------------|-------------|--------|
| `generate_chase_messages` | Tracking IDs, Route Codes, per-route breakdown | "X outstanding scrub errors" + reason counts | Removed all package/route detail |
| `generate_pickup_messages` | Tracking IDs, Route lookup, per-package list | "X packages awaiting pickup" + type breakdown | Removed route lookup feature entirely |
| `generate_rostering_messages` | Per-route slot status, slot-level detail | "X% compliance" per service type | Aggregated to percentages only |
| `generate_stc_messages` | VINs, Route IDs, vehicle swap details | "X% fleet compliance" | Removed all VIN/route detail |
| `generate_cc_messages` | DA IDs (anonymised), per-delivery detail | "X exceptions, Y% call rate" | Removed all DA/delivery detail |
| `generate_pod_messages` | DA IDs (anonymised), per-delivery detail | "X POD opportunities" | Removed all DA/delivery detail |
| `generate_noa_messages` | DA IDs, "follow-up" action language | "X NOA events" | Removed DA detail and action language |
| `generate_bags_messages` | Per-route grouping, route flags | "X unreturned bags (oldest: Y days)" | Removed route-level grouping |
| `generate_carrier_inv_messages` | Package detail | "X open investigations" | Simplified to counts |
| `generate_vsa_messages` | VINs, VRNs per vehicle | "X vehicles pending" | Removed all vehicle identifiers |
| `generate_tracer_bridge_messages` | Tracking IDs, per-package list | "X not recovered" per DSP | Removed all tracking IDs |
| `generate_nursery_overuse_messages` | DA deployment detail | **RAISES ERROR** | Tool removed — no compliant path |
| `generate_ridealong_overuse_messages` | DA staffing detail | **RAISES ERROR** | Tool removed — no compliant path |

#### New Features:
- `COMPLIANT_FOOTER` constant — standard footer for all messages
- All functions now return DSP-level aggregates only
- Removed tools raise descriptive errors explaining why

---

### 2. `app.py` — MAJOR UPDATE

**Original:** 26,515 bytes  
**New:** 24,144 bytes

#### Changes:

| Section | Change |
|---------|--------|
| `VERSION` | Changed from `'1.8'` to `'2.0'` |
| `TOOLS` dict | Updated all descriptions to indicate "DSP totals only" |
| `TOOLS` dict | Added `compliant_note` field to each tool |
| `TOOLS` dict | Removed `nursery_overuse` and `ridealong_overuse` entries |
| `GENERATORS` dict | Removed `nursery_overuse` and `ridealong_overuse` mappings |
| Imports | Removed imports for deleted generators |
| `/compliance` route | **NEW** — Compliance information page |
| `/health` endpoint | Added `'compliant': True` to response |
| Safe Mode comment | Added note that Safe Mode no longer necessary in v2.0 |

---

### 3. `templates/index.html` — UPDATED

#### Changes:
- Added compliance banner at top of page
- Changed tagline from "Generate and send per-DSP Slack messages" to "DSP-level summary metrics for informational awareness"
- Replaced Safe Mode info box with Framework Compliance info box
- Added "Compliance Info" card linking to `/compliance`
- Added compliance badges to tool cards
- Added warning about removed tools (Nursery, Ridealong)

---

### 4. `templates/compliance.html` — NEW FILE

**Size:** 12,777 bytes

New page documenting:
- Three compliance principles (DSP-level, Informational, Support-driven)
- Tool compliance status table (11 compliant, 2 removed)
- What was removed (route data, DA data, action requests, directive language)
- Framework reference (SOP, POE, four meeting types)

---

### 5. `README.md` — COMPLETE REWRITE

**Original:** 2,888 bytes  
**New:** 4,381 bytes

Changes:
- Added compliance principles section
- Updated tools table with v2.0 output examples
- Added "Removed Tools" section
- Added "What Changed from v1.x" table
- Added message format example
- Updated deployment instructions
- Added full changelog

---

## Message Format Changes

### v1.x Message Example (Chase):
```
Outstanding Shipments — [DSP]
Updated: DD/MM/YYYY
Total Packages: 12

Good morning. Please see below for any shipments not yet returned to station.

Reason A (5):
  TBA123456789 [Route-47]
  TBA234567890 [Route-47]
  ...

Root Cause: _______________

Actions:
1. Contact the driver to locate package
2. Confirm return ETA to station
3. Update this thread with status
4. Escalate if no response within 2 hours

Appreciate your support.
```

### v2.0 Message Example (Chase):
```
📦 Scrub Error Summary — [DSP]
Date: DD/MM/YYYY

Your DSP has 12 outstanding scrub error(s).

Breakdown by reason:
  • Reason A: 5
  • Reason B: 4
  • Reason C: 3

ℹ️ This is DSP-level summary data for your awareness.
For detailed breakdown, please use your DSP tools (SUI, Cortex, etc.).
📞 Need support? Contact OPS during DORM windows.
```

### Key Differences:
| Element | v1.x | v2.0 |
|---------|------|------|
| Tracking IDs | Listed individually | **Removed** |
| Route Codes | Shown per package | **Removed** |
| Action requests | "Contact driver", "Confirm ETA" | **Removed** |
| Root Cause prompt | Required input | **Removed** |
| Directive language | "Please see below" | Informational framing |
| Footer | None | Standard compliance footer |

---

## Removed Features

### Nursery Overuse Tool
- **Reason:** Directed DSP decisions regarding DA deployment
- **Framework violation:** "Do not direct DSP decisions regarding individual DAs or specific routes"
- **Resolution:** Function raises error explaining removal

### Ridealong Overuse Tool
- **Reason:** Directed DSP staffing and DA deployment decisions
- **Framework violation:** DSPs manage their own employees
- **Resolution:** Function raises error explaining removal

### Route Lookup (Pickups)
- **Reason:** Provided route-level data to DSPs
- **Framework violation:** Cannot share route-level data
- **Resolution:** Feature completely removed; `search_bytes` parameter ignored

### Safe Mode Anonymisation
- **Status:** Code preserved but no longer necessary
- **Reason:** v2.0 outputs no DA/route identifiers, so anonymisation is moot
- **Note:** Toggle kept for backwards compatibility but has no effect

---

## Deployment Notes

1. **Test locally first:**
   ```bash
   cd dsp-tools-hub-v2-compliant
   pip install -r requirements.txt
   python app.py
   ```

2. **Push to repository:**
   ```bash
   git add .
   git commit -m "v2.0: Framework-compliant rebuild (Week 21)"
   git push origin main
   ```

3. **Render.com will auto-deploy** from the GitHub repository

4. **Inform stakeholders:**
   - OPS team: New message format, removed tools
   - DSPs: Messages now show totals only, use SUI/Cortex for detail

---

## Compliance Verification Checklist

Before deployment, verify each tool passes:

- [ ] No route-level data in output
- [ ] No DA/TRID identifiers in output
- [ ] No VIN/VRN identifiers in output
- [ ] No action request language
- [ ] No directive language ("you must", "you need to")
- [ ] Compliant footer present
- [ ] Output is DSP-level totals only

---

**Compliance Contact:** OTRM / DSM  
**Framework:** On-Road DSP Collaboration SOP (Week 21)  
**Date:** 28 May 2026
