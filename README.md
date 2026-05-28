# DSP Tools Hub — Web v2.0 (COMPLIANT)

**Compliant with On-Road DSP Collaboration SOP (Week 21)**

Web application for generating DSP-level summary messages from daily metric CSVs.

## ✅ Compliance Principles

| Principle | Implementation |
|-----------|----------------|
| **DSP-Level Data Only** | All metrics aggregated to DSP totals — no route/DA detail |
| **Informational Language** | Data shared for awareness — no action requests |
| **Support-Driven** | DSPs access detailed data via their own tools (SUI, Cortex) |

## 🛠️ Tools (11 Total)

All tools output **DSP-level summary data only**:

| Tool | Output Example |
|------|----------------|
| 🔍 **Chase** | "Your DSP has 12 outstanding scrub errors" |
| 📬 **Pickups** | "Your DSP has 8 packages awaiting pickup" |
| 📋 **Rostering** | "Overall Compliance: 85%" |
| 🚚 **STC** | "Fleet Compliance: 94%" |
| 📞 **Contact Compliance** | "Total Exceptions: 23, Call Attempts: 65%" |
| 📷 **POD** | "POD Opportunities: 47" |
| 🔔 **NOA** | "Total NOA Events: 15" |
| 👜 **Unreturned Bags** | "Total Unreturned Bags: 6" |
| 🕵️ **Carrier Investigations** | "Open Investigations: 4" |
| 🛡️ **VSA** | "Vehicles Pending Inspection: 3" |
| 📊 **Tracer Bridge** | "Not Recovered: 9 packages" |

### ❌ Removed Tools

The following tools were removed as they have **no compliant path**:

- **🌱 Nursery Overuse** — Directed DSP DA deployment decisions
- **👥 Ridealong Overuse** — Directed DSP staffing decisions

## 🚀 Deployment

### Local Development

```bash
pip install -r requirements.txt
python app.py
```

### Docker (Render.com)

1. Push to GitHub: `dnr1deliveries-afk/dsp-tools-hub`
2. Create Render Web Service → Docker
3. Set environment variables:
   - `SECRET_KEY` — any random string
   - `GITHUB_TOKEN` — PAT with repo scope
   - `GITHUB_REPO` — `dnr1deliveries-afk/dsp-tools-hub`

## 📁 Project Structure

```
dsp-tools-hub-v2-compliant/
├── app.py                    # Flask application (v2.0)
├── processing/
│   └── dsp_core.py          # Compliant message generators
├── storage/
│   └── station_store.py     # Webhook storage
├── templates/
│   ├── base.html
│   ├── index.html           # Dashboard with compliance banner
│   ├── tool.html            # Tool upload/preview page
│   ├── setup.html           # Webhook management
│   ├── compliance.html      # Compliance information page
│   └── error.html
├── static/css/
│   └── hub.css
├── hub_data/stations/       # Per-station webhook configs
├── Dockerfile
├── requirements.txt
└── README.md
```

## 📋 Message Format

All messages follow the compliant format:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📦 Scrub Error Summary — [DSP]
Date: DD/MM/YYYY

Your DSP has X outstanding scrub error(s).

Breakdown by reason:
  • Reason A: X
  • Reason B: Y

ℹ️ This is DSP-level summary data for your awareness.
For detailed breakdown, please use your DSP tools (SUI, Cortex, etc.).
📞 Need support? Contact OPS during DORM windows.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

## 🔒 What Changed from v1.x

| Removed | Reason |
|---------|--------|
| Route-level data | Framework prohibits route-level sharing |
| DA/TRID identifiers | Framework prohibits DA-level sharing |
| VIN/VRN detail | Aggregated to fleet counts |
| Action request language | Messages are informational only |
| "Please chase" etc. | No directive language |
| Nursery Overuse tool | Directs DA deployment |
| Ridealong Overuse tool | Directs DA staffing |

## 📜 Framework Reference

Complies with:
- **On-Road DSP Collaboration SOP (Week 21)**
- **Principles of Engagement (POE)**
- **Four authorised meeting types:** DDM, DORM, DPR, DSP Roundtables

---

## Changelog

### v2.0 (2026-05-28)
- **BREAKING:** All tools now output DSP-level totals only
- **REMOVED:** Nursery Overuse tool (no compliant path)
- **REMOVED:** Ridealong Overuse tool (no compliant path)
- **REMOVED:** Route lookup from Pickups tool
- **REMOVED:** All DA/TRID identifiers from all tools
- **REMOVED:** All VIN/VRN detail from STC and VSA
- **REMOVED:** Action request language from all messages
- **ADDED:** Compliance information page (`/compliance`)
- **ADDED:** Compliant footer on all messages
- **ADDED:** Compliance banner on dashboard

### v1.8 (2026-05-08)
- Chase tool: Route Code lookup from Tracer file
- [Archived — non-compliant]

---

**Maintained by:** DNR1 OPS Team  
**Compliance:** Week 21 Framework
