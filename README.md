# DSP Tools Hub — Web v1.4

Web version of the DSP Tools Hub desktop app.
**Multi-station support** — each station has its own webhook configuration.

Flask + Bootstrap 5 · Docker · Render.com

## Features

- **10 Processing Tools** — Generate Slack messages from metric CSVs
- **Multi-Station Support** — Station selection on first visit, per-station webhooks
- **Safe Mode** — Anonymise driver IDs (last 4 chars) before sending
- **Dual Storage** — GitHub (persists across deploys) + local fallback

## Tools

| Tool | Input file |
|------|-----------:|
| 🔍 Chase | `OUTSTANDING SCRUB ERROR*.csv` |
| 📬 Pickups | `AWAITING PICK UP*.csv` + optional `SearchResults*.csv` |
| 📋 Rostering | `Rostering_Capacity_C_*.csv` |
| 🚚 STC | `Dive Deep Data Service Type Compliance*.csv` |
| 📞 CC | `Exceptions_Based_Dee_*.csv` |
| 📷 POD | `POD_Summary_*.csv` |
| 🔔 NOA | `Exceptions_Based_Dee_*.csv` |
| 👜 Bags | `List_of_not_returned_*.csv` |
| 🕵️ Carrier Investigations | `Carrier_Investigatio_*.csv` |
| 🛡️ VSA | `Dive Deep Data Total Expected VSA Audits (Cycle)*.csv` |

## Run locally

```bash
pip install -r requirements.txt
python app.py
```

## Deploy (Render)

1. Push to GitHub: `dnr1deliveries-afk/dsp-tools-hub`
2. Create new Render Web Service → Docker
3. Set environment variables:
   - `SECRET_KEY` — any random string
   - `GITHUB_TOKEN` — PAT with repo scope
   - `GITHUB_REPO` — `dnr1deliveries-afk/dsp-tools-hub`

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SECRET_KEY` | Yes | Flask session secret |
| `GITHUB_TOKEN` | Yes | PAT for webhook storage |
| `GITHUB_REPO` | Yes | `owner/repo` |
| `FLASK_DEBUG` | No | `true` for dev |
| `LOG_LEVEL` | No | `INFO` / `DEBUG` |

## Storage Layout

```
hub_data/
└── stations/
    ├── DNR1/
    │   ├── webhooks.json    # DSP webhook URLs
    │   └── settings.json    # Payload key, etc.
    ├── DRM3/
    │   ├── webhooks.json
    │   └── settings.json
    └── ...
```

## Changelog

### v1.4 (2026-06-14)
- Added **VSA (Vehicle Safety Audit)** tool — bi-weekly cycle, filters `inspection_passed = N`
- Safe Mode: VINs anonymised to `VIN-XXXX` tokens

### v1.3 (2026-04-02)
- **Multi-station support** — station selection modal on first visit
- Per-station webhook storage (`hub_data/stations/{STATION}/`)
- Station badge in navbar (click to change)
- Configurable webhook payload key per station

### v1.2 (2026-04-02)
- Added configurable webhook payload key setting
- Safe Mode tokens now use last 4 chars of driver ID

### v1.1 (2026-03-29)
- Added **Carrier Investigations** tool (DNR = Delivered Not Received)

### v1.0 (2026-03-26)
- Initial release with 8 tools
