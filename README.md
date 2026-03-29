# DSP Tools Hub — Web v1.1

Web version of the DSP Tools Hub desktop app.
Flask + Bootstrap 5 · Docker · Render.com

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

## Changelog

### v1.1 (2026-03-29)
- Added **Carrier Investigations** tool (DNR = Delivered Not Received)

### v1.0 (2026-03-26)
- Initial release with 8 tools
