
# Crime CSV → API (FastAPI)

This turns your crime CSV into a local web API for your app (SafeNav or anything else).

## Quick Start

1) Put your CSV at: `./data/crime.csv`  
   *OR* set an env var to point anywhere:
   ```bash
   set CRIME_CSV=C:\path\to\your.csv         # Windows (PowerShell: $env:CRIME_CSV="...")
   export CRIME_CSV=/path/to/your.csv        # macOS/Linux
   ```

2) Install dependencies (recommend a venv):
```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

3) Run the API:
```bash
uvicorn main:app --reload --port 8000
```
Open http://127.0.0.1:8000/docs for interactive API docs.

## Endpoints

- `GET /health` — basic health check
- `GET /columns` — see detected columns and total rows
- `GET /incidents` — filter/paginate results
  - Query params: `q, category, city, state, start_date, end_date, min_lat, max_lat, min_lng, max_lng, limit, offset, sort`
- `GET /stats?by=category|day|month|year|city|state` — simple aggregations
- `GET /geojson` — points as GeoJSON FeatureCollection (requires lat/lng)
- `GET /heatmap?bins=50` — grid heatmap counts (requires lat/lng)

## Column Auto‑Mapping

The API tries to auto-detect your column names. It looks for:

- Latitude: `latitude|lat|y`
- Longitude: `longitude|lng|lon|x`
- Date: `date|datetime|occurred_on|timestamp|reported_date|reported_at`
- Category: `category|offense|crime_type|offense_type|type|ucr|incident_type`
- Description: `description|details|summary|narrative|offense_description|incident_description`
- Id: `id|incident_id|case_number|case_id|event_number`
- City/State: `city|municipality|jurisdiction`, `state|province|region`

Check the mapping via `GET /columns`.

## Notes

- If there are no lat/lng columns, `/geojson` and `/heatmap` will return an error.
- Dates are parsed flexibly; if parsing fails, date-based features may be limited.
- CORS is open by default so you can call this from a local web app during development.

## Example Calls

All incidents (first 100):
```
GET http://127.0.0.1:8000/incidents
```

Tulsa only, last 30 days:
```
GET http://127.0.0.1:8000/incidents?city=Tulsa&start_date=2025-07-20
```

Heatmap (100 bins):
```
GET http://127.0.0.1:8000/heatmap?bins=100
```

## Production Tips

- Lock CORS to trusted origins.
- Run with a proper ASGI server (uvicorn/gunicorn) behind a reverse proxy.
- Consider chunked/streaming reads for very large CSVs or import into a DB.
