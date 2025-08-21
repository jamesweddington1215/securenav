
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Optional, List
import pandas as pd
import os
from datetime import datetime
import math

app = FastAPI(title="Crime CSV API", version="1.0.0", description="Serve your uploaded crime CSV as an API")

# CORS (open by default; adjust for production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CSV_PATH = os.environ.get("CRIME_CSV", os.path.join(os.path.dirname(__file__), "data", "crime.csv"))

_df = None
schema = {
    "lat": None,
    "lng": None,
    "date": None,
    "category": None,
    "description": None,
    "id": None,
    "city": None,
    "state": None
}

def _maybe_parse_date(s):
    # Try multiple common date formats
    fmts = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M",
        "%m/%d/%y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for f in fmts:
        try:
            return datetime.strptime(str(s), f)
        except Exception:
            pass
    # try pandas to_datetime as a fallback
    try:
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return None

def _auto_map_columns(df: pd.DataFrame):
    columns_lower = {c.lower(): c for c in df.columns}

    # helpers
    def pick(candidates):
        for name in candidates:
            if name in columns_lower:
                return columns_lower[name]
        return None

    lat_col = pick(["latitude", "lat", "y"])
    lng_col = pick(["longitude", "lon", "lng", "x"])
    date_col = pick(["date", "datetime", "occurred_on", "timestamp", "reported_date", "reported_at"])
    cat_col = pick(["category", "offense", "crime_type", "offense_type", "type", "ucr", "incident_type"])
    desc_col = pick(["description", "details", "summary", "narrative", "offense_description", "incident_description"])
    id_col = pick(["id", "incident_id", "case_number", "case_id", "event_number"])
    city_col = pick(["city", "municipality", "jurisdiction"])
    state_col = pick(["state", "province", "region"])

    return {
        "lat": lat_col,
        "lng": lng_col,
        "date": date_col,
        "category": cat_col,
        "description": desc_col,
        "id": id_col,
        "city": city_col,
        "state": state_col
    }

def _load_df():
    global _df, schema
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    schema = _auto_map_columns(df)

    # Ensure required columns exist
    if not schema["lat"] or not schema["lng"]:
        # Try to geocode-like columns; otherwise, we can still serve without geo endpoints
        pass

    # Normalize date column
    if schema["date"] and df[schema["date"]].notna().any():
        try:
            df["_dt"] = pd.to_datetime(df[schema["date"]], errors="coerce", utc=False)
        except Exception:
            df["_dt"] = df[schema["date"]].apply(_maybe_parse_date)
    else:
        df["_dt"] = pd.NaT

    # Normalize text fields
    if schema["category"] and schema["category"] in df.columns:
        df["_cat"] = df[schema["category"]].astype(str)
    else:
        df["_cat"] = ""

    if schema["description"] and schema["description"] in df.columns:
        df["_desc"] = df[schema["description"]].astype(str)
    else:
        df["_desc"] = ""

    # Normalize geo
    if schema["lat"] and schema["lat"] in df.columns:
        df["_lat"] = pd.to_numeric(df[schema["lat"]], errors="coerce")
    else:
        df["_lat"] = None

    if schema["lng"] and schema["lng"] in df.columns:
        df["_lng"] = pd.to_numeric(df[schema["lng"]], errors="coerce")
    else:
        df["_lng"] = None

    # Normalize id
    if schema["id"] and schema["id"] in df.columns:
        df["_id"] = df[schema["id"]].astype(str)
    else:
        df["_id"] = df.index.astype(str)

    # Normalize city/state
    df["_city"] = df[schema["city"]].astype(str) if schema["city"] and schema["city"] in df.columns else ""
    df["_state"] = df[schema["state"]].astype(str) if schema["state"] and schema["state"] in df.columns else ""

    _df = df

def get_df() -> pd.DataFrame:
    global _df
    if _df is None:
        _load_df()
    return _df

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/columns")
def columns():
    try:
        df = get_df()
        return {"columns": list(df.columns), "mapped": schema, "row_count": int(len(df))}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/incidents")
def incidents(
    q: Optional[str] = None,
    category: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_lat: Optional[float] = Query(None),
    max_lat: Optional[float] = Query(None),
    min_lng: Optional[float] = Query(None),
    max_lng: Optional[float] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    sort: str = Query("-date", description="Sort by: date or -date"),
):
    try:
        df = get_df()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    filt = pd.Series([True] * len(df))

    if q:
        q_lower = q.lower()
        filt &= df["_desc"].str.lower().str.contains(q_lower, na=False) | df["_cat"].str.lower().str.contains(q_lower, na=False)

    if category:
        filt &= df["_cat"].str.lower() == category.lower()

    if city:
        filt &= df["_city"].str.lower() == city.lower()

    if state:
        filt &= df["_state"].str.lower() == state.lower()

    # Date filtering
    if start_date:
        try:
            sd = pd.to_datetime(start_date)
            filt &= (df["_dt"] >= sd)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid start_date")
    if end_date:
        try:
            ed = pd.to_datetime(end_date)
            filt &= (df["_dt"] <= ed)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid end_date")

    # Bounding box
    if min_lat is not None: filt &= (df["_lat"] >= min_lat)
    if max_lat is not None: filt &= (df["_lat"] <= max_lat)
    if min_lng is not None: filt &= (df["_lng"] >= min_lng)
    if max_lng is not None: filt &= (df["_lng"] <= max_lng)

    res = df[filt].copy()

    # Sorting
    if sort == "date":
        res = res.sort_values(by="_dt", ascending=True, na_position="last")
    elif sort == "-date":
        res = res.sort_values(by="_dt", ascending=False, na_position="last")

    total = int(len(res))
    res = res.iloc[offset: offset + limit]

    # Minimal projection
    payload = []
    for _, r in res.iterrows():
        payload.append({
            "id": r.get("_id"),
            "date": None if pd.isna(r.get("_dt")) else r.get("_dt").isoformat(),
            "category": None if pd.isna(r.get("_cat")) else r.get("_cat"),
            "description": None if pd.isna(r.get("_desc")) else r.get("_desc"),
            "lat": None if pd.isna(r.get("_lat")) else float(r.get("_lat")) if r.get("_lat") is not None else None,
            "lng": None if pd.isna(r.get("_lng")) else float(r.get("_lng")) if r.get("_lng") is not None else None,
            "city": None if pd.isna(r.get("_city")) else r.get("_city"),
            "state": None if pd.isna(r.get("_state")) else r.get("_state"),
        })

    return {"total": total, "limit": limit, "offset": offset, "items": payload}

@app.get("/stats")
def stats(
    by: str = Query("category", description="category|day|month|year|city|state"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    try:
        df = get_df()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    filt = pd.Series([True] * len(df))

    if start_date:
        try:
            sd = pd.to_datetime(start_date)
            filt &= (df["_dt"] >= sd)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid start_date")
    if end_date:
        try:
            ed = pd.to_datetime(end_date)
            filt &= (df["_dt"] <= ed)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid end_date")

    res = df[filt].copy()

    if by == "category":
        grp = res.groupby("_cat", dropna=False).size().reset_index(name="count").sort_values("count", ascending=False)
        data = [{"key": str(k), "count": int(v)} for k, v in zip(grp["_cat"], grp["count"])]
    elif by in ("day", "month", "year"):
        if res["_dt"].isna().all():
            return {"by": by, "data": []}
        dt = res["_dt"].dropna()
        if by == "day":
            grp = dt.dt.date.value_counts().sort_index()
            data = [{"key": str(k), "count": int(v)} for k, v in grp.items()]
        elif by == "month":
            grp = dt.dt.to_period("M").value_counts().sort_index()
            data = [{"key": str(k), "count": int(v)} for k, v in grp.items()]
        else:
            grp = dt.dt.year.value_counts().sort_index()
            data = [{"key": int(k), "count": int(v)} for k, v in grp.items()]
    elif by == "city":
        grp = res.groupby("_city", dropna=False).size().reset_index(name="count").sort_values("count", ascending=False)
        data = [{"key": str(k), "count": int(v)} for k, v in zip(grp["_city"], grp["count"])]
    elif by == "state":
        grp = res.groupby("_state", dropna=False).size().reset_index(name="count").sort_values("count", ascending=False)
        data = [{"key": str(k), "count": int(v)} for k, v in zip(grp["_state"], grp["count"])]
    else:
        raise HTTPException(status_code=400, detail="Invalid 'by' parameter")

    return {"by": by, "data": data}

@app.get("/geojson")
def geojson():
    try:
        df = get_df()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if df["_lat"].isna().all() or df["_lng"].isna().all():
        raise HTTPException(status_code=400, detail="CSV has no latitude/longitude columns")

    features = []
    for _, r in df.iterrows():
        if pd.isna(r["_lat"]) or pd.isna(r["_lng"]):
            continue
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(r["_lng"]), float(r["_lat"])]},
            "properties": {
                "id": r["_id"],
                "date": None if pd.isna(r["_dt"]) else r["_dt"].isoformat(),
                "category": r["_cat"],
                "description": r["_desc"],
                "city": r["_city"],
                "state": r["_state"],
            }
        })
    return {"type": "FeatureCollection", "features": features}

@app.get("/heatmap")
def heatmap(bins: int = Query(50, ge=5, le=400)):
    try:
        df = get_df()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if df["_lat"].isna().all() or df["_lng"].isna().all():
        raise HTTPException(status_code=400, detail="CSV has no latitude/longitude columns")

    lat = df["_lat"].dropna()
    lng = df["_lng"].dropna()
    if lat.empty or lng.empty:
        return {"bins": bins, "grid": []}

    lat_min, lat_max = float(lat.min()), float(lat.max())
    lng_min, lng_max = float(lng.min()), float(lng.max())

    if lat_min == lat_max or lng_min == lng_max:
        # Degenerate case
        return {"bins": bins, "grid": [{"lat": lat_min, "lng": lng_min, "count": int(len(lat))}]}

    # Compute grid
    lat_step = (lat_max - lat_min) / bins
    lng_step = (lng_max - lng_min) / bins

    grid = {}
    for la, lo in zip(lat, lng):
        i = min(int((la - lat_min) / lat_step), bins - 1)
        j = min(int((lo - lng_min) / lng_step), bins - 1)
        key = (i, j)
        grid[key] = grid.get(key, 0) + 1

    result = []
    for (i, j), count in grid.items():
        cell_lat = lat_min + (i + 0.5) * lat_step
        cell_lng = lng_min + (j + 0.5) * lng_step
        result.append({"lat": cell_lat, "lng": cell_lng, "count": int(count)})

    return {"bins": bins, "bounds": {"lat_min": lat_min, "lat_max": lat_max, "lng_min": lng_min, "lng_max": lng_max}, "grid": result}
