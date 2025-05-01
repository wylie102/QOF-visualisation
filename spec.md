# QOF Visualization Tool Specification

## 1. Overview

A web-based, interactive dashboard for visualizing Quality and Outcomes Framework (QOF) performance of GP practices across England, enriched with contextual measures and geospatial mapping.

## 2. Objectives

- Provide an intuitive map-based view of GP practice performance by QOF indicator and achievement percentage.
- Enable dynamic filtering by indicator and achievement bands (<20%, 20–40%, 40–60%, >60%).
- Enrich spatial data with additional measures: IMD decile, practice list size, % over 65, CQC rating, and patients per GP (FTE).
- Support drill-down and roll-up: practice → PCN → ICB.
- Automate data refresh and deployment for production use.

---

## 3. Data Sources

1. **QOF Achievement Data**
   - URL to versioned ZIP (e.g., 2023–24)
   - Extract, transform, load into DuckDB or SQLMesh
2. **GP Practice Info & Coordinates**
   - Parquet file of practice names, lat, lon, telephone, postcode
3. **Indicator Mapping & Descriptions**
   - `mapping_indicators_YYYY.parquet`
   - `PCD_Output_Descriptions_YYYYMMDD.parquet`
4. **Postcode → Practice Catchment Mapping**
   - CSV/Parquet: postcode → practice code
5. **Practice → PCN / ICB Mapping**
   - NHS Digital Open Data
6. **LSOA → IMD Data**
   - IMD decile, subdomain scores per LSOA
   - ONS Postcode Directory to map postcodes to LSOAs
7. **Additional Measures**
   - Practice list size, age/sex distribution (NHS Digital)
   - CQC ratings (CQC public data)
   - GP FTE counts (NHS Digital workforce data)

---

## 4. Data Modeling & Pre-Processing

- Use a Python script (`data_refresh.py`) to:
  1. Check file existence and staleness (e.g., >24 h).
  2. Run DuckDB SQL queries to pivot and calculate percentages.
  3. Export `percent_achieved.parquet`.
- Optionally use SQLMesh for versioned, incremental models:
  - `clean_gp_addresses`, `qof_cleaned`, `geocoded_postcodes`, `mart_qof_geo`.
- Load final Parquet into DuckDB in-memory for fast querying.

---

## 5. Application Architecture

```
raw_sources (parquet/CSV/zip)
    ↓ data_refresh.py / SQLMesh
percent_achieved.parquet + enriched tables
    ↓
Dash App (Python)
 ├─ app.py (entrypoint)
 ├─ data_refresh integration
 ├─ duckdb in-memory table initialization
 ├─ layout.py (UI definition)
 └─ callbacks.py (interactive logic)
    ↓
User interacts via web browser
```

---

## 6. Key Features

- **Map Visualization**: `scatter_mapbox` of practice or aggregated PCN/ICB centroids
- **Interactive Controls**:
  - Dropdown for QOF indicator codes
  - Checklist or slider for achievement bands
  - Toggle: practice ⇄ PCN ⇄ ICB view
- **Dynamic Filtering**: Python/SQL slicing in callbacks (in-memory DuckDB)
- **Context Layers**:
  - Color/size encoding: IMD decile, CQC rating, patients per GP
- **Deployment**:
  - Dash served via Gunicorn; configured with `Procfile`
  - Hosted on Render.com with auto-refresh on GitHub push
- **Testing**: pytest for layout & callback structure

---

## 7. Deployment & Automation

- **Requirements**: `requirements.txt` (dash, plotly, duckdb, polars, gunicorn)
- **Procfile**: `web: gunicorn app:app`
- **Render Setup**:
  - GitHub repo connected
  - Auto-deploy on push
  - Scheduled health checks
- **Data Refresh**:
  - `data_refresh.refresh_data()` invoked at app startup
  - Optional cron/APS scheduler for nightly runs

---

## 8. Value Proposition

- **Geospatial insight**: identify clusters and outliers by performance
- **Contextual understanding**: explain variation with deprivation, demographics, staffing, quality ratings
- **Interactive benchmarking**: customize views by indicator and thresholds
- **Decision support** for ICBs, PCNs, commissioners, and public health teams

---

## 9. Future Extensions

- Non-convex hull catchment polygons
- Time-series comparison across years
- Integration with patient experience and service access metrics
- Multi-page Dash app with sections per disease domain
- Exportable reports (PDF/PNG) and shareable links

---
