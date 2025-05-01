# Plan

## Stage 1: Foundations & Ingestion

Goal: Establish a reproducible pipeline that grabs raw QOF and GP data and produces a clean “percent_achieved.parquet.”
 • Tools/Ideas:
 • Existing DuckDB scripts (pivot_query, percent_achieved_query)
 • A simple Python “data_refresh.py” that checks timestamps and reruns those queries
 • Store outputs as Parquet in data/

⸻

## Stage 2: Practice Geocoding

Goal: Produce a reliable practice_coordinates.parquet table for all GP practices.
 • Tools/Ideas:
 • Your async + sync fallback geocoding script
 • DuckDB table writes to Parquet
 • Validate coordinates coverage (>95% of practices)

⸻

## Stage 3: Static Map Prototype

Goal: Wire up a one-off Plotly map showing practices for a hard-coded indicator and band (<20%).
 • Tools/Ideas:
 • Load both Parquets into DuckDB (or Polars) in a simple Python script
 • Render px.scatter_mapbox locally to validate layouts, hover templates, marker sizing

⸻

Stage 4: Dash Skeleton & Dynamic Filters

Goal: Embed the static map into a Dash app with:

 1. Dropdown of all INDICATOR_CODE values
 2. Checklist of achievement bands
 3. Callback that re-renders the map on selection

 • Tools/Ideas:
 • Project structure: app.py, layout.py, callbacks.py
 • In-memory loading of Parquet at startup
 • Pandas/Polars slicing in callbacks

⸻

## Stage 5: Contextual Metrics Integration

Goal: Enrich each practice point with the five extra data layers (IMD, list size, % >65, CQC, GP FTE).
 • Tools/Ideas:
 • Download/join LSOA→IMD and postcode directory stuff
 • NHS Digital CSV → Parquet for list size, FTE, CQC
 • DuckDB/SQLMesh models or DuckDB joins in your data_refresh step
 • Expose extra fields via custom_data or marker color/size

⸻

## Stage 6: Roll-up Views (PCN & ICB)

Goal: Allow toggling between practice-level and aggregated PCN/ICB-level views.
 • Tools/Ideas:
 • File or model mapping practice→PCN and practice→ICB from NHS Digital
 • Compute centroids (or convex hulls later) via SQL or Pandas/GeoPandas
 • Add a “view level” radio or dropdown to Dash, and update callback logic accordingly

⸻

## Stage 7: Production & Automation

Goal: Deploy a robust, auto-refreshing dashboard on Render.com (or similar).
 • Tools/Ideas:
 • requirements.txt + Procfile (gunicorn app:app)
 • Hook data_refresh.refresh_data() into app startup for staleness checks
 • Set up GitHub → Render auto-deploy on push
 • Add scheduled background refresh (cron or apscheduler) if nightly QOF updates

⸻

Future Extensions (Post-MVP)
 • Polygons: Generate convex/concave hulls of PCN borders
 • Time series: Side-by-side year-over-year comparisons
 • Additional layers: Patient survey scores, multimorbidity rates
 • Export & sharing: PDF/PNG snapshots, public links, embedding APIs
