# ba882-team5-boston311
BA882 Team 5 - Boston 311 Data Pipeline and Analytic Project

## Running dbt (dev vs prod)

dbt connection settings are in `dbt/profiles.yml` (no secrets; you log in with your own Google account).

**Dev (your own sandbox):**
```bash
export DBT_DEV_DATASET=boston311_<yourname>_dev   # once per terminal session, or add it to ~/.bashrc
cd dbt
dbt build --profiles-dir .
```
If `DBT_DEV_DATASET` isn't set, dbt falls back to a shared `boston311_dev` dataset.

**Prod (shared; what the dashboard reads):** `boston311_analytics`. Build only from up-to-date `main`:
```bash
cd dbt
dbt build --target prod --profiles-dir .
```
Once dbt runs in Airflow, prod will be rebuilt automatically after each weekly load.
