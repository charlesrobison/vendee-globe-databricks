# Vendee Globe Race Tracker  

A data engineering project that ingests, processes, and analyzes data from the official [Vendée Globe API](https://www.vendeeglobe.org/) using a modern data pipeline architecture.  

The project demonstrates orchestration with **Prefect**, raw-to-curated modeling with the **Bronze/Silver/Gold** pattern, and is designed to be **Databricks-ready** for cloud-scale execution.  

---

## 🚀 Project Architecture  

```text
airflow/                     # Legacy DAG version (kept for reference)
databricks_notebooks/
  00_fetch_vendee_data.py    # Fetch raw race snapshots from the API
  01_bronze_ingest.py        # Ingest raw JSON → Bronze
  02_silver_transform.py     # Clean + normalize → Silver
  03_gold_models.py          # Analytics tables → Gold
scripts/
  inspect_latest.py           # Quick JSON inspection utility
data/
  raw/                       # Raw snapshots from API
  processed/                 # Local Bronze/Silver/Gold outputs
docs/
  pipeline_architecture.md    # Documentation for design decisions
prefect_pipeline.py          # Prefect orchestration flow
run_pipeline.py              # Simple sequential runner (local simulation)
```
## 📊 Pipeline Flow

1. Fetch
* Calls the official Vendée Globe API
* Saves raw JSON snapshots to /data/raw

2. Bronze Layer
* Loads JSON snapshots
* Explodes boat data into structured rows

3. Silver Layer
* Cleans and normalizes data
* Converts lat/lon into decimal degrees
* Extracts numeric values from text fields (21.8 kts → 21.8)

4. Gold Layer
* Produces analytics-ready tables
* Leaderboards, rank deltas, rolling averages

5. Orchestration (Prefect)
* Handles scheduling and execution order
* Can run locally or be deployed to Prefect Cloud

## 🛠️ Tech Stack

* Python 3.12
* Prefect for orchestration
* PySpark for scalable transformations
* Databricks-ready architecture (Bronze/Silver/Gold pattern)
* Git + GitHub for version control

## ⚡ How to Run

1. Clone the repo:
```bash
git clone https://github.com/<your-username>/VENDEE-GLOBE-DATABRICKS.git
cd VENDEE-GLOBE-DATABRICKS
```

2. Create virtual environment:
```bash
python -m venv vendee_env
source vendee_env/bin/activate
pip install -r requirements.txt
```

3. Set your API key in .env:
```env
VGL_API_KEY=your_api_key_here
```

4. Run pipeline (local):
```bash
python run_pipeline.py
```

5. Orchestrate with Prefect:
```bash
python prefect_pipeline.py
```

## 📈 Example Output
* Bronze Layer: 35 boats, raw metrics per update
* Silver Layer: Cleaned metrics (speeds, headings, distances as floats)
* Gold Layer: Leaderboard with ranks, trends, and deltas

## 🔮 Future Enhancements
* Databricks integration for cloud-scale execution
* Historical Vendée Globe datasets for richer analysis
* Real-time dashboard (Sigma, Streamlit, or similar)
* Automated deployment with Docker + CI/CD