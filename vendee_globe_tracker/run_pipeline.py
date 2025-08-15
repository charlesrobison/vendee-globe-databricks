from subprocess import run

# Paths to your Databricks notebooks (or PySpark scripts)
notebooks = [
    "databricks_notebooks/01_bronze_ingest.py",
    "databricks_notebooks/02_silver_transform.py",
    "databricks_notebooks/03_gold_models.py",
]

for nb in notebooks:
    print(f"Running {nb}...")
    result = run(["python", nb], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"Error running {nb}:\n{result.stderr}")
        break
