from prefect import flow, task
import subprocess


@task
def fetch_task():
    subprocess.run(["python", "scripts/fetch_vendee_data.py"], check=True)

@task
def bronze():
    subprocess.run(["python", "databricks_notebooks/01_bronze_ingest.py"])

@task
def silver():
    subprocess.run(["python", "databricks_notebooks/02_silver_transform.py"])

@task
def gold():
    subprocess.run(["python", "databricks_notebooks/03_gold_models.py"])

@flow
def vendee_pipeline():
    fetch_task()
    bronze()
    silver()
    gold()

if __name__ == "__main__":
    vendee_pipeline()
