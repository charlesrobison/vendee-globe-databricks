from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import os

# Initialize Spark session
spark = SparkSession.builder.appName("VendeeGlobe Bronze Ingest").getOrCreate()

RAW_PATH = "data/raw"
BRONZE_PATH = "data/processed/bronze"
os.makedirs(BRONZE_PATH, exist_ok=True)

# Read all JSON snapshots
df_raw = spark.read.option("multiline", "true").json(f"{RAW_PATH}/*.json")

# Flatten the 'latestdata.data' array
df = df_raw.select(
    explode(col("latestdata.data")).alias("boat_data")
).select("boat_data.*")  # now each boat is a row

print("Schema of ingested data:")
df.printSchema()

print(f"Number of records ingested: {df.count()}")

# Save as Parquet
output_path = os.path.join(BRONZE_PATH, "vendee_positions")
df.write.mode("overwrite").parquet(output_path)

print(f"✅ Bronze ingest complete. Data written to {output_path}")
