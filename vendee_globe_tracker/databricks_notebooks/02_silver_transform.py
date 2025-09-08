from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, udf
from pyspark.sql.types import DoubleType
import re

spark = SparkSession.builder.appName("Silver Transform").getOrCreate()

# Input/Output paths
bronze_path = "data/processed/bronze/vendee_positions"
silver_path = "data/processed/silver/vendee_positions_clean"

# Load Bronze
df_bronze = spark.read.parquet(bronze_path)

# --- Helper: convert Latitude/Longitude to decimal degrees ---
def dms_to_dd(dms: str) -> float:
    """
    Convert coordinates like "44°29.53'N" or "15°26.08'W" to decimal degrees.
    """
    if dms is None:
        return None
    match = re.match(r"(\d+)°(\d+\.\d+)'([NSEW])", dms.strip())
    if not match:
        return None
    degrees, minutes, direction = match.groups()
    dd = float(degrees) + float(minutes) / 60
    if direction in ["S", "W"]:
        dd = -dd
    return dd

udf_dms_to_dd = udf(dms_to_dd, DoubleType())

# --- Normalize numeric columns (strip units like "21.8 kts") ---
def strip_units(df, colname, pattern="[^0-9\\.-]"):
    return df.withColumn(colname, regexp_replace(col(colname), pattern, "").cast("double"))

df_silver = (
    df_bronze
    .withColumn("Latitude_dd", udf_dms_to_dd(col("Latitude")))
    .withColumn("Longitude_dd", udf_dms_to_dd(col("Longitude")))
)

# Apply unit stripping to speed/distance/VMG columns
columns_to_clean = [
    "DTF", "DTL",
    "Speed_24h", "Speed_30min", "Speed_LastReport",
    "Distance_24h", "Distance_30min", "Distance_LastReport",
    "VMG_24h", "VMG_30min", "VMG_LastReport"
]

for c in columns_to_clean:
    df_silver = strip_units(df_silver, c)

# Select only cleaned columns for analytics
df_silver = df_silver.select(
    "Boat", "Skipper_Boat", "Rank", "Country",
    "Latitude_dd", "Longitude_dd",
    "Speed_24h", "Speed_30min", "Speed_LastReport",
    "Distance_24h", "Distance_30min", "Distance_LastReport",
    "VMG_24h", "VMG_30min", "VMG_LastReport",
    "DTF", "DTL", "Time_FR"
)

# Write out Silver
df_silver.write.mode("overwrite").parquet(silver_path)

print("Silver transformation complete.")
print("Number of records in Silver:", df_silver.count())
df_silver.show(5, truncate=False)
