from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max

spark = SparkSession.builder.appName("Gold Models").getOrCreate()

# Input/Output paths
silver_path = "data/processed/silver/vendee_positions_clean"
gold_leaderboard_path = "data/processed/gold/vendee_leaderboard"
gold_performance_path = "data/processed/gold/vendee_performance"

# Load Silver
df_silver = spark.read.parquet(silver_path)

# --- 1. Leaderboard ---
# Take the latest record per boat (using max Time_FR)
df_leaderboard = (
    df_silver
    .groupBy("Boat", "Skipper_Boat", "Country")
    .agg(
        spark_max("Time_FR").alias("Last_Report"),
        spark_max("Rank").alias("Rank"),
        spark_max("DTF").alias("DTF_nm")
    )
    .orderBy(col("Rank").cast("int"))
)

# Save leaderboard
df_leaderboard.write.mode("overwrite").parquet(gold_leaderboard_path)

print("🏁 Leaderboard created.")
df_leaderboard.show(10, truncate=False)


# --- 2. Performance Trends ---
# Compute average performance metrics per boat
df_performance = (
    df_silver
    .groupBy("Boat", "Skipper_Boat", "Country")
    .agg(
        avg("Speed_24h").alias("Avg_Speed_24h"),
        avg("Distance_24h").alias("Avg_Distance_24h"),
        avg("VMG_24h").alias("Avg_VMG_24h")
    )
    .orderBy(col("Avg_Speed_24h").desc())
)

# Save performance
df_performance.write.mode("overwrite").parquet(gold_performance_path)

print("📊 Performance trends created.")
df_performance.show(10, truncate=False)
