from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INPUT_CSV = Path("taxi_trips_clean.csv")
OUTPUT_DIR = Path("processed_data")
SUMMARY_QUERY = """
SELECT
    company,
    COUNT(*) AS trip_count,
    ROUND(AVG(fare), 2) AS avg_fare,
    ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute
FROM trips
GROUP BY company
ORDER BY trip_count DESC
"""


def main() -> None:
    if not INPUT_CSV.exists():
        print(f"Error: input file not found: {INPUT_CSV}")
        raise SystemExit(1)

    spark = SparkSession.builder.appName("CS498HW4Preprocess").getOrCreate()

    try:
        trips = spark.read.csv(str(INPUT_CSV), header=True, inferSchema=True)
        trips = trips.withColumn(
            "fare_per_minute",
            F.col("fare") / (F.col("trip_seconds") / F.lit(60.0)),
        )
        trips.createOrReplaceTempView("trips")
        summary = spark.sql(SUMMARY_QUERY)
        summary.write.mode("overwrite").json(str(OUTPUT_DIR))
        print(f"Completed: wrote company summary JSON to {OUTPUT_DIR}/")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
