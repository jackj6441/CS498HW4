from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INPUT_CSV = Path("taxi_trips_clean.csv")
OUTPUT_DIR = Path("processed_data")


def main() -> None:
    if not INPUT_CSV.exists():
        print(f"Error: input file not found: {INPUT_CSV}")
        raise SystemExit(1)

    spark = (
        SparkSession.builder.appName("CS498HW4Preprocess").getOrCreate()
    )

    try:
        trips_df = spark.read.csv(
            str(INPUT_CSV),
            header=True,
            inferSchema=True,
        )

        trips_df = trips_df.withColumn(
            "fare_per_minute",
            F.col("fare") / (F.col("trip_seconds") / F.lit(60.0)),
        )

        trips_df.createOrReplaceTempView("trips")

        company_summary_df = spark.sql(
            """
            SELECT
                company,
                COUNT(*) AS trip_count,
                ROUND(AVG(fare), 2) AS avg_fare,
                ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute
            FROM trips
            GROUP BY company
            ORDER BY trip_count DESC
            """
        )

        company_summary_df.write.mode("overwrite").json(str(OUTPUT_DIR))
        print(f"Completed: wrote company summary JSON to {OUTPUT_DIR}/")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
