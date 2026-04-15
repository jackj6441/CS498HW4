from pathlib import Path

import pandas as pd


INPUT_CSV = Path("taxi_trips.csv")
OUTPUT_CSV = Path("taxi_trips_clean.csv")
SOURCE_COLUMNS = [
    "Trip ID",
    "Taxi ID",
    "Company",
    "Pickup Community Area",
    "Dropoff Community Area",
    "Fare",
    "Trip Seconds",
]
RENAME_MAP = {
    "Trip ID": "trip_id",
    "Taxi ID": "driver_id",
    "Company": "company",
    "Pickup Community Area": "pickup_area",
    "Dropoff Community Area": "dropoff_area",
    "Fare": "fare",
    "Trip Seconds": "trip_seconds",
}
NUMERIC_TYPES = {
    "pickup_area": int,
    "dropoff_area": int,
    "fare": float,
    "trip_seconds": int,
}


def main() -> None:
    if not INPUT_CSV.exists():
        print(f"Error: input file not found: {INPUT_CSV}")
        raise SystemExit(1)

    try:
        df = pd.read_csv(INPUT_CSV, usecols=SOURCE_COLUMNS)
    except ValueError as exc:
        print(f"Error: failed to read required columns from {INPUT_CSV}: {exc}")
        raise SystemExit(1)

    df = df[SOURCE_COLUMNS].rename(columns=RENAME_MAP)
    df = df.dropna()
    df = df.astype(NUMERIC_TYPES)
    df = df[(df["fare"] > 0) & (df["trip_seconds"] > 0)].head(10_000)

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Success: cleaned data saved to {OUTPUT_CSV} with {len(df)} rows.")


if __name__ == "__main__":
    main()
