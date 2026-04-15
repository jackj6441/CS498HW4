import os
from pathlib import Path

import pandas as pd
from neo4j import GraphDatabase


INPUT_CSV = Path("taxi_trips_clean.csv")
DEFAULT_NEO4J_URI = "bolt://localhost:7687"
DEFAULT_NEO4J_USER = "neo4j"
BATCH_SIZE = 1000
REQUIRED_COLUMNS = [
    "trip_id",
    "driver_id",
    "company",
    "dropoff_area",
    "fare",
    "trip_seconds",
]


def execute_write(session, work, *args):
    if hasattr(session, "execute_write"):
        return session.execute_write(work, *args)
    return session.write_transaction(work, *args)


def clear_database(tx) -> None:
    tx.run("MATCH (n) DETACH DELETE n")


def load_batch(tx, rows) -> None:
    tx.run(
        """
        UNWIND $rows AS row
        MERGE (d:Driver {driver_id: row.driver_id})
        MERGE (c:Company {name: row.company})
        MERGE (a:Area {area_id: row.dropoff_area})
        MERGE (d)-[:WORKS_FOR]->(c)
        CREATE (d)-[:TRIP {
            trip_id: row.trip_id,
            fare: row.fare,
            trip_seconds: row.trip_seconds
        }]->(a)
        """,
        rows=rows,
    )


def batched(rows, batch_size):
    for start in range(0, len(rows), batch_size):
        yield rows[start : start + batch_size]


def load_rows() -> list[dict]:
    if not INPUT_CSV.exists():
        raise SystemExit(f"Error: input file not found: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        missing_list = ", ".join(missing_columns)
        raise SystemExit(
            f"Error: missing required columns in {INPUT_CSV}: {missing_list}"
        )

    rows = []
    for row in df[REQUIRED_COLUMNS].itertuples(index=False):
        rows.append(
            {
                "trip_id": str(row.trip_id),
                "driver_id": str(row.driver_id),
                "company": str(row.company),
                "dropoff_area": int(row.dropoff_area),
                "fare": float(row.fare),
                "trip_seconds": int(row.trip_seconds),
            }
        )

    return rows


def main() -> None:
    neo4j_uri = os.getenv("NEO4J_URI", DEFAULT_NEO4J_URI)
    neo4j_user = os.getenv("NEO4J_USER", DEFAULT_NEO4J_USER)
    neo4j_password = os.getenv("NEO4J_PASSWORD")

    if not neo4j_password:
        raise SystemExit("Error: NEO4J_PASSWORD environment variable is required.")

    rows = load_rows()

    driver = GraphDatabase.driver(
        neo4j_uri,
        auth=(neo4j_user, neo4j_password),
    )

    try:
        if hasattr(driver, "verify_connectivity"):
            driver.verify_connectivity()

        with driver.session() as session:
            execute_write(session, clear_database)
            for batch in batched(rows, BATCH_SIZE):
                execute_write(session, load_batch, batch)
    finally:
        driver.close()

    print(f"Loaded {len(rows)} trips into Neo4j from {INPUT_CSV}.")


if __name__ == "__main__":
    main()
