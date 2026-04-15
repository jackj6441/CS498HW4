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
LOAD_QUERY = """
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
"""


def clear_database(tx) -> None:
    tx.run("MATCH (n) DETACH DELETE n")


def load_batch(tx, rows) -> None:
    tx.run(LOAD_QUERY, rows=rows)


def main() -> None:
    if not INPUT_CSV.exists():
        raise SystemExit(f"Error: input file not found: {INPUT_CSV}")

    neo4j_uri = os.getenv("NEO4J_URI", DEFAULT_NEO4J_URI)
    neo4j_user = os.getenv("NEO4J_USER", DEFAULT_NEO4J_USER)
    neo4j_password = os.getenv("NEO4J_PASSWORD")

    if not neo4j_password:
        raise SystemExit("Error: NEO4J_PASSWORD environment variable is required.")

    try:
        df = pd.read_csv(INPUT_CSV, usecols=REQUIRED_COLUMNS)
    except ValueError as exc:
        raise SystemExit(
            f"Error: failed to read required columns from {INPUT_CSV}: {exc}"
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

    driver = GraphDatabase.driver(
        neo4j_uri,
        auth=(neo4j_user, neo4j_password),
    )

    try:
        if hasattr(driver, "verify_connectivity"):
            driver.verify_connectivity()

        with driver.session() as session:
            write = (
                session.execute_write
                if hasattr(session, "execute_write")
                else session.write_transaction
            )
            write(clear_database)
            for start in range(0, len(rows), BATCH_SIZE):
                write(load_batch, rows[start : start + BATCH_SIZE])
    finally:
        driver.close()

    print(f"Loaded {len(rows)} trips into Neo4j from {INPUT_CSV}.")


if __name__ == "__main__":
    main()
