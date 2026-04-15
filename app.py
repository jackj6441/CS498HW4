from flask import Flask, jsonify, request


app = Flask(__name__)


def todo_response(endpoint: str):
    return (
        jsonify(
            {
                "status": "todo",
                "endpoint": endpoint,
                "message": "Endpoint scaffold created. Query logic still needs implementation.",
            }
        ),
        501,
    )


@app.get("/graph-summary")
def graph_summary():
    return todo_response("/graph-summary")


@app.get("/top-companies")
def top_companies():
    _ = request.args.get("n", default=5, type=int)
    return todo_response("/top-companies")


@app.get("/high-fare-trips")
def high_fare_trips():
    _ = request.args.get("area_id", type=int)
    _ = request.args.get("min_fare", type=float)
    return todo_response("/high-fare-trips")


@app.get("/co-area-drivers")
def co_area_drivers():
    _ = request.args.get("driver_id")
    return todo_response("/co-area-drivers")


@app.get("/avg-fare-by-company")
def avg_fare_by_company():
    return todo_response("/avg-fare-by-company")


@app.get("/area-stats")
def area_stats():
    _ = request.args.get("area_id", type=int)
    return todo_response("/area-stats")


@app.get("/top-pickup-areas")
def top_pickup_areas():
    _ = request.args.get("n", default=5, type=int)
    return todo_response("/top-pickup-areas")


@app.get("/company-compare")
def company_compare():
    _ = request.args.get("company1")
    _ = request.args.get("company2")
    return todo_response("/company-compare")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
