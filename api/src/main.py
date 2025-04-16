from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession
from typing import List, Dict, Any
import json
from datetime import datetime, timedelta

app = FastAPI(
    title="Web Traffic Analytics API",
    description="API for accessing processed web traffic data",
    version="1.0.0",
)

# Initialize Spark session
spark = SparkSession.builder.appName("WebTrafficAPI").getOrCreate()


@app.get("/")
async def root():
    """Root endpoint returning API information."""
    return {
        "name": "Web Traffic Analytics API",
        "version": "1.0.0",
        "endpoints": [
            "/metrics/endpoints",
            "/metrics/ip/{ip}",
            "/metrics/top-ips",
            "/metrics/response-times",
        ],
    }


@app.get("/metrics/endpoints")
async def get_endpoint_metrics(hours: int = 24) -> List[Dict[str, Any]]:
    """
    Get metrics for all endpoints in the last N hours.

    Args:
        hours: Number of hours to look back (default: 24)
    """
    try:
        # Calculate time range
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        # Read and filter data
        df = spark.read.parquet("data/processed/metrics")
        filtered_df = df.filter(
            (df.window.start >= start_time) & (df.window.end <= end_time)
        )

        # Aggregate metrics
        metrics = (
            filtered_df.groupBy("endpoint")
            .agg({"request_count": "sum", "avg_response_time": "avg"})
            .collect()
        )

        return [
            {
                "endpoint": row["endpoint"],
                "total_requests": row["sum(request_count)"],
                "avg_response_time": row["avg(avg_response_time)"],
            }
            for row in metrics
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/ip/{ip}")
async def get_ip_metrics(ip: str, hours: int = 24) -> Dict[str, Any]:
    """
    Get metrics for a specific IP address.

    Args:
        ip: IP address to query
        hours: Number of hours to look back (default: 24)
    """
    try:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        df = spark.read.parquet("data/processed/logs")
        filtered_df = df.filter(
            (df.timestamp >= start_time) & (df.timestamp <= end_time) & (df.ip == ip)
        )

        metrics = filtered_df.agg(
            {"*": "count", "response_time": "avg", "status_code": "count"}
        ).collect()[0]

        return {
            "ip": ip,
            "total_requests": metrics["count(1)"],
            "avg_response_time": metrics["avg(response_time)"],
            "success_rate": metrics["count(status_code)"] / metrics["count(1)"] * 100,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/top-ips")
async def get_top_ips(limit: int = 10, hours: int = 24) -> List[Dict[str, Any]]:
    """
    Get top IP addresses by request count.

    Args:
        limit: Number of IPs to return (default: 10)
        hours: Number of hours to look back (default: 24)
    """
    try:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        df = spark.read.parquet("data/processed/logs")
        filtered_df = df.filter(
            (df.timestamp >= start_time) & (df.timestamp <= end_time)
        )

        top_ips = (
            filtered_df.groupBy("ip")
            .count()
            .orderBy("count", ascending=False)
            .limit(limit)
            .collect()
        )

        return [{"ip": row["ip"], "request_count": row["count"]} for row in top_ips]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/response-times")
async def get_response_times(hours: int = 24) -> Dict[str, Any]:
    """
    Get response time statistics.

    Args:
        hours: Number of hours to look back (default: 24)
    """
    try:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        df = spark.read.parquet("data/processed/logs")
        filtered_df = df.filter(
            (df.timestamp >= start_time) & (df.timestamp <= end_time)
        )

        stats = filtered_df.agg(
            {"response_time": ["min", "max", "avg", "stddev"]}
        ).collect()[0]

        return {
            "min_response_time": stats["min(response_time)"],
            "max_response_time": stats["max(response_time)"],
            "avg_response_time": stats["avg(response_time)"],
            "stddev_response_time": stats["stddev(response_time)"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
