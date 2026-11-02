import sys
import importlib
from pyspark.sql import SparkSession, functions as F
import argparse
import glob
from functools import reduce

from m1 import flag_invalid, cleaning_stats, clean
COLUMNS = {
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "PULocationID": "int",
    "DOLocationID": "int",
    "passenger_count": "int",
    "trip_distance": "double",
    "RatecodeID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "tip_amount": "double",
    "total_amount": "double",
}

def enforce_schema(df):
    return df.select([F.col(c).cast(t).alias(c) for c, t in COLUMNS.items()])


def load_zones(spark, path):
    return (spark.read.option("header", True).csv(path)
            .select(F.col("LocationID").cast("int"),
                    "Borough", "Zone", "service_zone"))
def load_trips(spark, input_path, years, fraction=1.0):
    files = []
    for y in years:
        files += sorted(glob.glob(f"{input_path}/yellow_tripdata_{y}-*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {input_path} for {years}")
    dfs = [enforce_schema(spark.read.parquet(f)) for f in files]
    df = reduce(lambda a, b: a.unionByName(b), dfs)
    if fraction < 1.0:
        df = df.sample(fraction=fraction, seed=42)
    return df

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input-path", required=True)
    p.add_argument("--zone-lookup", required=True)
    p.add_argument("--years", type=int, nargs="+", default=[2025])
    p.add_argument("--fraction", type=float, default=1.0)
    p.add_argument("--output-path", default="output")
    return p.parse_args()


def main():
    args = parse_args()
    spark = (SparkSession.builder.appName("taxi-pipeline")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    trips = load_trips(spark, args.input_path, args.years, args.fraction)
    trips.printSchema()
    flagged = flag_invalid(trips)
    stats = cleaning_stats(flagged)
    stats.show()
    stats.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{args.output_path}/m1_stats")

    trips_clean = clean(flagged)
    print("clean rows:", trips_clean.count())
    spark.stop()


if __name__ == "__main__":
    main()