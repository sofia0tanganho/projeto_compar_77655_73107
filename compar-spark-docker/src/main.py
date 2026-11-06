import os
from pyspark.sql import SparkSession, functions as F
import argparse
import glob
from functools import reduce

from m1 import flag_invalid, cleaning_stats, clean
import m2
import m3
import o1, o2
from profiling import Profiler

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
    p.add_argument("--write-output", action="store_true",
                   help="write real files; default uses the noop sink")
    p.add_argument("--label", default="run")
    p.add_argument("--cache-flows", action="store_true")
    p.add_argument("--flows-partitions", type=int, default=0)
    p.add_argument("--broadcast-stats", action="store_true")
    return p.parse_args()

def run(df, path, write):
    if write:
        df.write.mode("overwrite").parquet(path)
    else:
        df.write.format("noop").mode("overwrite").save()

def main():
    args = parse_args()
    os.makedirs("spark-events", exist_ok=True)
    spark = (SparkSession.builder.appName("taxi-pipeline")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.eventLog.enabled", "true")
             .config("spark.eventLog.dir", "spark-events")
             .getOrCreate())
    profiler = Profiler(spark, args.label, f"{args.output_path}/metrics",
                        {"years": args.years, "fraction": args.fraction})
    trips = load_trips(spark, args.input_path, args.years, args.fraction)
    trips.printSchema()
    zones = load_zones(spark, args.zone_lookup)

    #M1 related 
    profiler.start("M1")
    flagged = flag_invalid(trips, args.years)
    stats = cleaning_stats(flagged)
    stats.show()
    stats.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{args.output_path}/m1_stats")

    trips_clean = clean(flagged)
    print("clean rows:", trips_clean.count())
    profiler.stop()

    #M2 related
    profiler.start("M2")
    demand = m2.build(trips_clean, zones)
    m2.write(demand, args.output_path)
    profiler.stop()

    #M3 related
    profiler.start("M3")
    zone_stats = m3.build(trips_clean, zones)
    run(zone_stats, f"{args.output_path}/m3_zone_month", args.write_output)
    profiler.stop()

    #O1 related
    profiler.start("O1")
    base = o1.addFarePerKm(trips_clean)
    flagged1 = o1.flagAnomalies(base, o1.bucketStats(base), broadcast=args.broadcast_stats)
    rates = o1.anomaly_rates(flagged1, zones)
    run(rates, f"{args.output_path}/o1_rates", args.write_output)
    profiler.stop()

    #O2 related
    profiler.start("O2")
    flows = o2.build_monthly_flows(trips_clean, zones)
    if args.flows_partitions > 0:
        flows = flows.coalesce(args.flows_partitions)
    if args.cache_flows:                
        flows.cache()
    top20 = o2.top_routes(flows, 20)
    reversals = o2.share_reversals(flows)
    o2.write(flows, top20, reversals, args.output_path if args.write_output else None)
    if args.cache_flows:                 
        flows.unpersist()
    profiler.stop()

    profiler.report()
    spark.stop()


if __name__ == "__main__":
    main()