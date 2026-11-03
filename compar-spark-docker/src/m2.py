from pyspark.sql import functions as F

def build(trips, zones):
    pickup_zone = zones.select(F.col("LocationID").alias("PULocationID"),
                               F.col("Borough").alias("pickup_borough"))
    with_borough = trips.join(pickup_zone, "PULocationID")

    with_cols = (with_borough
        .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
        .withColumn("duration_min",
                    (F.unix_timestamp("tpep_dropoff_datetime") -
                     F.unix_timestamp("tpep_pickup_datetime")) / 60.0))

    return (with_cols
        .groupBy("pickup_borough", "day_of_week", "hour_of_day")
        .agg(F.count("*").alias("trips"),
             F.avg("fare_amount").alias("avg_fare"),
             F.avg("duration_min").alias("avg_duration_min"),
             F.avg("trip_distance").alias("avg_distance_mi")))


def write(demand, out_dir):
    demand.write.mode("overwrite").partitionBy("pickup_borough").parquet(f"{out_dir}/m2_demand")