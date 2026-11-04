from pyspark.sql import Window, functions as F

deviation_sigmas = 1.5


def build(trips, zones):
    zone_info = zones.select(F.col("LocationID").alias("PULocationID"),
                             F.col("Zone").alias("zone"),
                             F.col("Borough").alias("borough"))
    monthly = (trips
        .withColumn("year", F.year("tpep_pickup_datetime"))
        .withColumn("month", F.month("tpep_pickup_datetime"))
        .groupBy("PULocationID", "year", "month")
        .agg(F.count("*").alias("trips"),
             F.avg("fare_amount").alias("avg_fare"),
             F.stddev("fare_amount").alias("std_fare"),
             F.avg("trip_distance").alias("avg_distance_mi"),
             F.stddev("trip_distance").alias("std_distance_mi"))
        .join(zone_info, "PULocationID")
        .withColumn("month_idx", F.col("year") * 12 + F.col("month")))  

    by_borough_month = Window.partitionBy("borough", "year", "month").orderBy(F.desc("trips"))
    zone_3m = Window.partitionBy("PULocationID").orderBy("month_idx").rangeBetween(-2, 0)
    borough_3m = Window.partitionBy("borough").orderBy("month_idx").rangeBetween(-2, 0)
    zone_prev_3m = Window.partitionBy("PULocationID").orderBy("month_idx").rangeBetween(-3, -1)

    return (monthly
        .withColumn("volume_rank_in_borough", F.rank().over(by_borough_month))
        .withColumn("rolling_3m_share",
                    F.sum("trips").over(zone_3m) / F.sum("trips").over(borough_3m))
        .withColumn("prev_3m_mean_trips", F.avg("trips").over(zone_prev_3m))
        .withColumn("prev_3m_std_trips", F.stddev("trips").over(zone_prev_3m))
        .withColumn("volume_anomaly", F.coalesce(
            F.abs(F.col("trips") - F.col("prev_3m_mean_trips")) >
            deviation_sigmas * F.col("prev_3m_std_trips"),
            F.lit(False)))
        .drop("month_idx"))  


def write(result, out_dir):
    result.write.mode("overwrite").parquet(f"{out_dir}/m3_zone_month")