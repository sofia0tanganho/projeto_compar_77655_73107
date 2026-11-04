from pyspark.sql import functions as F
from pyspark.sql import Window

def build_monthly_flows(trips, zones):
    monthly= (trips
              .withColumn("year", F.year("tpep_pickup_datetime"))
              .withColumn("month", F.month("tpep_pickup_datetime"))
              .groupBy("year","month", "PULocationID", "DOLocationID")
              .agg(F.count("*").alias("trips"))
              )
    
    outbound= (monthly.groupBy("year", "month", "PULocationID").agg(F.sum("trips").alias("outbound_trips")))

    flows= (monthly
            .join(outbound, ["year", "month", "PULocationID"])
            .withColumn("route_share", F.col("trips")/F.col("outbound_trips"))
            )
    pickup = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Zone").alias("pickup_zone"),
        F.col("Borough").alias("pickup_borough"),
    )

    dropoff = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("Borough").alias("dropoff_borough"),
    )

    return (
        flows.join(pickup, "PULocationID", "left")
        .join(dropoff, "DOLocationID", "left")
    )


def top_routes(flows, n=20):
    window= Window.partitionBy("year", "month", "pickup_borough").orderBy(
        F.desc("trips")
    )
    return (flows.withColumn("route_rank", F.row_number().over(window)).filter(F.col("route_rank")<=n))

def share_reversals(flows):
    route_years= flows.select("year", "PULocationID", "DOLocationID", "pickup_zone", "pickup_borough", "dropoff_zone","dropoff_borough").distinct()

    months= (route_years.select("year").distinct().withColumn("month", F.explode(F.sequence(F.lit(1), F.lit(12)))))

    grid= route_years.join(months, "year")

    monthly_shares= flows.select("year", "month", "PULocationID", "DOLocationID","route_share")

    complete= (grid.join(monthly_shares, ["year", "month", "PULocationID", "DOLocationID"], "left").fillna({"route_share": 0.0}))

    changes= (complete.groupBy("year", "PULocationID", "DOLocationID","pickup_zone", "pickup_borough", "dropoff_zone", "dropoff_borough")
                    .agg(
                    F.max(F.when(F.col("month") == 1, F.col("route_share"))).alias("jan_share"),
                    F.max(F.when(F.col("month") == 3, F.col("route_share"))).alias("mar_share"),
                    F.max(F.when(F.col("month") == 7, F.col("route_share"))).alias("jul_share"),
                    F.max(F.when(F.col("month") == 9, F.col("route_share"))).alias("sep_share"),
                )

                .withColumn("q1_change", F.col("mar_share") - F.col("jan_share"))
                .withColumn("q3_change", F.col("sep_share") - F.col("jul_share"))
                .withColumn("share_reversal",F.col("q1_change") * F.col("q3_change") < 0)
            )
    return changes.filter(F.col("share_reversal"))

def write(flows, top20, reversals, out_dir=None):
    for name, df in [("o2_monthly_flows", flows), ("o2_top20_routes", top20),("o2_share_reversals", reversals)]:
        if out_dir:
            df.write.mode("overwrite").parquet(f"{out_dir}/{name}")
        else:
            df.write.format("noop").mode("overwrite").save()