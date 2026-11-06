from pyspark.sql import functions as F

kmPerMile= 1.61
sigma= 3.0

def addFarePerKm(trips):
    return(trips
           .withColumn("fare_per_km", F.col("fare_amount")/ (F.col("trip_distance")* kmPerMile))
           .withColumn("hour", F.hour("tpep_pickup_datetime"))
           .withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin(1, 7))
           )

def bucketStats(df):
    return(df.groupBy("PULocationID", "hour").agg(F.mean("fare_per_km").alias("mean_fpk"), F.stddev("fare_per_km").alias("std_fpk")))

def flagAnomalies(df, stats, broadcast = False):
    if broadcast:
        stats = F.broadcast(stats)
    joined= df.join(stats, on=["PULocationID", "hour"], how="left")
    return joined.withColumn("is_anomaly", F.coalesce(F.col("fare_per_km")> F.col("mean_fpk") + sigma * F.col("std_fpk"), F.lit(False)))


def anomaly_rates(flagged, zones):
    zonesSelected = zones.select(F.col("LocationID").alias("PULocationID"), F.col("Borough").alias("pickup_borough"))
    return (flagged.join(zonesSelected, on="PULocationID", how= "left").groupBy("pickup_borough", "payment_type", "is_weekend")
            .agg(F.count("*").alias("trips"), F.sum(F.col("is_anomaly").cast("int")).alias("anomalies"))
            .withColumn("anomaly_rate", F.col("anomalies")/ F.col("trips"))
            .orderBy("pickup_borough", "payment_type", "is_weekend")
            )