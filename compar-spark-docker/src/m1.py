from pyspark.sql import functions as F

FLAGS = ["f_fare", "f_dist", "f_pax", "f_time", "f_zone"]

REQUIRED_FIELDS = [
    "fare_amount",
    "trip_distance",
    "passenger_count",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
]

def _nsafe(cond):
    return F.coalesce(cond, F.lit(False))


def flag_invalid(df):

    missing= [c for c in REQUIRED_FIELDS if c not in df.columns]
    if missing:
        raise ValueError(f"Input DataFrame is missing required columns: {missing}")



    flagged = (df
        .withColumn("f_fare", _nsafe(F.col("fare_amount") <= 0))
        .withColumn("f_dist", _nsafe(F.col("trip_distance") <= 0))
        .withColumn("f_pax",  _nsafe((F.col("passenger_count") == 0) |
                                     (F.col("passenger_count") > 6)))
        .withColumn("f_time", _nsafe(F.col("tpep_dropoff_datetime") <=
                                     F.col("tpep_pickup_datetime")))
        .withColumn("f_zone", _nsafe(~F.col("PULocationID").between(1, 263) |
                                     ~F.col("DOLocationID").between(1, 263))))
    
    
    
    # any_flag = F.col(FLAGS[0])
    # for f in FLAGS[1:]:
    #     any_flag = any_flag | F.col(f)
    # return flagged.withColumn("f_any", any_flag)
    null_condition = F.lit(False)
    for field in REQUIRED_FIELDS:
        null_condition = null_condition | F.col(field).isNull()

    flagged = flagged.withColumn("f_null", null_condition)

    any_condition = F.col("f_null")
    for flag in FLAGS:
        any_condition = any_condition | F.col(flag)

    return flagged.withColumn("f_any", any_condition)


def cleaning_stats(flagged):
    expected_flags = FLAGS + ["f_null", "f_any"]
    missing = [c for c in ["tpep_pickup_datetime"] + expected_flags
               if c not in flagged.columns]
    if missing:
        raise ValueError(f"Flagged DataFrame is missing columns: {missing}")

    return (
        flagged
        .groupBy(F.year("tpep_pickup_datetime").alias("year"))
        .agg(
            F.count("*").alias("total"),
            *[
                F.sum(F.col(flag).cast("long")).alias("removed_" + name)
                for flag, name in [
                    ("f_fare", "fare"),
                    ("f_dist", "dist"),
                    ("f_pax", "pax"),
                    ("f_time", "time"),
                    ("f_zone", "zone"),
                    ("f_null", "null"),
                    ("f_any", "any"),
                ]
            ],
        )
        .orderBy("year")
    )


def clean(flagged):
    return flagged.filter(~F.col("f_any")).drop(*FLAGS, "f_any")