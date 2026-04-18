from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Spark Session Config

spark = (
    SparkSession.builder
    .appName("TrafficSilverLayer")  # Here we provide another name for application as this is different process, this \
    # application will process the silver stage data, coming from delta table of bronze
    # cluster master
    .master("spark://spark-master:7077")
    # delta lake
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read Bronze Stream

bronze_df = (
    spark.readStream
    # Unlike in bronze where we provide format as kafka as data is coming from kafka topic, here we provide delta \
    # because we basically reading data from delta table now
    .format("delta")
    # This will read data from delta table of traffic_bronze, basically from write path of bronze
    .load("/opt/spark/warehouse/traffic_bronze")
)

# Data Quality Flag

dq_df = bronze_df.withColumn(
    # Here it will create col dq_flag, when col 'vehicle_id', 'event_time' is null, it will insert value \
    # "MISSING_VEHICLE" or "MISSING_TIME" respectively or if col raw_json contains string "CORRUPTED" then insert \
    # value "CORRUPT_JSON", otherwise OK
    "dq_flag",
    when(col("vehicle_id").isNull(), "MISSING_VEHICLE")
    .when(col("event_time").isNull(), "MISSING_TIME")
    .when(col("raw_json").contains("CORRUPTED"), "CORRUPT_JSON")
    .otherwise("OK")
)

# Safe Type Casting

typed = dq_df.withColumn(
    # Here we create a col to get only safe speed values, the column 'speed' which we created earlier will be \
    # converted here to int datatype and name it as "speed_int", so speed_int will have all the accurate values
    "speed_int",
    (col("speed").cast("int"))
).withColumn(
    # Same here we create a col "event_ts", the column "event_time" is converted into timestamp datatype and name it \
    # "event_ts"
    "event_ts",
    to_timestamp("event_time")
)

# Business Validation Rules
# This is something which can be asked by stakeholders to include it in dataframe to validate their needs

validated = typed.withColumn(
    # Here we create a column "speed_valid" to check if speed_int has values in a specific range, if it has value in \
    # given range then it will print 1, else if out of range then 0
    "speed_valid",
    when((col("speed_int") >= 0) & (col("speed_int") <= 160), 1).otherwise(0)
    # Another column time_valid to check if any future event is being processed accidentally so for safe side a 10 min \
    # interval is added, if its less than that then fine otherwise fault data
).withColumn(
    "time_valid",
    when(col("event_ts") <= current_timestamp() + expr("INTERVAL 10 MINUTES"), 1).otherwise(0)
)

# Filter Good Records

# Here we filter out only good records so if for eg, dq_flag == 'MISSING_VEHICLE' or 'MISSING_TIME' then skip those \
# rows and where dq_flag == 'OK' take only those rows
# Same for speed_valid and time_valid

# noinspection PyTypeChecker
clean_stream = validated.filter(
    (col("dq_flag") == "OK") &
    (col("speed_valid") == 1) &
    (col("time_valid") == 1)
)

# Handle Late Data

# Here we accept only those data which is under 15 minute mark from event generated time from current time, that means \
# data arrives in kafka and if that data is 15 mins late than current time stamp (which is handled by .withWatermark) \
# i.e current timestamp == 02:47, event gets generated event_ts == 02:32 then its flagged as late

watermarked = clean_stream.withWatermark("event_ts", "15 minutes")

# Deduplication

# Here we take those columns which cannot have duplicate fields, for eg: vehicle_id must be unique, if there are 2 \
# vehicle with same IDs then its faulty data
# vehicle_id is not vehicle_no

deduped = watermarked.dropDuplicates(
    ["vehicle_id", "event_ts"]
)

# Feature Engineering

# This is just to implement some features, we can describe this as additional features client has required to \
# incorporate

silver_final = (
    deduped
    # 'hour' col to get hour mark from event_ts field
    .withColumn("hour", hour("event_ts"))
    # 'peak_flag' col is to get '1' if hour is between 8 to 11 in morning OR 17 to 20 in evening else 0
    # This will give us all the info and analysis of traffic in peak hours, we can filter through peak hours to get \
    # this info
    .withColumn("peak_flag",
                when((col("hour").between(8, 11)) |
                     (col("hour").between(17, 20)), 1).otherwise(0))
    # 'speed_band' to get direct info on cars speed categorised as low, medium, high
    .withColumn("speed_band",
                when(col("speed_int") < 30, "LOW")
                .when(col("speed_int") < 70, "MEDIUM")
                .otherwise("High"))
)
# Write Silver Table

silver_query = (
    silver_final.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_silver")
    .option("path", "/opt/spark/warehouse/traffic_silver")
    .start()
)

spark.streams.awaitAnyTermination()

# This will keep running file in powershell, it will be complete when _delta_log folder is created under \
# traffic_silver folder and files are created in traffic_silver folder

# Further we will make a gold layer python file where all the dimension tables and fact table are made and write them \
# to their folders
