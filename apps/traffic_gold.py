from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Spark Session Config

spark = (
    SparkSession.builder
    .appName("TrafficGoldLayer")
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

# Read Silver Silver

silver_stream = (
    spark.readStream
    .format("delta")
    .load("/opt/spark/warehouse/traffic_silver")
)

# Dimension Zone - We are creating dimension table for city zone column

dim_zone = silver_stream.select(
    "city_zone"
).dropDuplicates() \
.withColumn(
    "zone_type",
    # We create new column called zone_type
    # When city_zone == "CBD" then zone_type should be commercial, same for below techpark, etc
    when(col("city_zone") == "CBD", "Commercial")
    .when(col("city_zone") == "TECHPARK", "IT HUB")
    .when(col("city_zone").isin("AIRPORT", "TRAINSTATION"), "Transit Hub")
    .otherwise("Residential")
) \
.withColumn(
    "traffic_risk",
    # Create another column called traffic_risk
    # When city_zone == "CBD" or "AIRPORT" , etc then traffic_risk is high, same for others
    when(col("city_zone").isin("CBD", "AIRPORT", "TRAINSTATION"), "HIGH")
    .when(col("city_zone") == "TECHPARK", "MEDIUM")
    .otherwise("LOW")
)

zone_query = (
    dim_zone.writeStream
    .format("delta")
    .outputMode("append")
    # We save this into below path and a dimension table dim_zone will be created there, (it will create folder \
    # dim_zone once this script will run)
    .option("checkpointLocation", "/opt/spark/warehouse/chk/dim_zone")
    .option("path", "/opt/spark/warehouse/dim_zone")
    .start()
)

# Dimension Road - Here we are creating dimension table for road_id

dim_road = silver_stream.select(
    "road_id"
).dropDuplicates() \
.withColumn(
    "road_type",
    # Create a new column road_type
    # When road_id is R100 | R200 then its a 'highway' otherwise remaining road_id is 'City road'
    when(col("road_id").isin("R100", "R200"), "Highway")
    .otherwise("City Road")
) \
.withColumn(
    "speed_limit",
    # Same here we create column speed_limit and give limits to certain roads
    when(col("road_id").isin("R100", "R200"), 100)
    .otherwise(60)
)

road_query = (
    dim_road.writeStream
    .format("delta")
    .outputMode("append")
    # dim_road table will be saved in below path
    .option("checkpointLocation", "/opt/spark/warehouse/chk/dim_road")
    .option("path", "/opt/spark/warehouse/dim_road")
    .start()
)

# Fact Table

fact_stream = silver_stream.select(
    "vehicle_id",
    "road_id",
    "city_zone",
    # We ignored original 'speed' column here, because it was string datatype, we took 'speed_int' here because its int
    "speed_int",
    "congestion_level",
    # Same for below 'event_ts' because it has timestamp datatype which is correct unlike the original event_time \
    # which was string datatype
    "event_ts",
    "peak_flag",
    "speed_band",
    "hour",
    "weather"
    # speed_valid and time_valid isnt included because those are just flags which were already used for filtering data
)

# Added another column date to get date of event generated, but it wont matter because every event will just have one \
# date because it will have just todays stream, if for analytical purpose on multiple days, just ran stream on \
# different days, for some time, to compare
fact_enriched = fact_stream.withColumn("date", to_date("event_ts"))

fact_query = (
    fact_enriched.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/fact_traffic")
    .option("path", "/opt/spark/warehouse/fact_traffic")
    .start()
)

spark.streams.awaitAnyTermination()