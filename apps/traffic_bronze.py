from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Spark Session Config

spark = (
    SparkSession.builder  # Opening spark session using SparkSession.builder.appname
    .appName("TrafficStreamingLakehouse")  # This will be our appname, we can see it in spark master UI
    # cluster master
    .master("spark://spark-master:7077")  # We are connecting this spark session to spark master using this port
    # delta lake
    # Because we are going to save data in Delta Lake format we need this package "spark.sql.extensions"
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)
#   This will show logs only which has more severity, any normal logs which doesnt pose any threat will be ignored \
#   otherwise CLI will get flooded
spark.sparkContext.setLogLevel("WARN")

#   Read Kafka Raw Stream

raw_stream = (
    # Used readStream because we're reading stream if it were a flat file like csv then spark.read
    spark.readStream
    # We provide here format kafka because we are reading data from kafka offset
    .format("kafka")
    # We used 9092 port here because this py file running inside spark container and spark container is inside docker \
    # container
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "traffic-topic")
    # We have 2 options in reading offsets.
    # Offsets are just serialize feed in kafka, basically it is just similar to LSN, its just LSN is in source and \
    # offset is in kafka
    # Now here option 'latest' means, if this code gets interrupted and we have to run it again it will simply pick \
    # after the last offset value which was read, so if 0,1,2 is read > code interrupts and restarts > 3,4,5 and so on..
    # Now in option 'earliest' means, if this code gets interrupted it will start reading again from starting offset, \
    # so 0,1,2 is read > code gets interrupted and restarts > 0,1,2,3,4,5
    # This 'earliest' option will create duplicates
    .option("startingOffsets", "latest")
    # This will read data from kafka topic 'traffic-topic'
    .load()
)

#   Now above will read stream of data that is saved in kafka from topic 'traffic-topic' from listener port 9092 which \
#   internal listener port with offset option as 'latest'

#   Convert Binary to String

#   Below code will convert above binary input 'raw_stream' which we are constantly getting from source to string for \
#   operating as dataframe with 2 cols raw_json, kafka_timestamp
#   Change data type to string by using cast through selectExpr function where we can use SQL query and return a value \
#   and assign it to column raw_json
#   For timestamp just a name change, here kafka_timestamp is a timestamp when kafka receives the data, not the time \
#   when data is generated at source/CLI
#   Earlier when it was serialized using lambda into a binary record, now we de-serialize it and entire record now as \
#   a string is whole one record with its timestamp in another col, so this both is one record

json_stream = raw_stream.selectExpr(
    "CAST(value AS STRING) as raw_json",
    "timestamp as kafka_timestamp"
)

# Flexible Schema

traffic_schema = StructType([  # StructType to define schema
    StructField("vehicle_id", StringType()),
    StructField("road_id", StringType()),
    StructField("city_zone", StringType()),
    # Here below speed we have given datatype as stringtype because along with int type data, it also has faulty data \
    # which we gave it in traffic_dirty_producer.py
    StructField("speed", StringType()),
    StructField("congestion_level", IntegerType()),
    StructField("weather", StringType()),
    StructField("event_time", StringType())
])

#   Below to incorporate our schema and cols we create another col named data in which parsed/serialize values will be \
#   there, so now we have 3 cols raw_json, kafka_timestamp, data

#   So now one record will consist of a raw_json with all values, a timestamp and values with schema

parsed = (json_stream.withColumn
          ("data", from_json(col("raw_json"), traffic_schema))
          )

#   Now below we separate the field with schema in data column to their corresponding values as column
#   Here data.* will split all values in data column as a separate column and data col will be gone and will be \
#   replaced by multiple cols

flattened = parsed.select(
    "raw_json",
    "kafka_timestamp",
    "data.*"
)

#   Bronze Delta Write
#   Here we now write our data to delta table

bronze_query = (
    flattened.writeStream
    .format("delta")
    .outputMode("append")
    # This is just like offset where it checks till what point data is written and at what point needs to write again
    # /opt/spark/warehouse/ we take this path from spark master container in YML and add chk/traffic_bronze
    # So complete path "/opt/spark/warehouse/chk/traffic_bronze"
    .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_bronze")
    # This is to save actual data into path, just remove chk from above path
    .option("path", "/opt/spark/warehouse/traffic_bronze")
    .start()
)

#   When kafka streams data, i.e lets say it streamed one record then process will get terminated automatically, \
#   it wont wait for new record, so after implementing this line, it will wait for next stream record and will keep \
#   on waiting until we manually interrupt by command
spark.streams.awaitAnyTermination()
