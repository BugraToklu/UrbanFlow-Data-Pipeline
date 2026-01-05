from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_TOPIC = "city_traffic"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
POSTGRES_URL = "jdbc:postgresql://urbanflow-db:5432/urbanflow"
POSTGRES_PROPERTIES = {
    "user": "admin",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(df, epoch_id):
    print(f"Writing Batch {epoch_id} to database... Record Count: {df.count()}")

    df.write \
        .jdbc(url=POSTGRES_URL, table="traffic_data", mode="append", properties=POSTGRES_PROPERTIES)

def main():
    spark = SparkSession.builder \
        .appName("UrbanFlowStream") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("taxi_id", StringType()),
        StructField("event_time", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("speed", DoubleType()),
        StructField("status", StringType())
    ])

    print("Listening to Kafka...")
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))

    query = df_parsed.writeStream \
        .foreachBatch(write_to_postgres) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()