import kafka
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
    .appName("Kafka-PySpark-Integration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()

# InfluxDB Configuration
influxdb_url = "https://us-east-1-1.aws.cloud2.influxdata.com/api/v2/write?org=61936fbb21363b48&bucket=case_study&precision=s"
influxdb_token = "wqMtYys22CDmZBNZ62qD-VGDP4R-z1IODYJBYRPzTWrDsTrCnsS5jPFJd03eY6Odn2FqCW_KhnC8oRyDPIQi_g=="

# Define Kafka Variables
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "pop"

# Initialize a set to track processed records
processed_records = set()

# Define schema based on the new JSON structure
schema = StructType([
    StructField("data", ArrayType(
        StructType([
            StructField("ID Nation", StringType(), True),
            StructField("Nation", StringType(), True),
            StructField("ID Year", IntegerType(), True),
            StructField("Year", StringType(), True),
            StructField("Population", IntegerType(), True),
            StructField("Slug Nation", StringType(), True)
        ])
    ), True),
    StructField("source", ArrayType(
        StructType([
            StructField("measures", ArrayType(StringType()), True),
            StructField("annotations", StructType([
                StructField("source_name", StringType(), True),
                StructField("source_description", StringType(), True),
                StructField("dataset_name", StringType(), True),
                StructField("dataset_link", StringType(), True),
                StructField("table_id", StringType(), True),
                StructField("topic", StringType(), True),
                StructField("subtopic", StringType(), True)
            ]), True),
            StructField("name", StringType(), True),
            StructField("substitutions", ArrayType(StringType()), True)
        ])
    ), True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Cast binary data to string
value_df = df.selectExpr("CAST(value AS STRING)")

# Apply schema to parse JSON
structured_df = value_df.select(from_json(col("value"), schema).alias("parsed")).select("parsed.*")

# Flatten the data array
flattened_df = structured_df.select(
    col("data")
).withColumn("nation_data", explode(col("data")))  # Flatten the data array

# Extract fields from nation_data
final_df = flattened_df.select(
    col("nation_data.`ID Nation`").alias("ID_Nation"),
    col("nation_data.Nation"),
    col("nation_data.`ID Year`").alias("ID_Year"),
    col("nation_data.Year"),
    col("nation_data.Population"),
    col("nation_data.`Slug Nation`").alias("Slug_Nation")
)

# Function to write the data to InfluxDB
def write_to_influxdb(data):
    # Use Population as a unique identifier
    unique_id = f"{data.get('Year')}-{data.get('Population')}"
    if unique_id in processed_records:
        return  # Skip if already processed

    # After successfully writing, add the unique ID to the processed set
    processed_records.add(unique_id)

    # Measurement name
    measurement = "population_data"

    # Prepare field sets
    fields = [
        f"ID_Nation=\"{data['ID_Nation']}\"",
        f"Nation=\"{data['Nation']}\"",
        f"ID_Year={data['ID_Year']}",
        f"Year=\"{data['Year']}\"",
        f"Population={data['Population']}",
        f"Slug_Nation=\"{data['Slug_Nation']}\""
    ]

    fields_str = ','.join(fields)

    # Combine into line protocol format
    line_protocol_data = f"{measurement} {fields_str}"

    # Prepare headers
    headers = {
        "Authorization": f"Token {influxdb_token}",
        "Content-Type": "text/plain; charset=utf-8",
    }

    # Send the line protocol data to InfluxDB
    response = requests.post(influxdb_url, data=line_protocol_data, headers=headers)
    
    # Tracking the request result
    if response.status_code != 204:
        print(f"Error writing to InfluxDB: {response.text}")
    else:
        print('Done!')

# Function to process each batch of data
def process_batch(batch_df, batch_id):
    for row in batch_df.collect():
        # Convert row to dictionary
        data = row.asDict()
        # Write each record to InfluxDB
        write_to_influxdb(data)

# Process the streaming data
query = final_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

# Await termination
query.awaitTermination()

# Stop the Spark session
spark.stop()

