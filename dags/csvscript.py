from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CSV to DataFrame") \
    .getOrCreate()

# Define the schema for the CSV file
schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("att_date", StringType(), True),
    StructField("actual_intime", StringType(), True),
    StructField("actual_outtime", StringType(), True),
    StructField("regular_intime", StringType(), True),
    StructField("regular_outtime", StringType(), True),
    StructField("location", StringType(), True),
    StructField("attendance", StringType(), True)
])

# Path to the CSV file
csv_file_path = "/mnt/data/data.csv"

# Read the CSV file into a DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(csv_file_path)

# Show the DataFrame
df.show()

# Print the schema
df.printSchema()

# Stop the SparkSession
spark.stop()
