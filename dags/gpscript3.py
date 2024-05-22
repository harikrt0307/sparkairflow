from pyspark.sql import SparkSession
import requests
from io import StringIO

# Create a SparkSession
spark = SparkSession.builder.appName("InsertIntoGreenPlum").getOrCreate()

# Configure GreenPlum connection details
greenplum_host = "172.19.33.16"
greenplum_port = "5432"
greenplum_database = "gpadmin"
greenplum_user = "gpadmin"
greenplum_password = "gpadmin"

# Download the CSV file from the GitHub URL
excel_file_url = "https://github.com/harikrt0307/sparkairflow/raw/main/dags/data1.csv"
response = requests.get(excel_file_url)
csv_data = response.text

# Create an RDD from the CSV data
csv_rdd = spark.sparkContext.parallelize([csv_data])

# Convert the RDD to a DataFrame
df = spark.read.csv(csv_rdd.wholeTextFiles().map(lambda x: x[1]), header=True, inferSchema=True)

# Write the DataFrame to the GreenPlum table
jdbc_url = f"jdbc:postgresql://{greenplum_host}:{greenplum_port}/{greenplum_database}"

df.write.format("jdbc") \
   .option("url", jdbc_url) \
   .option("dbtable", "airflow_test") \
   .option("user", greenplum_user) \
   .option("password", greenplum_password) \
   .mode("append") \
   .save()

# Stop the SparkSession
spark.stop()
