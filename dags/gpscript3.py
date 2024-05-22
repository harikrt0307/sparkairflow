from pyspark.sql import SparkSession
import tempfile
import requests
import os

# Create a SparkSession
spark = SparkSession.builder.appName("InsertIntoGreenPlum").getOrCreate()

# Configure GreenPlum connection details
greenplum_host = "172.19.33.16"
greenplum_port = "5432"
greenplum_database = "gpadmin"
greenplum_user = "gpadmin"
greenplum_password = "gpadmin"

# Download the CSV file from the GitHub URL to a temporary location
excel_file_url = "https://github.com/harikrt0307/sparkairflow/raw/main/dags/data1.csv"
temp_dir = tempfile.gettempdir()
temp_file_path = os.path.join(temp_dir, "data1.csv")

response = requests.get(excel_file_url)
with open(temp_file_path, "wb") as file:
    file.write(response.content)

# Read data from the temporary CSV file into a DataFrame
df = spark.read.option("header", "true").option("inferSchema", "true").csv(temp_file_path)

# Write the DataFrame to the GreenPlum table
jdbc_url = f"jdbc:postgresql://{greenplum_host}:{greenplum_port}/{greenplum_database}"

df.write.format("jdbc") \
   .option("url", jdbc_url) \
   .option("dbtable", "airflow_test") \
   .option("user", greenplum_user) \
   .option("password", greenplum_password) \
   .mode("append") \
   .save()

# Clean up the temporary file
os.remove(temp_file_path)

# Stop the SparkSession
spark.stop()
