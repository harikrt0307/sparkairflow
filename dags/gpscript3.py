from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# Create a SparkSession
spark = SparkSession.builder.appName("InsertIntoGreenPlum").getOrCreate()

# Configure GreenPlum connection details
greenplum_host = "172.19.33.16"
greenplum_port = "5432"
greenplum_database = "gpadmin"
greenplum_user = "gpadmin"
greenplum_password = "gpadmin"

# Read the CSV file from the GitHub URL as text
excel_file_url = "https://github.com/harikrt0307/sparkairflow/raw/main/dags/data1.csv"
text_df = spark.read.text(excel_file_url)

# Split the text into rows and parse the CSV
csv_df = text_df.select(split(col("value"), ",").alias("cols"))
df = csv_df.select([col("cols")[i].alias(f"col{i}") for i in range(len(csv_df.first().cols))])

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
