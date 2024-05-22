from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("InsertIntoGreenPlum").getOrCreate()

# Configure GreenPlum connection details
greenplum_host = "172.19.33.16"
greenplum_port = "5432"
greenplum_database = "gpadmin"
greenplum_user = "gpadmin"
greenplum_password = "gpadmin"

# Read data from CSV into a DataFrame
excel_file_path = "https://github.com/harikrt0307/sparkairflow/raw/main/dags/data1.csv"
df = spark.read.csv(excel_file_path, header=True, inferSchema=True)

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
