from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("InsertIntoGreenPlum").getOrCreate()

# Configure GreenPlum connection details
greenplum_host = "172.19.33.16"
greenplum_port = "5432"
greenplum_database = "gpadmin"
greenplum_user = "gpadmin"
greenplum_password = "gpadmin"

# Create a Spark DataFrame with the data to insert
data = [("John", 30), ("Jane", 25)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

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
