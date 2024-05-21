from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("SimpleSparkJob").getOrCreate()

# Create a list of data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 28)]

# Create a DataFrame from the list
data_df = spark.createDataFrame(data, ["name", "age"])

# Print the DataFrame
print("Data:")
data_df.show()

# Select specific columns
selected_df = data_df.select("name")

# Print the selected columns
print("Names:")
selected_df.show()

# Stop the SparkSession
spark.stop()
