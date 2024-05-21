from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Simple PySpark DataFrame Example") \
    .getOrCreate()

# Sample data in the form of a list of tuples
data = [
    (1, "Alice", 29),
    (2, "Bob", 31),
    (3, "Cathy", 25),
    (4, "David", 35)
]

# Define the schema for the DataFrame
columns = ["ID", "Name", "Age"]

# Create a DataFrame from the sample data
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Perform some basic DataFrame operations

# Select specific columns
df_select = df.select("Name", "Age")
df_select.show()

# Filter rows where Age is greater than 30
df_filtered = df.filter(df.Age > 30)
df_filtered.show()

# Group by Age and count the number of occurrences
df_grouped = df.groupBy("Age").count()
df_grouped.show()

# Stop the SparkSession
spark.stop()
