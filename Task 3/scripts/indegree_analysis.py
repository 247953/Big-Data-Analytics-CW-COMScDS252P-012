from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType
import time

def main():
    spark = SparkSession.builder \
        .appName("SNAP Web-Berkstan In-Degree Analysis") \
        .getOrCreate()

    # Define the schema for the input data
    schema = StructType([
        StructField("FromNodeId", IntegerType(), True),
        StructField("ToNodeId", IntegerType(), True)
    ])

    # Path inside the Docker container mounted from host ./data/web-BerkStan.txt
    data_path = "/opt/spark/data/web-BerkStan.txt"

    # 1. Parse text file into dataframe, stripping comment metadata headers
    raw_lines = spark.read.text(data_path)
    filtered_lines = raw_lines.filter(~col("value").startswith("#"))

    # Split space/tab separated values into integer columns 
    edges_df = filtered_lines.select(
        col("value").substr(1, 100).alias("raw")
    ).selectExpr(
        "cast(split(trim(raw), '\\\\s+')[0] as int) as FromNodeId",
        "cast(split(trim(raw), '\\\\s+')[1] as int) as ToNodeId"
    )

    # Cache edge dataframe to reduce re-computation during transformations stage
    edges_df.cache()

    # 2. Group and aggregate target vertex indexes (ToNodeId) for in-degree distributions
    in_degree_df = edges_df.groupBy("ToNodeId") \
        .count() \
        .withColumnRenamed("count", "in_degree")

    # 3. Identify Top 50 dominant destination nodes
    top_50_nodes = in_degree_df.orderBy(col("in_degree").desc()).limit(50)

    # Persist the top 50 nodes in memory for further analysis
    top_50_nodes.cache()

    print("=== Top 50 Dominant Destination Nodes(In-Degree) ===")
    top_50_nodes.show(50, truncate=False)

    input("\nJob finished! Press Enter in terminal to stop Spark and close UI...") 
    
    spark.stop()

if __name__ == "__main__":
    main()