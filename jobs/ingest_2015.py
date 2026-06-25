import sys
from pyspark.sql import SparkSession

def main():
    input_base = sys.argv[1]
    output_base = sys.argv[2] # Παίρνουμε και το output_base
    spark = SparkSession.builder.appName("Ingest 2015 CSV to Parquet").getOrCreate()
    
    print("Reading 2015 CSV from HDFS...")
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(f"{input_base}/yellow_tripdata_2015.csv")
    
    print("Writing 2015 Parquet to USER HDFS directory...")
    # Γράφουμε στο δικό σας φάκελο (/user/iorizou/project2026/parquet/...)
    df.write.mode("overwrite").parquet(f"{output_base}/parquet/yellow_tripdata_2015")
    print("Ingestion completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()