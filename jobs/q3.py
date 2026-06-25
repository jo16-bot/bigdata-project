import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, StringType

spark = SparkSession.builder.appName("nyc-taxi-q3-full-benchmark").getOrCreate()

# Paths
INPUT_BASE = sys.argv[1] if len(sys.argv) > 1 else "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data"
OUTPUT_BASE = sys.argv[2] if len(sys.argv) > 2 else "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/iorizou/project2026"

parquet_path = f"{OUTPUT_BASE}/parquet/yellow_tripdata_2015"
csv_path = f"{OUTPUT_BASE}/data"
zone_lookup_path = f"{INPUT_BASE}/taxi_zone_lookup.csv"

# Schema για ομοιομορφία
taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("RateCodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

df_zones = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(zone_lookup_path)
execution_times = {}

# 1. DataFrame API
start = time.time()
df_p = spark.read.parquet(parquet_path).filter((col("trip_distance") > 0) & (col("fare_amount") > 0))
res_df = df_p.groupBy("RateCodeID").agg(count("*").alias("trips")).collect()
execution_times["DataFrame API"] = time.time() - start

# 2. Spark SQL
start = time.time()
df_p.createOrReplaceTempView("taxi")
res_sql = spark.sql("SELECT RateCodeID, COUNT(*) FROM taxi GROUP BY RateCodeID").collect()
execution_times["Spark SQL"] = time.time() - start

# 3. Ablation Study (No Broadcast)
start = time.time()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
res_abl = df_p.join(df_zones, df_p["RateCodeID"] == df_zones["LocationID"]).groupBy("Zone").count().collect()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
execution_times["Ablation (No Broadcast)"] = time.time() - start

# 4. CSV Benchmark
start = time.time()
df_csv = spark.read.format("csv").option("header", "true").schema(taxi_schema).load(csv_path)
res_csv = df_csv.filter((col("trip_distance") > 0) & (col("fare_amount") > 0)).groupBy("RateCodeID").count().collect()
execution_times["CSV Benchmark"] = time.time() - start

print("\n--- BENCHMARK RESULTS ---")
for k, v in execution_times.items():
    print(f"{k}: {v:.2f} seconds")

spark.stop()