import sys
import time
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Εισαγωγή του κοινού βοηθητικού module
from common import parse_args, calculate_personal_params, get_spark_session

def main():
    # 1. Ανάλυση ορισμάτων
    args = parse_args()
    params = calculate_personal_params(args.student_id)
    
    print(f"=== Έναρξη Προετοιμασίας Parquet για το AM: {args.student_id} ===")
    print(f"Έγκυρες ώρες: {params['valid_hours']}")
    print(f"Έγκυρες ημέρες (2024): {params['valid_days_2024']}")

    # 2. Εκκίνηση SparkSession
    spark = get_spark_session("Prepare-Parquet")
    start_time = time.time()

    # 3. Ρητό Σχήμα (Explicit Schema) για τα Yellow Taxi CSV
    taxi_schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True)
    ])

    # 4. Επεξεργασία Δεδομένων 2015
    print(">>> Επεξεργασία έτους 2015...")
    # ΔΙΟΡΘΩΣΗ: Απευθείας το αρχείο χωρίς -*.csv
    df_2015 = spark.read.format("csv") \
        .option("header", "true") \
        .schema(taxi_schema) \
        .load(f"{args.input_base}/yellow_tripdata_2015.csv")

    df_2015_filtered = df_2015 \
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
        .filter((F.year("tpep_pickup_datetime") == 2015) & 
                (F.col("pickup_hour").isin(params['valid_hours'])))

    output_path_2015 = f"{args.output_base}/data/parquet/yellow_tripdata_2015"
    df_2015_filtered.write.mode("overwrite").parquet(output_path_2015)

    # 5. Επεξεργασία Δεδομένων 2024
    print(">>> Επεξεργασία έτους 2024...")
    # ΔΙΟΡΘΩΣΗ: Απευθείας το αρχείο χωρίς -*.csv
    df_2024 = spark.read.format("csv") \
        .option("header", "true") \
        .schema(taxi_schema) \
        .load(f"{args.input_base}/yellow_tripdata_2024.csv")

    df_2024_filtered = df_2024 \
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
        .withColumn("pickup_day", F.dayofmonth("tpep_pickup_datetime")) \
        .filter((F.year("tpep_pickup_datetime") == 2024) & 
                (F.col("pickup_day").isin(params['valid_days_2024'])) & 
                (F.col("pickup_hour").isin(params['valid_hours'])))

    output_path_2024 = f"{args.output_base}/data/parquet/yellow_tripdata_2024"
    df_2024_filtered.write.mode("overwrite").parquet(output_path_2024)

    end_time = time.time()
    duration = end_time - start_time
    print(f"=== Η προετοιμασία ολοκληρώθηκε επιτυχώς σε {duration:.2f} δευτερόλεπτα! ===")

if __name__ == "__main__":
    main()
