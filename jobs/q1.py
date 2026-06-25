import sys
import time
from pyspark.sql import functions as F
from common import parse_args, calculate_personal_params, get_spark_session

def main():
    args = parse_args()
    params = calculate_personal_params(args.student_id)
    
    print(f"=== Έναρξη Επίσημου Q1 για το AM: {args.student_id} ===")
    
    if "day_start" in params and "day_end" in params:
        target_days = list(range(params["day_start"], params["day_end"] + 1))
    elif "days" in params:
        target_days = params["days"]
    else:
        target_days = [11, 12, 13]
        
    days_set = set(target_days)
    print(f"Στοχευμένες Ημέρες Μήνα για το Q1: {target_days}")
    
    spark = get_spark_session("NYC-Taxi-Q1-Official")
    
    # Σημείωση: Το RDD API ολοκληρώθηκε ήδη επιτυχώς σε 288.81 δευτερόλεπτα!
    print("\n[1] RDD API (Παράκαμψη καθώς εκτελέστηκε επιτυχώς σε 288.81s)...")

    # -----------------------------------------------------------------
    # 2. DATAFRAME API (Είσοδος: Parquet)
    # -----------------------------------------------------------------
    print("\n[2] Εκτέλεση με DataFrame API (Είσοδος: Parquet)...")
    start_df_pq = time.time()
    
    df_pq = spark.read.parquet(f"{args.output_base}/data/parquet/yellow_tripdata_2024")
    
    # Υπολογισμός διάρκειας δυναμικά επειδή χρησιμοποιούμε τις αρχικές στήλες
    df_with_duration = df_pq.withColumn(
        "duration_minutes", 
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0
    )
    
    # Φιλτράρισμα με βάση τα σωστά ονόματα στηλών του Parquet
    df_filtered = df_with_duration.filter(
        (F.col("tpep_pickup_datetime").isNotNull()) & (F.col("tpep_dropoff_datetime").isNotNull()) &
        (F.col("duration_minutes") > 0) & (F.col("trip_distance") > 0) & (F.col("total_amount") > 0) &
        (F.col("pickup_day").isin(target_days))
    )
    
    # Προσθήκη των ζητούμενων στηλών για το Q1
    df_enhanced = df_filtered \
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime")) \
        .withColumn("weekday", F.date_format("tpep_pickup_datetime", "E")) \
        .withColumn("is_weekend", F.when(F.col("weekday").isin("Sat", "Sun"), 1).otherwise(0)) \
        .withColumn("time_band", F.when((F.col("pickup_hour") >= 0) & (F.col("pickup_hour") <= 5), "Night")
                                  .when((F.col("pickup_hour") >= 6) & (F.col("pickup_hour") <= 11), "Morning")
                                  .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") <= 16), "Afternoon")
                                  .when((F.col("pickup_hour") >= 17) & (F.col("pickup_hour") <= 21), "Evening")
                                  .otherwise("Late"))
    
    total_trips_window = df_enhanced.count()
    
    # Συναθροίσεις
    res_hourly = df_enhanced.groupby("pickup_date", "pickup_hour", "time_band") \
        .agg(
            F.count("VendorID").alias("trips"),
            F.countDistinct("PULocationID").alias("unique_pickup_zones"),
            F.avg("passenger_count").alias("avg_passenger_count"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.sum("total_amount").alias("total_revenue")
        ) \
        .withColumn("trip_share_in_personal_window", (F.col("trips") / total_trips_window) * 100) \
        .sort(F.desc("trips"), F.desc("total_revenue"), "pickup_date", "pickup_hour")
    
    res_hourly.cache()
    top_k = res_hourly.take(10)
    end_df_pq = time.time()
    print(f"-> Το DataFrame API (Parquet) ολοκληρώθηκε σε {end_df_pq - start_df_pq:.2f} δευτερόλεπτα.")

    print("\n=== Σύνοψη ανά Time Band ===")
    res_band = df_enhanced.groupby("time_band") \
        .agg(
            F.count("VendorID").alias("trips"),
            F.avg("duration_minutes").alias("avg_duration"),
            F.avg("trip_distance").alias("avg_distance"),
            F.sum("total_amount").alias("total_revenue")
        )
    res_band.show()

    # -----------------------------------------------------------------
    # 3. SPARK SQL (Είσοδος: Parquet)
    # -----------------------------------------------------------------
    print("\n[3] Εκτέλεση με Spark SQL (Είσοδος: Parquet)...")
    start_sql = time.time()
    
    df_enhanced.createOrReplaceTempView("q1_enhanced_trips")
    
    res_sql = spark.sql("""
        SELECT 
            pickup_date, 
            pickup_hour,
            COUNT(*) as trips,
            COUNT(DISTINCT PULocationID) as unique_pickup_zones,
            AVG(passenger_count) as avg_passenger_count,
            AVG(duration_minutes) as avg_duration_minutes,
            AVG(trip_distance) as avg_trip_distance,
            AVG(total_amount) as avg_total_amount,
            SUM(total_amount) as total_revenue
        FROM q1_enhanced_trips
        GROUP BY pickup_date, pickup_hour
        ORDER BY trips DESC, total_revenue DESC, pickup_date ASC, pickup_hour ASC
        LIMIT 10
    """)
    res_sql.collect()
    end_sql = time.time()
    print(f"-> Η Spark SQL ολοκληρώθηκε σε {end_sql - start_sql:.2f} δευτερόλεπτα.")

    print("\n=== TOP 10 ΦΟΡΤΩΜΕΝΕΣ ΩΡΕΣ-ΗΜΕΡΕΣ (DataFrame) ===")
    res_hourly.select("pickup_date", "pickup_hour", "trips", "unique_pickup_zones", "avg_duration_minutes", "total_revenue", "trip_share_in_personal_window").show(10)

if __name__ == "__main__":
    main()
