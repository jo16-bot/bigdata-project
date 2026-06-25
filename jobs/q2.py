import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import math

# Εισαγωγή κοινών παραμέτρων (AM: 2121221 -> Ώρες: 11, 12, 13)
# Αν έχεις έτοιμο το common.py, το χρησιμοποιείς, αλλιώς ορίζουμε τα όρια εδώ:
TARGET_HOURS = [11, 12, 13]

def main():
    if len(sys.argv) < 3:
        print("Usage: q2.py <input_base_path> <output_base_path>")
        sys.exit(-1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]

    spark = SparkSession.builder \
        .appName("NYC Taxi - Q2 Spatial Analysis") \
        .getOrCreate()

    # Ορισμός ορίων Νέας Υόρκης (Bounding Box)
    # Lat: 40.50 έως 40.95, Lon: -74.25 έως -73.70
    LAT_MIN, LAT_MAX = 40.50, 40.95
    LON_MIN, LON_MAX = -74.25, -73.70

    print(f"Starting Q2 Analysis for AM parameters. Target Hours: {TARGET_HOURS}")

    # =========================================================================
    # 1. ΥΛΟΠΟΙΗΣΗ: DataFrame API ΧΩΡΙΣ UDF (Native Functions)
    # =========================================================================
    t0_df_native = time.time()
    
    # Διαβάζουμε τα δεδομένα του 2015 (υποθέτουμε parquet, ή csv αν οριστεί)
    # Σημείωση: Το 2015 έχει στήλες pickup_latitude, pickup_longitude κλπ.
    df = spark.read.parquet(f"{output_base}/parquet/yellow_tripdata_2015")

    # Φιλτράρισμα: Έγκυρα timestamps, θετικές αποστάσεις/διάρκειες και ρεαλιστικά όρια ΝΥ
    df_cleaned = df.filter(
        (F.col("tpep_pickup_datetime").isNotNull()) & 
        (F.col("tpep_dropoff_datetime").isNotNull()) &
        ((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0 > 0) &
        (F.col("trip_distance") > 0) &
        (F.col("pickup_latitude").between(LAT_MIN, LAT_MAX)) &
        (F.col("pickup_longitude").between(LON_MIN, LON_MAX)) &
        (F.col("dropoff_latitude").between(LAT_MIN, LAT_MAX)) &
        (F.col("dropoff_longitude").between(LON_MIN, LON_MAX)) &
        (F.hour("tpep_pickup_datetime").isin(TARGET_HOURS))
    )

    # Μετατροπές & Υπολογισμός Haversine με Native Functions
    R = 6371.0  # Ακτίνα της Γης σε km
    
    df_metrics = df_cleaned.withColumn("trip_distance_km", F.col("trip_distance") * 1.60934) \
        .withColumn("duration_hours", (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600.0) \
        .withColumn("duration_minutes", F.col("duration_hours") * 60.0) \
        .withColumn("speed_kmh", F.col("trip_distance_km") / F.col("duration_hours")) \
        .withColumn("duration_per_km", F.col("duration_minutes") / F.col("trip_distance_km"))

    # Τύπος Haversine με Spark SQL functions
    dlat = F.radians(F.col("dropoff_latitude") - F.col("pickup_latitude"))
    dlon = F.radians(F.col("dropoff_longitude") - F.col("pickup_longitude"))
    lat1 = F.radians(F.col("pickup_latitude"))
    lat2 = F.radians(F.col("dropoff_latitude"))

    a = F.sin(dlat / 2)**2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2)**2
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    df_metrics = df_metrics.withColumn("haversine_km", c * R)

    # Φιλτράρισμα haversine_km > 0.2 και υπολογισμός gap / detour / congestion
    df_final_native = df_metrics.filter(F.col("haversine_km") > 0.2) \
        .withColumn("distance_gap_km", F.col("trip_distance_km") - F.col("haversine_km")) \
        .withColumn("detour_ratio", F.col("trip_distance_km") / F.col("haversine_km")) \
        .withColumn("congestion_candidate", F.when((F.col("speed_kmh") < 10) & (F.col("trip_distance_km") >= 1), 1).otherwise(0))

    # Cache για να μην ξαναϋπολογίζεται στα επόμενα βήματα
    df_final_native.cache()

    # Συναθροίσεις ανά pickup_hour (Native DataFrame API)
    df_agg_native = df_final_native.groupBy(F.hour("tpep_pickup_datetime").alias("pickup_hour")).agg(
        F.count("*").alias("trips"),
        F.avg("duration_minutes").alias("avg_duration_minutes"),
        F.percentile_approx("duration_minutes", 0.5).alias("median_duration_minutes"),
        F.percentile_approx("duration_minutes", 0.9).alias("p90_duration_minutes"),
        F.avg("speed_kmh").alias("avg_speed_kmh"),
        (F.sum("trip_distance_km") / F.sum("duration_hours")).alias("agg_speed_kmh"),
        F.avg("haversine_km").alias("avg_haversine_km"),
        F.avg("distance_gap_km").alias("avg_distance_gap_km"),
        F.avg("detour_ratio").alias("avg_detour_ratio"),
        F.avg("congestion_candidate").alias("congestion_candidate_share")
    ).orderBy("pickup_hour")

    print("=== NATIVE DATAFRAME AGGREGATION ===")
    df_agg_native.show()
    t_df_native = time.time() - t0_df_native
    print(f"Native DataFrame API completed in {t_df_native:.2f} seconds.")

    # 5 πιο αργές (μεγαλύτερη διάρκεια ανά χιλιόμετρο) και 5 ταχύτερες διαδρομές
    print("=== TOP 5 SLOWEST TRIPS PER KM ===")
    df_final_native.orderBy(F.col("duration_per_km").desc()).select(
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "duration_minutes", "pickup_latitude", "pickup_longitude", "haversine_km", "duration_per_km"
    ).show(5)

    print("=== TOP 5 FASTEST TRIPS ===")
    df_final_native.orderBy(F.col("speed_kmh").desc()).select(
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "duration_minutes", "pickup_latitude", "pickup_longitude", "haversine_km", "speed_kmh"
    ).show(5)

    # =========================================================================
    # 2. ΥΛΟΠΟΙΗΣΗ: DataFrame API ΜΕ PYTHON UDF
    # =========================================================================
    t0_df_udf = time.time()
    
    def haversine_py(lat1, lon1, lat2, lon2):
        if None in (lat1, lon1, lat2, lon2): return None
        R_earth = 6371.0
        d_lat = math.radians(lat2 - lat1)
        d_lon = math.radians(lon2 - lon1)
        a_val = math.sin(d_lat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon/2)**2
        c_val = 2 * math.atan2(math.sqrt(a_val), math.sqrt(1-a_val))
        return c_val * R_earth

    haversine_udf = F.udf(haversine_py, DoubleType())

    df_udf = df_cleaned.withColumn("trip_distance_km", F.col("trip_distance") * 1.60934) \
        .withColumn("duration_hours", (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600.0) \
        .withColumn("duration_minutes", F.col("duration_hours") * 60.0) \
        .withColumn("speed_kmh", F.col("trip_distance_km") / F.col("duration_hours")) \
        .withColumn("haversine_km", haversine_udf("pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude"))

    df_final_udf = df_udf.filter(F.col("haversine_km") > 0.2) \
        .withColumn("distance_gap_km", F.col("trip_distance_km") - F.col("haversine_km")) \
        .withColumn("detour_ratio", F.col("trip_distance_km") / F.col("haversine_km")) \
        .withColumn("congestion_candidate", F.when((F.col("speed_kmh") < 10) & (F.col("trip_distance_km") >= 1), 1).otherwise(0))

    df_agg_udf = df_final_udf.groupBy(F.hour("tpep_pickup_datetime").alias("pickup_hour")).count()
    df_agg_udf.show() # Απλό action για να μετρήσουμε τον χρόνο του UDF pipeline
    t_df_udf = time.time() - t0_df_udf
    print(f"Python UDF DataFrame API completed in {t_df_udf:.2f} seconds.")

    # Εκτύπωση Φυσικού Σχεδίου (Physical Plan) για σύγκριση
    print("=== NATIVE PHYSICAL PLAN ===")
    df_final_native.explain()
    print("=== UDF PHYSICAL PLAN ===")
    df_final_udf.explain()

    # =========================================================================
    # 3. ΥΛΟΠΟΙΗΣΗ: SPARK SQL
    # =========================================================================
    t0_sql = time.time()
    df_final_native.createOrReplaceTempView("q2_precomputed_trips")
    
    df_sql = spark.sql("""
        SELECT 
            hour(tpep_pickup_datetime) as pickup_hour,
            count(*) as trips,
            avg(duration_minutes) as avg_duration_minutes,
            percentile_approx(duration_minutes, 0.5) as median_duration_minutes,
            percentile_approx(duration_minutes, 0.9) as p90_duration_minutes,
            avg(speed_kmh) as avg_speed_kmh,
            sum(trip_distance_km) / sum(duration_hours) as agg_speed_kmh,
            avg(haversine_km) as avg_haversine_km,
            avg(distance_gap_km) as avg_distance_gap_km,
            avg(detour_ratio) as avg_detour_ratio,
            avg(congestion_candidate) as congestion_candidate_share
        FROM q2_precomputed_trips
        GROUP BY hour(tpep_pickup_datetime)
        ORDER BY pickup_hour
    """)
    
    print("=== SPARK SQL AGGREGATION ===")
    df_sql.show()
    t_sql = time.time() - t0_sql
    print(f"Spark SQL completed in {t_sql:.2f} seconds.")

    spark.stop()

if __name__ == "__main__":
    main()