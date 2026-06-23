import sys
import time
import json
from pyspark.sql import functions as F
from common import parse_args, calculate_personal_params, get_spark_session

def main():
    args = parse_args()
    
    print(f"=== Έναρξη Καθολικού EDA (Όλο το Έτος) ===")
    spark = get_spark_session("NYC-Taxi-Full-EDA")
    
    parquet_base = f"{args.output_base}/data/parquet"
    
    # 1. Φόρτωση Δεδομένων
    df_2015 = spark.read.parquet(f"{parquet_base}/yellow_tripdata_2015")
    df_2024 = spark.read.parquet(f"{parquet_base}/yellow_tripdata_2024")
    
    # Διάβασμα του Lookup από την αρχική CSV πηγή
    print("-> Φόρτωση Taxi Zone Lookup...")
    df_zone = spark.read.option("header", "true").csv(f"{args.input_base}/taxi_zone_lookup.csv")

    metrics_eda = {}

    # --- 5.1.1 Schema Dump & Describe ---
    print("\n[1] === Schema Dumps ===")
    df_2015.printSchema()
    df_2024.printSchema()
    
    # --- 5.1.2 Πίνακας Nulls ---
    print("\n[2] === Υπολογισμός Nulls (Ποσοστά) ===")
    for year, df in [("2015", df_2015), ("2024", df_2024)]:
        total_rows = df.count()
        null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
        null_percentages = {k: (v / total_rows) * 100 for k, v in null_counts.items()}
        metrics_eda[f"nulls_{year}"] = dict(sorted(null_percentages.items(), key=lambda item: item[1], reverse=True))

    # --- 5.1.3 & 5.1.4 Κατανομές Ωρών & Ημερών ---
    print("\n[3] === Κατανομές Χρόνου ===")
    metrics_eda["hourly_distribution_2015"] = df_2015.groupby("pickup_hour").count().sort("pickup_hour").collect()
    metrics_eda["hourly_distribution_2024"] = df_2024.groupby("pickup_hour").count().sort("pickup_hour").collect()
    metrics_eda["daily_distribution_2024"] = df_2024.groupby("pickup_day").count().sort("pickup_day").collect()

    metrics_eda["hourly_distribution_2015"] = {r["pickup_hour"]: r["count"] for r in metrics_eda["hourly_distribution_2015"]}
    metrics_eda["hourly_distribution_2024"] = {r["pickup_hour"]: r["count"] for r in metrics_eda["hourly_distribution_2024"]}
    metrics_eda["daily_distribution_2024"] = {r["pickup_day"]: r["count"] for r in metrics_eda["daily_distribution_2024"]}

    # --- 5.1.5 & 5.1.6 Λογαριθμικές Κατανομές ---
    print("\n[4] === Προετοιμασία Λογαριθμικών Ιστογραμμάτων ===")
    metrics_eda["negative_total_amount_2015"] = df_2015.filter(F.col("total_amount") < 0).count()
    metrics_eda["negative_total_amount_2024"] = df_2024.filter(F.col("total_amount") < 0).count()

    # --- 5.1.7 Joinability Check (2024) ---
    print("\n[5] === Joinability Check ===")
    total_2024 = df_2024.count()
    # Κάνουμε cast σε string ή int για σιγουριά στο join
    df_zone_clean = df_zone.withColumnRenamed("LocationID", "Zone_LocationID")
    joined_2024 = df_2024.join(df_zone_clean, df_2024.PULocationID == df_zone_clean.Zone_LocationID, "inner").count()
    metrics_eda["joinability_percentage_2024"] = (joined_2024 / total_2024) * 100
    print(f"Joinability 2024: {metrics_eda['joinability_percentage_2024']:.2f}%")

    # --- 5.1.8 Top-10 Zones (Pickup) ---
    print("\n[6] === Top-10 Pickup Zones ===")
    top_10_zones = df_2024.groupby("PULocationID").count() \
                          .join(df_zone_clean, F.col("PULocationID") == df_zone_clean.Zone_LocationID) \
                          .select("PULocationID", "Zone", "count").sort(F.desc("count")).limit(10).collect()
    metrics_eda["top_10_pickup_zones_2024"] = [{r["Zone"]: r["count"]} for r in top_10_zones]

    # --- 5.1.9 Δείγμα Ανωμαλιών ---
    print("\n[7] === Ανίχνευση Ακραίων Τιμών (Anomalies) ===")
    for year, df in [("2015", df_2015), ("2024", df_2024)]:
        print(f"--- Ανωμαλίες {year} ---")
        df.sort(F.desc("trip_distance")).select("trip_distance", "duration_minutes", "total_amount").show(5)
        df.sort(F.desc("duration_minutes")).select("trip_distance", "duration_minutes", "total_amount").show(5)
        df.filter(F.col("total_amount") < 0).select("trip_distance", "duration_minutes", "total_amount").show(5)

    # 8. Αποθήκευση σε JSON
    local_metrics_path = "/tmp/eda_metrics.json"
    with open(local_metrics_path, "w") as f:
        json.dump(metrics_eda, f, indent=4)
        
    import os
    hdfs_metrics_path = f"{args.output_base}/data/parquet/metrics/eda_metrics.json"
    os.system(f"hadoop fs -mkdir -p {args.output_base}/data/parquet/metrics")
    os.system(f"hadoop fs -put -f {local_metrics_path} {hdfs_metrics_path}")
    print(f"\n=== Το EDA ολοκληρώθηκε! Τα αποτελέσματα αποθηκεύτηκαν στο HDFS: {hdfs_metrics_path} ===")

if __name__ == "__main__":
    main()
