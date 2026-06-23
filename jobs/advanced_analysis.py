import sys
import time
from pyspark.sql import functions as F
from common import parse_args, calculate_personal_params, get_spark_session

def main():
    args = parse_args()
    params = calculate_personal_params(args.student_id)
    
    print(f"=== Έναρξη Advanced Analysis για το AM: {args.student_id} ===")
    
    spark = get_spark_session("NYC-Taxi-Advanced")
    start_time = time.time()

    parquet_base = f"{args.output_base}/data/parquet"
    
    # Φόρτωση των έτοιμων Parquet δεδομένων
    df_2015 = spark.read.parquet(f"{parquet_base}/yellow_tripdata_2015")
    df_2024 = spark.read.parquet(f"{parquet_base}/yellow_tripdata_2024")

    for year, df in [("2015", df_2015), ("2024", df_2024)]:
        print(f"\n================ Προχωρημένη Ανάλυση Έτους {year} ================")
        
        # Ερώτημα 1: Top Trip (Pickup -> Dropoff)
        print("\n--- Top 3 Πιο Δημοφιλείς Διαδρομές (PULocation -> DOLocation) ---")
        df.groupby("PULocationID", "DOLocationID") \
          .count() \
          .sort(F.desc("count")) \
          .show(3)
          
        # Ερώτημα 2: Μέσο Tip ανά Vendor
        print("\n--- Μέσο Tip Amount ανά VendorID ---")
        df.groupby("VendorID") \
          .agg(F.avg("tip_amount").alias("avg_tip_amount")) \
          .sort("VendorID") \
          .show()

        # Ερώτημα 3: Συσχετίσεις (Correlations)
        print("\n--- Συσχετίσεις Μεταβλητών (Correlations) ---")
        corr_dist_fare = df.stat.corr("trip_distance", "fare_amount")
        corr_dist_tip = df.stat.corr("trip_distance", "tip_amount")
        corr_fare_tip = df.stat.corr("fare_amount", "tip_amount")
        
        print(f"Συσχέτιση Trip Distance & Fare Amount: {corr_dist_fare:.4f}")
        print(f"Συσχέτιση Trip Distance & Tip Amount:  {corr_dist_tip:.4f}")
        print(f"Συσχέτιση Fare Amount & Tip Amount:    {corr_fare_tip:.4f}")

    end_time = time.time()
    print(f"\n=== Η Advanced Analysis ολοκληρώθηκε σε {end_time - start_time:.2f} δευτερόλεπτα ===")

if __name__ == "__main__":
    main()
