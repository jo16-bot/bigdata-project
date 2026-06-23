import sys
import time
from pyspark.sql import functions as F
from common import parse_args, calculate_personal_params, get_spark_session

def main():
    args = parse_args()
    params = calculate_personal_params(args.student_id)
    
    print(f"=== Έναρξη EDA για το AM: {args.student_id} ===")
    
    spark = get_spark_session("NYC-Taxi-EDA")
    start_time = time.time()

    # Διαδρομές Parquet
    parquet_base = f"{args.output_base}/data/parquet"
    
    # 1. Φόρτωση Δεδομένων
    print("-> Φόρτωση Parquet αρχείων...")
    df_2015 = spark.read.parquet(f"{parquet_base}/yellow_tripdata_2015")
    df_2024 = spark.read.parquet(f"{parquet_base}/yellow_tripdata_2024")

    # 2. Συνολικές Εγγραφές
    count_2015 = df_2015.count()
    count_2024 = df_2024.count()
    print(f"\n[+] Σύνολο εγγραφών 2015: {count_2015}")
    print(f"[+] Σύνολο εγγραφών 2024: {count_2024}")

    # 3. Ανάλυση ανά Έτος
    for year, df in [("2015", df_2015), ("2024", df_2024)]:
        print(f"\n================ Ανάλυση Έτους {year} ================")
        
        # Κατανομή ανά ώρα
        print("\n--- Κατανομή Διαδρομών ανά Ώρα ---")
        df.groupby("pickup_hour").count().sort("pickup_hour").show()
        
        # Top 13 Pickup Locations
        print("\n--- Top 13 Pickup Locations (PULocationID) ---")
        df.groupby("PULocationID").count().sort(F.desc("count")).show(13)
        
        # Top 13 Dropoff Locations
        print("\n--- Top 13 Dropoff Locations (DOLocationID) ---")
        df.groupby("DOLocationID").count().sort(F.desc("count")).show(13)

    end_time = time.time()
    print(f"\n=== Το EDA ολοκληρώθηκε σε {end_time - start_time:.2f} δευτερόλεπτα ===")

if __name__ == "__main__":
    main()
