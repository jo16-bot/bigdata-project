from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("check-hdfs-paths").getOrCreate()
sc = spark.sparkContext

OUTPUT_BASE = sys.argv[2] if len(sys.argv) > 2 else ""

print("\n" + "="*50)
print("🔍 LOOKING FOR HDFS FILES IN USER DIRECTORY:")
print("="*50)

try:
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    
    # Έλεγχος του φακέλου σου
    out_path = sc._jvm.org.apache.hadoop.fs.Path(OUTPUT_BASE)
    print(f"\nChecking directory: {OUTPUT_BASE}")
    if fs.exists(out_path):
        for status in fs.listStatus(out_path):
            print(f" -> {status.getPath().toString()}")
    else:
        print("❌ Directory does not exist!")
except Exception as e:
    print(f"Error reading HDFS: {e}")

print("="*50 + "\n")
spark.stop()