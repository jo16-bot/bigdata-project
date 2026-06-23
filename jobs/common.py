import argparse
import json
import time
from pyspark.sql import SparkSession

def parse_args():
    """Ανάλυση ορισμάτων γραμμής εντολών (CLI Arguments)"""
    parser = argparse.ArgumentParser(description="Big Data Project 2026 - Common Argument Parser")
    parser.add_argument("--student-id", type=int, required=True, help="Your Student ID (AM) as an integer")
    parser.add_argument("--input-base", type=str, required=True, help="HDFS base path for input data")
    parser.add_argument("--output-base", type=str, required=True, help="HDFS base path for output results")
    parser.add_argument("--run-tag", type=str, default="default", help="Run tag for Q6 experiments")
    return parser.parse_args()

def calculate_personal_params(am):
    """Υπολογισμός παραμέτρων εξατομίκευσης από το ΑΜ"""
    h = am % 24
    L = 4
    d = (am % 26) + 1
    K = (am % 11) + 10
    
    # Υπολογισμός ωριαίου παραθύρου με κυκλικότητα
    valid_hours = []
    for i in range(L):
        valid_hours.append((h + i) % 24)
        
    # Ημέρες για το 2024
    valid_days_2024 = [d, d + 1, d + 2]
    
    return {
        "AM": am,
        "h": h,
        "L": L,
        "d": d,
        "K": K,
        "valid_hours": valid_hours,
        "valid_days_2024": valid_days_2024
    }

def get_spark_session(app_name="Spark-Job"):
    """Δημιουργία και επιστροφή του SparkSession"""
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def save_metrics(spark, am, job_name, duration, extra_metrics=None):
    """Αποθήκευση μετρικών σε αρχείο JSON στο HDFS"""
    app_id = spark.sparkContext.applicationId
    
    metrics = {
        "application_id": app_id,
        "job_name": job_name,
        "student_id": am,
        "execution_time_seconds": duration
    }
    if extra_metrics:
        metrics.update(extra_metrics)
        
    # Μετατροπή σε RDD και αποθήκευση στο HDFS ως JSON
    metrics_json = json.dumps(metrics)
    # Ο καθηγητής θέλει τα αποτελέσματα στη διαδρομή εξόδου του χρήστη
    # Θα αποθηκεύεται σε έναν δυναμικό φάκελο metrics ανά job
    return metrics_json

