from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import config

spark = SparkSession.builder \
    .appName("CICIDS2018 Preprocessing") \
    .config("spark.hadoop.fs.defaultFS", config.HDFS_URL) \
    .getOrCreate()

def preprocess():
    dt = datetime.strptime(config.PROCESS_DATE, "%Y-%m-%d")

    raw_path = (
        f"{config.HDFS_BASE_PATH}/raw/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    )
    warehouse_path = (
        f"{config.HDFS_BASE_PATH}/warehouse/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    )

    print(f"[Preprocessing] PROCESS_DATE={config.PROCESS_DATE}")
    print(f"[Preprocessing] Reading raw data from: {raw_path}")

    df = spark.read.parquet(raw_path)
    print(f"[Preprocessing] Raw rows: {df.count()}")
    df = df.dropDuplicates()
    print(f"[Preprocessing] Rows after dedup: {df.count()}")

    print(f"[Preprocessing] Writing warehouse to: {warehouse_path}")

    df.write.mode("overwrite").parquet(warehouse_path)

    print("[Preprocessing] Done!")

if __name__ == "__main__":
    preprocess()