from pyspark.sql import SparkSession
from datetime import datetime
import config
import os


def create_spark():
    pod_ip = os.getenv("POD_IP")

    return (
        SparkSession.builder
        .appName("CICIDS2018 Bronze To Silver")
        .master("spark://spark-master-svc:7077")
        .config("spark.driver.host", pod_ip)
        .config("spark.driver.bindAddress", "0.0.0.0")

        # =========================
        # MinIO S3A
        # =========================
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # =========================
        # Spark tuning (dev)
        # =========================
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.executor.instances", "1")
        .config("spark.executor.memory", "512m")
        .config("spark.executor.cores", "1")
        .getOrCreate()
    )


def preprocess():
    spark = create_spark()

    dt = datetime.strptime(config.PROCESS_DATE, "%Y-%m-%d")

    bronze_path = (
        f"s3a://{config.MINIO_BUCKET}/bronze/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    )

    silver_path = (
        f"s3a://{config.MINIO_BUCKET}/silver/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    )

    print(f"[Preprocessing] PROCESS_DATE={config.PROCESS_DATE}")
    print(f"[Preprocessing] Reading bronze data from: {bronze_path}")

    df = spark.read.parquet(bronze_path)

    raw_count = df.count()
    print(f"[Preprocessing] Bronze rows: {raw_count}")

    # basic cleaning
    df = df.dropDuplicates()

    clean_count = df.count()
    print(f"[Preprocessing] Silver rows after dedup: {clean_count}")

    print(f"[Preprocessing] Writing silver to: {silver_path}")

    (
        df.write
        .mode("overwrite")
        .parquet(silver_path)
    )

    print("[Preprocessing] Bronze → Silver done!")


if __name__ == "__main__":
    preprocess()