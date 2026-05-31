from pyspark.sql import SparkSession
import os
import config


def create_spark(app_name: str, stage: str):
    """
    stage:
        - preprocess
        - feature
        - train
    """

    pod_ip = os.getenv("POD_IP")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master-svc:7077")
        .config("spark.driver.host", pod_ip)
        .config("spark.driver.bindAddress", "0.0.0.0")

        # ===== S3 / MinIO =====
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # ===== CORE STABILITY =====
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")

        # ===== SERIALIZER =====
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # ===== COMPRESSION =====
        .config("spark.shuffle.compress", "true")
        .config("spark.shuffle.spill.compress", "true")

        # ===== AQE =====
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    # =========================
    # STAGE-BASED TUNING
    # =========================

    if stage == "preprocess":
        builder = (
            builder
            .config("spark.executor.instances", "2")
            .config("spark.executor.cores", "1")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "16")
            .config("spark.sql.files.maxPartitionBytes", "128m")
        )

    elif stage == "feature":
        builder = (
            builder
            .config("spark.executor.instances", "2")
            .config("spark.executor.cores", "2")
            .config("spark.executor.memory", "3g")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "32")
            .config("spark.sql.files.maxPartitionBytes", "128m")
        )

    elif stage == "train":
        builder = (
            builder
            .config("spark.executor.instances", "2")
            .config("spark.executor.cores", "2")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "3g")
            .config("spark.sql.shuffle.partitions", "32")
            .config("spark.sql.files.maxPartitionBytes", "256m")
        )

    else:
        raise ValueError("stage must be: preprocess | feature | train")

    spark = builder.getOrCreate()

    print("\n========== SPARK CONFIG ==========")
    print(f"APP: {app_name}")
    print(f"STAGE: {stage}")
    print(f"executors: {spark.conf.get('spark.executor.instances')}")
    print(f"cores: {spark.conf.get('spark.executor.cores')}")
    print(f"memory: {spark.conf.get('spark.executor.memory')}")
    print(f"shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
    print("==================================\n")

    return spark