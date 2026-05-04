from logger import Logger
from spark_session import create_spark
import config
from datetime import datetime
from pyspark.sql import functions as F

def feature_engineer():
    log = Logger("SILVER -> GOLD")
    log.start()
    
    spark = create_spark(
        app_name="silver_to_gold",
        stage="feature"
    )
    log.log("Spark session created")

    dt = datetime.strptime(config.PROCESS_DATE, "%Y-%m-%d")

    silver_path = (
        f"s3a://{config.MINIO_BUCKET}/silver/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    )

    gold_path = (
        f"s3a://{config.MINIO_BUCKET}/gold/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    )

    df = spark.read.parquet(silver_path)
    log.log(f"Load silver ({df.rdd.getNumPartitions()} partitions)")


    df = df.withColumn(
        "is_well_known_port",
        F.when(F.col("Dst Port") < 1024, 1).otherwise(0)
    )
    df = df.withColumn(
        "is_ssh_port",
        F.when(F.col("Dst Port") == 22, 1).otherwise(0)
    )
    df = df.withColumn(
        "is_web_port",
        F.when((F.col("Dst Port") == 80) | (F.col("Dst Port") == 443), 1).otherwise(0)
    )
    log.log("Added port-based features")


    df = df.withColumn(
        "pkt_ratio",
        F.col("Tot Fwd Pkts") / (F.col("Tot Bwd Pkts") + F.lit(1))
    )
    df = df.withColumn(
        "byte_ratio",
        F.col("TotLen Fwd Pkts") / (F.col("TotLen Bwd Pkts") + F.lit(1.0))
    )
    df = df.withColumn(
        "bytes_per_pkt",
        (F.col("TotLen Fwd Pkts") + F.col("TotLen Bwd Pkts")) /
        (F.col("Tot Fwd Pkts") + F.col("Tot Bwd Pkts") + F.lit(1))
    )
    log.log("Added ratio-based features")

    df = df.withColumn(
        "Label",
        F.when(F.col("Label") == "Infilteration", "Infiltration")
        .otherwise(F.col("Label"))
    ).withColumn(
        "Label",
        F.when(F.col("Label") == "Benign", "Benign")

        .when(F.col("Label").isin(
            "FTP-BruteForce",
            "SSH-Bruteforce",
            "Brute Force -Web",
            "Brute Force -XSS"
        ), "BruteForce")

        .when(F.col("Label").isin(
            "DoS attacks-GoldenEye",
            "DoS attacks-Slowloris",
            "DoS attacks-SlowHTTPTest",
            "DoS attacks-Hulk"
        ), "DoS")

        .when(F.col("Label").isin(
            "DDoS attacks-LOIC-HTTP",
            "DDOS attack-LOIC-UDP",
            "DDOS attack-HOIC"
        ), "DDoS")

        .when(F.col("Label") == "SQL Injection", "WebAttack")
        .when(F.col("Label") == "Bot", "Bot")
        .when(F.col("Label") == "Infiltration", "Infiltration")

        .otherwise("Unknown")
    )

    df.write.mode("overwrite").parquet(gold_path)

    log.end()

if __name__ == "__main__":
    feature_engineer()