from pyspark.sql import functions as F
from pyspark.sql.types import *
from functools import reduce
from datetime import datetime
import config
from logger import Logger
from spark_session import create_spark

def quote_column(column_name):
    return f"`{column_name.replace('`', '``')}`"


def safe_cast_column(field):
    source = quote_column(field.name)

    if isinstance(field.dataType, IntegerType):
        return F.expr(f"try_cast({source} as int)").alias(field.name)
    if isinstance(field.dataType, LongType):
        return F.expr(f"try_cast({source} as bigint)").alias(field.name)
    if isinstance(field.dataType, DoubleType):
        return F.expr(f"try_cast({source} as double)").alias(field.name)

    return F.col(field.name).cast(field.dataType).alias(field.name)


def preprocess():
    log = Logger("BRONZE → SILVER")
    log.start()
    
    spark = create_spark(
        app_name="bronze_to_silver",
        stage="preprocess"
    )
    log.log("SPARK")

    dt = datetime.strptime(config.PROCESS_DATE, "%Y-%m-%d")

    bronze_path = (
        f"s3a://{config.MINIO_BUCKET}/bronze/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    )

    silver_path = (
        f"s3a://{config.MINIO_BUCKET}/silver/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    )

    df = spark.read.parquet(bronze_path)
    log.log("READ")

    schema = StructType([
        StructField("Dst Port", IntegerType(), True),
        StructField("Protocol", IntegerType(), True),
        StructField("Timestamp", StringType(), True),

        StructField("Flow Duration", LongType(), True),
        StructField("Tot Fwd Pkts", LongType(), True),
        StructField("Tot Bwd Pkts", LongType(), True),
        StructField("TotLen Fwd Pkts", LongType(), True),
        StructField("TotLen Bwd Pkts", LongType(), True),

        StructField("Fwd Pkt Len Max", LongType(), True),
        StructField("Fwd Pkt Len Min", LongType(), True),
        StructField("Fwd Pkt Len Mean", DoubleType(), True),
        StructField("Fwd Pkt Len Std", DoubleType(), True),

        StructField("Bwd Pkt Len Max", LongType(), True),
        StructField("Bwd Pkt Len Min", LongType(), True),
        StructField("Bwd Pkt Len Mean", DoubleType(), True),
        StructField("Bwd Pkt Len Std", DoubleType(), True),

        StructField("Flow Byts/s", DoubleType(), True),
        StructField("Flow Pkts/s", DoubleType(), True),

        StructField("Flow IAT Mean", DoubleType(), True),
        StructField("Flow IAT Std", DoubleType(), True),
        StructField("Flow IAT Max", LongType(), True),
        StructField("Flow IAT Min", LongType(), True),

        StructField("Fwd IAT Tot", LongType(), True),
        StructField("Fwd IAT Mean", DoubleType(), True),
        StructField("Fwd IAT Std", DoubleType(), True),
        StructField("Fwd IAT Max", LongType(), True),
        StructField("Fwd IAT Min", LongType(), True),

        StructField("Bwd IAT Tot", LongType(), True),
        StructField("Bwd IAT Mean", DoubleType(), True),
        StructField("Bwd IAT Std", DoubleType(), True),
        StructField("Bwd IAT Max", LongType(), True),
        StructField("Bwd IAT Min", LongType(), True),

        StructField("Fwd PSH Flags", IntegerType(), True),
        StructField("Bwd PSH Flags", IntegerType(), True),
        StructField("Fwd URG Flags", IntegerType(), True),
        StructField("Bwd URG Flags", IntegerType(), True),

        StructField("Fwd Header Len", LongType(), True),
        StructField("Bwd Header Len", LongType(), True),

        StructField("Fwd Pkts/s", DoubleType(), True),
        StructField("Bwd Pkts/s", DoubleType(), True),

        StructField("Pkt Len Min", LongType(), True),
        StructField("Pkt Len Max", LongType(), True),
        StructField("Pkt Len Mean", DoubleType(), True),
        StructField("Pkt Len Std", DoubleType(), True),
        StructField("Pkt Len Var", DoubleType(), True),

        StructField("FIN Flag Cnt", IntegerType(), True),
        StructField("SYN Flag Cnt", IntegerType(), True),
        StructField("RST Flag Cnt", IntegerType(), True),
        StructField("PSH Flag Cnt", IntegerType(), True),
        StructField("ACK Flag Cnt", IntegerType(), True),
        StructField("URG Flag Cnt", IntegerType(), True),
        StructField("CWE Flag Count", IntegerType(), True),
        StructField("ECE Flag Cnt", IntegerType(), True),

        StructField("Down/Up Ratio", DoubleType(), True),

        StructField("Pkt Size Avg", DoubleType(), True),
        StructField("Fwd Seg Size Avg", DoubleType(), True),
        StructField("Bwd Seg Size Avg", DoubleType(), True),

        StructField("Fwd Byts/b Avg", LongType(), True),
        StructField("Fwd Pkts/b Avg", LongType(), True),
        StructField("Fwd Blk Rate Avg", LongType(), True),
        StructField("Bwd Byts/b Avg", LongType(), True),
        StructField("Bwd Pkts/b Avg", LongType(), True),
        StructField("Bwd Blk Rate Avg", LongType(), True),

        StructField("Subflow Fwd Pkts", LongType(), True),
        StructField("Subflow Fwd Byts", LongType(), True),
        StructField("Subflow Bwd Pkts", LongType(), True),
        StructField("Subflow Bwd Byts", LongType(), True),

        StructField("Init Fwd Win Byts", LongType(), True),
        StructField("Init Bwd Win Byts", LongType(), True),

        StructField("Fwd Act Data Pkts", LongType(), True),
        StructField("Fwd Seg Size Min", LongType(), True),

        StructField("Active Mean", DoubleType(), True),
        StructField("Active Std", DoubleType(), True),
        StructField("Active Max", LongType(), True),
        StructField("Active Min", LongType(), True),

        StructField("Idle Mean", DoubleType(), True),
        StructField("Idle Std", DoubleType(), True),
        StructField("Idle Max", LongType(), True),
        StructField("Idle Min", LongType(), True),

        StructField("Label", StringType(), True),
    ])

    df = df.select(*[f.name for f in schema.fields])
    df = df.select([safe_cast_column(field) for field in schema.fields])

    df = df.withColumn(
        "Timestamp",
        F.expr("try_to_timestamp(`Timestamp`, 'dd/MM/yyyy HH:mm:ss')")
    )

    df = df.na.drop()

    float_cols = [
        f.name for f in schema.fields
        if isinstance(f.dataType, DoubleType)
    ]
    condition = reduce(
        lambda a, b: a & b,
        [(F.col(c) != float("inf")) & (F.col(c) != float("-inf")) for c in float_cols]
    )
    df = df.filter(condition)
    df = df.dropDuplicates()
    log.log("CLEAN")

    df.write.mode("overwrite").parquet(silver_path)
    log.log("WRITE")

    log.end()

if __name__ == "__main__":
    preprocess()
