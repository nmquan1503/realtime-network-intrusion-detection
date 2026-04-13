import config
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def create_spark():
    return (
        SparkSession.builder
        .appName("CICIDS2018 Trainer (Skeleton)")
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def train():
    spark = create_spark()

    input_path = f"s3a://{config.MINIO_BUCKET}/silver/"
    model_path = f"s3a://{config.MINIO_BUCKET}/models/rf_model"

    print(f"[Trainer] Reading: {input_path}")

    df = spark.read.parquet(input_path)
    print(f"[Trainer] Raw rows: {df.count()}")

    # =========================
    # CLEAN BASIC
    # =========================
    df = df.filter(col("Label").isNotNull())

    # =========================
    # LABEL
    # =========================
    indexer = StringIndexer(inputCol="Label", outputCol="label")
    df = indexer.fit(df).transform(df)

    # =========================
    # INT FEATURES ONLY
    # =========================
    feature_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, IntegerType)
        and f.name not in ["label"]
    ]

    print(f"[Trainer] INT Features: {len(feature_cols)}")

    # =========================
    # SAFE FILTER (remove nulls only for selected cols)
    # =========================
    df = df.dropna(subset=feature_cols + ["label"])

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip"
    )

    df = assembler.transform(df).select("features", "label")

    # =========================
    # SPLIT
    # =========================
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    print(f"[Trainer] Train: {train_df.count()}")
    print(f"[Trainer] Test: {test_df.count()}")

    # =========================
    # MODEL
    # =========================
    model = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=10
    )

    trained = model.fit(train_df)

    pred = trained.transform(test_df)

    acc = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    ).evaluate(pred)

    print(f"[Trainer] Accuracy: {acc}")

    trained.write().overwrite().save(model_path)

    print("[Trainer] Done")


if __name__ == "__main__":
    train()