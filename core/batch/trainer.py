import config
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder \
    .appName("CICIDS2018 Trainer") \
    .config("spark.hadoop.fs.defaultFS", config.HDFS_URL) \
    .getOrCreate()

def train():
    warehouse_path = f"{config.HDFS_BASE_PATH}/warehouse/"
    print(f"[Trainer] Reading FULL data from: {warehouse_path}")
    df = spark.read.parquet(warehouse_path)
    print(f"[Trainer] Total rows: {df.count()}")

    indexer = StringIndexer(inputCol="Label", outputCol="label")
    df = indexer.fit(df).transform(df)

    feature_cols = [c for c in df.columns if c not in ["Label", "label"]]

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    df = assembler.transform(df)

    df = df.select("features", "label")

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    print(f"[Trainer] Train size: {train_df.count()}")
    print(f"[Trainer] Test size: {test_df.count()}")

    model = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=100
    )
    trained_model = model.fit(train_df)

    predictions = trained_model.transform(test_df)

    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )

    accuracy = evaluator.evaluate(predictions)

    print(f"[Trainer] Accuracy: {accuracy}")

    model_path = f"{config.HDFS_BASE_PATH}/models/rf_full_model"

    print(f"[Trainer] Saving model to: {model_path}")

    trained_model.write().overwrite().save(model_path)

    print("[Trainer] Training completed!")

if __name__ == "__main__":
    train()