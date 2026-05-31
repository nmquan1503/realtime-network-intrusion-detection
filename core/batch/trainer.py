import config
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from logger import Logger
from spark_session import create_spark
from xgboost.spark import SparkXGBClassifier
from pyspark.ml import Pipeline
from datetime import datetime
from pyspark.sql.functions import col, when, lit

def train():
    log = Logger("GOLD -> MODEL")
    log.start()

    spark = create_spark(
        app_name="gold_to_model",
        stage="train"
    )
    log.log("SPARK")

    now = datetime.now()

    gold_base_path = f"s3a://{config.MINIO_BUCKET}/gold/"
    model_base_path = (
        f"s3a://{config.MINIO_BUCKET}/model/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{now.strftime('%H-%M-%S')}/"
    )

    df = spark.read.parquet(gold_base_path)
    log.log("READ")

    feature_cols = [
        'Fwd Seg Size Min', 'Fwd IAT Tot', 'Flow Duration', 'Init Fwd Win Byts',
        'Bwd Pkts/s', 'Flow IAT Max', 'Fwd IAT Max', 'Fwd Header Len', 'Bwd IAT Tot',
        'Flow IAT Mean', 'Fwd IAT Mean', 'byte_ratio', 'Subflow Fwd Byts',
        'is_well_known_port', 'Dst Port', 'TotLen Fwd Pkts', 'Fwd IAT Min',
        'Flow IAT Min', 'Flow Pkts/s', 'TotLen Bwd Pkts', 'Fwd Pkts/s',
        'Subflow Bwd Byts', 'is_web_port', 'Bwd Header Len', 'pkt_ratio',
        'Bwd Pkt Len Max', 'Pkt Len Max', 'Idle Max', 'Idle Min', 'Idle Mean',
        'Bwd Pkt Len Std', 'Fwd Seg Size Avg', 'Fwd Pkt Len Mean', 'Bwd IAT Max',
        'Pkt Len Var', 'Pkt Len Std', 'Flow Byts/s', 'Subflow Bwd Pkts',
        'Fwd Pkts/b Avg', 'Tot Bwd Pkts', 'Fwd Pkt Len Max', 'Tot Fwd Pkts',
        'Protocol', 'Pkt Size Avg', 'Bwd IAT Mean', 'Bwd Seg Size Avg',
        'Bwd Pkt Len Mean', 'Pkt Len Mean', 'Subflow Fwd Pkts', 'bytes_per_pkt'
    ]

    label_indexer = StringIndexer(
        inputCol="Label",
        outputCol="label",
        handleInvalid="keep"
    )
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    xgb = SparkXGBClassifier(
        features_col="features",
        label_col="label",
        prediction_col="prediction",
        objective="multi:softprob",
        num_class=7,
        max_depth=6,
        eta=0.1,
        num_round=120,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="mlogloss"
    )

    pipeline = Pipeline(stages=[
        label_indexer,
        assembler,
        xgb
    ])
    log.log("PIPELINE")

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    log.log("SPLIT")

    model = pipeline.fit(train_df)
    log.log("TRAIN")

    pred = model.transform(test_df)
    log.log("PREDICT")

    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1"
    )

    acc = evaluator_acc.evaluate(pred)
    f1 = evaluator_f1.evaluate(pred)

    cm = pred.groupBy("label", "prediction").count()

    actual = pred.groupBy("label").count().withColumnRenamed("count", "actual")
    predicted = pred.groupBy("prediction").count().withColumnRenamed("count", "predicted")
    
    tp = cm.filter(col("label") == col("prediction")) \
           .withColumnRenamed("count", "tp") \
           .select(col("label"), "tp")

    per_class = actual.join(tp, on="label", how="left") \
        .join(predicted, actual.label == predicted.prediction, how="left") \
        .drop("prediction") \
        .fillna(0)

    per_class = per_class.withColumn(
        "precision",
        when(col("predicted") == 0, 0).otherwise(col("tp") / col("predicted"))
    ).withColumn(
        "recall",
        when(col("actual") == 0, 0).otherwise(col("tp") / col("actual"))
    ).withColumn(
        "f1",
        when((col("precision") + col("recall")) == 0, 0)
        .otherwise(2 * col("precision") * col("recall") / (col("precision") + col("recall")))
    )

    global_metrics = spark.createDataFrame(
        [("global", None, None, None, acc, f1)],
        ["type", "label", "precision", "recall", "accuracy", "f1"]
    )

    per_class_metrics = per_class.select(
        lit("per_class").alias("type"),
        col("label"),
        col("precision"),
        col("recall"),
        lit(None).cast("double").alias("accuracy"),
        col("f1")
    )

    metrics_df = global_metrics.unionByName(per_class_metrics)
    log.log("EVAL")

    model.write().overwrite().save(f"{model_base_path}/model/")
    metrics_df.coalesce(1).write.mode("overwrite").json(f"{model_base_path}/metrics/")
    log.log("WRITE")

    log.end()

if __name__ == "__main__":
    train()