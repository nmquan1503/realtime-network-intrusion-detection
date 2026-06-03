import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

import boto3
from pyspark.ml import PipelineModel

import config


MODEL_SUCCESS_RE = re.compile(
    r"^model/year=(\d{4})/month=(\d{2})/day=(\d{2})/"
    r"(\d{2})-(\d{2})-(\d{2})/model/metadata/_SUCCESS$"
)


@dataclass
class LoadedModel:
    version: str
    path: str
    pipeline: PipelineModel
    assembler: object
    classifier: object
    labels: List[str]
    feature_columns: List[str]


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def _iter_model_success_keys(s3):
    continuation_token = None

    while True:
        kwargs = {
            "Bucket": config.MINIO_BUCKET,
            "Prefix": config.MODEL_PREFIX,
        }
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        response = s3.list_objects_v2(**kwargs)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            if MODEL_SUCCESS_RE.match(key):
                yield key

        if not response.get("IsTruncated"):
            break
        continuation_token = response.get("NextContinuationToken")


def find_latest_model_version() -> Optional[Tuple[str, str]]:
    s3 = create_s3_client()
    candidates = []

    for key in _iter_model_success_keys(s3):
        match = MODEL_SUCCESS_RE.match(key)
        if not match:
            continue
        year, month, day, hour, minute, second = map(int, match.groups())
        created_at = datetime(year, month, day, hour, minute, second)
        version = f"year={year:04d}/month={month:02d}/day={day:02d}/{hour:02d}-{minute:02d}-{second:02d}"
        model_path = f"s3a://{config.MINIO_BUCKET}/model/{version}/model/"
        candidates.append((created_at, version, model_path))

    if not candidates:
        return None

    _, version, model_path = max(candidates, key=lambda item: item[0])
    return version, model_path


def load_pipeline_model(spark, version: str, model_path: str) -> LoadedModel:
    pipeline = PipelineModel.load(model_path)
    if len(pipeline.stages) < 3:
        raise ValueError(f"Expected at least 3 pipeline stages, got {len(pipeline.stages)}")

    label_indexer = pipeline.stages[0]
    assembler = pipeline.stages[1]
    classifier = pipeline.stages[2]

    labels = list(getattr(label_indexer, "labels", []))
    feature_columns = list(assembler.getInputCols())

    return LoadedModel(
        version=version,
        path=model_path,
        pipeline=pipeline,
        assembler=assembler,
        classifier=classifier,
        labels=labels,
        feature_columns=feature_columns,
    )
