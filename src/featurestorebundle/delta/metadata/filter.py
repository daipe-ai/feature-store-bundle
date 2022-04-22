from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def get_metadata_for_entity(metadata: DataFrame, entity_name: str) -> DataFrame:
    return metadata.filter(f.col("entity") == entity_name)


def get_metadata_for_features(metadata: DataFrame, features: List[str]) -> DataFrame:
    return metadata.filter(f.col("feature").isin(features))
