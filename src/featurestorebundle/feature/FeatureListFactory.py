from typing import List
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureList import FeatureList


class FeatureListFactory:
    def create(self, metadata: DataFrame, entity_name: str, features: List[str]) -> FeatureList:
        metadata = metadata.filter(f.col("entity") == entity_name)

        if features:
            metadata = metadata.filter(f.col("feature").isin(features))

        rows = metadata.collect()
        feature_instances = []

        for row in rows:
            default_value = None if row.default_value == "None" else row.default_value
            feature_template = FeatureTemplate(row.feature_template, row.description_template, default_value, row.category)
            feature_instance = FeatureInstance(row.entity, row.feature, row.description, row.dtype, row.extra, feature_template)
            feature_instances.append(feature_instance)

        return FeatureList(feature_instances)
