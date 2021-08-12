from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.metadata.MetadataExtractor import MetadataExtractor


class MetadataWriter:
    def __init__(self, spark: SparkSession, metadata_extractor: MetadataExtractor):
        self.__spark = spark
        self.__metadata_extractor = metadata_extractor

    def write_metadata(self, metadata_path: str, entity: Entity, feature_list: FeatureList, features_data: DataFrame):
        metadata_columns = ["feature", "description", "category", "feature_template", "misc"]
        metadata_df = self.__spark.createDataFrame(
            self.__metadata_extractor.get_metadata(entity, feature_list, features_data.columns), metadata_columns
        )

        if not DeltaTable.isDeltaTable(self.__spark, metadata_path):
            metadata_df.write.format("delta").save(metadata_path)
            return

        delta_table = DeltaTable.forPath(self.__spark, metadata_path)

        update_set = {
            "description": "source.description",
            "category": "source.category",
            "feature_template": "source.feature_template",
            "misc": "source.misc",
        }

        insert_set = {"feature": "source.feature", **update_set}

        (
            delta_table.alias("target")
            .merge(metadata_df.alias("source"), "target.feature = source.feature")
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_set)
            .execute()
        )
