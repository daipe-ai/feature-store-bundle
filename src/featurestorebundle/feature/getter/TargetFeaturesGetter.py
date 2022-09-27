from typing import Optional
from datetime import datetime
from pyspark.sql import DataFrame
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.reader.FeaturesTableReader import FeaturesTableReader
from featurestorebundle.target.getter.TargetsGetter import TargetsGetter


class TargetFeaturesGetter:
    def __init__(self, features_reader: FeaturesTableReader, targets_getter: TargetsGetter, table_names: TableNames):
        self.__features_reader = features_reader
        self.__targets_getter = targets_getter
        self.__table_names = table_names

    # pylint: disable=too-many-locals
    def get_for_target(
        self,
        feature_list: FeatureList,
        target_id: str,
        date_from: Optional[datetime],
        dato_to: Optional[datetime],
        time_diff: Optional[str],
    ) -> DataFrame:
        entity = feature_list.entity
        base_db = self.__get_base_db(feature_list)
        features_table = self.__table_names.get_target_features_full_table_name_for_base_db(base_db, entity.name, target_id)
        features = self.__features_reader.read(features_table)
        targets = self.__targets_getter.get_targets(entity, target_id, date_from, dato_to, time_diff)

        if len(targets.columns) != 2:
            raise Exception("Targets dataframe must have exactly two columns [id, timestamp]")

        features_id_column = features.schema[0]
        features_time_column = features.schema[1]
        targets_id_column = targets.schema[0]
        targets_time_column = targets.schema[1]

        if features_id_column.name != targets_id_column.name or features_id_column.dataType != targets_id_column.dataType:
            raise Exception(
                f"Features and Targets id column mismatch "
                f"{features_id_column.name} ({features_id_column.dataType.typeName()}) != "
                f"{targets_id_column.name} ({targets_id_column.dataType.typeName()})"
            )

        if features_time_column.name != targets_time_column.name or features_time_column.dataType != targets_time_column.dataType:
            raise Exception(
                f"Features and Targets time column mismatch "
                f"{features_time_column.name} ({features_time_column.dataType.typeName()}) != "
                f"{targets_time_column.name} ({targets_time_column.dataType.typeName()})"
            )

        features_data = features.join(targets, on=entity.get_primary_key())
        features_data = features_data.select(*entity.get_primary_key(), *feature_list.get_names())

        return features_data

    def __get_base_db(self, feature_list: FeatureList) -> str:
        base_db = {feature.template.base_db for feature in feature_list.get_all()}

        if len(base_db) != 1:
            raise Exception("TargetFeaturesGetter: Can only obtain features from one base database")

        return base_db.pop()
