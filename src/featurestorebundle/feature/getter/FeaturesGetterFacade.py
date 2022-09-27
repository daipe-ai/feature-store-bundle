from typing import List, Optional
from functools import reduce
from datetime import datetime
from pyspark.sql import DataFrame

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.getter.LatestFeaturesGetter import LatestFeaturesGetter
from featurestorebundle.feature.getter.TargetFeaturesGetter import TargetFeaturesGetter


class FeaturesGetterFacade:
    def __init__(self, latest_features_getter: LatestFeaturesGetter, target_features_getter: TargetFeaturesGetter):
        self.__latest_features_getter = latest_features_getter
        self.__target_features_getter = target_features_getter

    def get_latest(self, feature_list: FeatureList, timestamp: Optional[datetime]) -> DataFrame:
        feature_list_split = self.__split_feature_list_by_base_dbs(feature_list)

        feature_tables = [self.__latest_features_getter.get_latest(feature_list, timestamp) for feature_list in feature_list_split]

        return self.__join_feature_tables(feature_list.entity, feature_tables)

    def get_for_target(
        self,
        feature_list: FeatureList,
        target_id: str,
        date_from: Optional[datetime],
        dato_to: Optional[datetime],
        time_diff: Optional[str],
    ) -> DataFrame:
        feature_list_split = self.__split_feature_list_by_base_dbs(feature_list)

        feature_tables = [
            self.__target_features_getter.get_for_target(feature_list, target_id, date_from, dato_to, time_diff)
            for feature_list in feature_list_split
        ]

        return self.__join_feature_tables(feature_list.entity, feature_tables)

    def __split_feature_list_by_base_dbs(self, feature_list: FeatureList) -> List[FeatureList]:
        base_dbs = {feature.template.base_db for feature in feature_list.get_all()}

        # pylint: disable=cell-var-from-loop
        return [feature_list.filter(lambda feature_instance: feature_instance.template.base_db == base_db) for base_db in base_dbs]

    def __join_feature_tables(self, entity: Entity, dataframes: List[DataFrame]) -> DataFrame:
        return reduce(lambda df1, df2: df1.join(df2, on=entity.get_primary_key(), how="outer"), dataframes)
