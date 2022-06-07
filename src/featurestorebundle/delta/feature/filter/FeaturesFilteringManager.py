from datetime import datetime
from datetime import timedelta
from pyspark.sql import DataFrame
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.delta.feature.filter.LatestFeaturesFilterer import LatestFeaturesFilterer
from featurestorebundle.delta.feature.filter.TargetFeaturesFilterer import TargetFeaturesFilterer


class FeaturesFilteringManager:
    def __init__(
        self,
        latest_features_filterer: LatestFeaturesFilterer,
        target_features_filterer: TargetFeaturesFilterer,
    ):
        self.__latest_features_filterer = latest_features_filterer
        self.__target_features_filterer = target_features_filterer

    def get_latest(
        self,
        feature_store: DataFrame,
        feature_list: FeatureList,
        timestamp: datetime,
        lookback: timedelta,
        skip_incomplete_rows: bool,
    ):
        return self.__latest_features_filterer.get_latest(
            feature_store,
            feature_list,
            timestamp,
            lookback,
            skip_incomplete_rows,
        )

    def get_for_target(
        self,
        feature_store: DataFrame,
        targets: DataFrame,
        feature_list: FeatureList,
        skip_incomplete_rows: bool,
    ) -> DataFrame:
        return self.__target_features_filterer.get_for_target(
            feature_store,
            targets,
            feature_list,
            skip_incomplete_rows,
        )
