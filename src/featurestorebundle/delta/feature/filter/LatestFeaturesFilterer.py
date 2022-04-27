from typing import List, Dict
from functools import reduce
from datetime import datetime
from datetime import timedelta
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.frequency.Frequencies import Frequencies
from featurestorebundle.delta.feature.filter.IncompleteRowsHandler import IncompleteRowsHandler


class LatestFeaturesFilterer:
    def __init__(self, incomplete_rows_handler: IncompleteRowsHandler):
        self.__incomplete_rows_handler = incomplete_rows_handler

    def get_latest(
        self,
        feature_store: DataFrame,
        feature_list: FeatureList,
        timestamp: datetime,
        look_back_days: timedelta,
        skip_incomplete_rows: bool,
    ) -> DataFrame:
        id_column = feature_list.entity.id_column
        time_column = feature_list.entity.time_column
        pk_columns = feature_list.entity.get_primary_key()

        feature_store = self.__filter_relevant_time_range(feature_store, timestamp, look_back_days)
        feature_groups = self.__group_features_with_same_compute_date(feature_list, timestamp)
        dataframes = [
            feature_store.filter(f.col(time_column) == datetime_).select(id_column, *features_)
            for datetime_, features_ in feature_groups.items()
        ]
        features_data = reduce(lambda df1, df2: df1.join(df2, on=id_column, how="outer"), dataframes)
        features_data = features_data.withColumn(time_column, f.lit(timestamp))
        features_data = features_data.select(*pk_columns, *feature_list.get_names())
        features_data = self.__incomplete_rows_handler.handle(features_data, skip_incomplete_rows)

        return features_data

    def __group_features_with_same_compute_date(self, feature_list: FeatureList, timestamp: datetime) -> Dict[datetime, List[str]]:
        feature_groups = {}

        for feature in feature_list.get_all():
            last_compute_date = self.__get_last_compute_date_relative_to_timestamp(
                feature.template.start_date, timestamp, feature.template.frequency
            )

            if last_compute_date not in feature_groups:
                feature_groups[last_compute_date] = []

            feature_groups[last_compute_date].append(feature.name)

        return feature_groups

    def __get_last_compute_date_relative_to_timestamp(self, start_date: datetime, timestamp: datetime, frequency: str) -> datetime:
        if frequency == Frequencies.daily:
            return timestamp

        if frequency == Frequencies.weekly:
            return timestamp - timedelta(days=timestamp.weekday())

        if frequency == Frequencies.monthly:
            return timestamp.replace(day=1)

        frequency_in_days = int(frequency[:-1])

        return timestamp - timedelta(days=(timestamp - start_date).days % frequency_in_days)

    def __filter_relevant_time_range(self, feature_store: DataFrame, timestamp: datetime, look_back_days: timedelta) -> DataFrame:
        look_back_date = timestamp - look_back_days
        time_column = feature_store.columns[1]

        return feature_store.filter(f.col(time_column).between(look_back_date, timestamp))
