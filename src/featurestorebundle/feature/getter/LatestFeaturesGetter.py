from typing import List, Dict, Optional
from functools import reduce
from datetime import datetime
from datetime import timedelta
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.frequency.Frequencies import Frequencies
from featurestorebundle.feature.reader.FeaturesTableReader import FeaturesTableReader


class LatestFeaturesGetter:
    def __init__(self, features_reader: FeaturesTableReader, table_names: TableNames):
        self.__features_reader = features_reader
        self.__table_names = table_names

    def get_latest(self, feature_list: FeatureList, timestamp: Optional[datetime]) -> DataFrame:
        entity = feature_list.entity

        base_db = self.__get_base_db(feature_list)

        if timestamp is None:
            full_table_name = self.__table_names.get_latest_features_full_table_name_for_base_db(base_db, entity.name)

            return self.__features_reader.read(full_table_name).select(*entity.get_primary_key(), *feature_list.get_names())

        feature_groups = self.__group_features_with_same_compute_date(feature_list, timestamp)

        dataframes = []

        for datetime_, features_ in feature_groups.items():
            full_table_name = self.__table_names.get_archive_features_full_table_name_for_base_db(
                base_db, entity.name, datetime_.strftime("%Y-%m-%d")
            )

            dataframes.append(self.__features_reader.read(full_table_name).select(entity.id_column, *features_))

        features_data = reduce(lambda df1, df2: df1.join(df2, on=entity.id_column, how="outer"), dataframes)
        features_data = features_data.withColumn(entity.time_column, f.lit(timestamp))
        features_data = features_data.select(*entity.get_primary_key(), *feature_list.get_names())

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

    def __get_base_db(self, feature_list: FeatureList) -> str:
        base_db = {feature.template.base_db for feature in feature_list.get_all()}

        if len(base_db) != 1:
            raise Exception("LatestFeaturesGetter: Can only obtain features from one base database")

        return base_db.pop()
