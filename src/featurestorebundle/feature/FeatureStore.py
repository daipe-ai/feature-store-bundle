from typing import List, Optional, Callable
from functools import reduce
from datetime import datetime, timedelta
from pyspark.sql import DataFrame

from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureStoreStorageInfo import FeatureStoreStorageInfo
from featurestorebundle.feature.reader.FeaturesReader import FeaturesReader
from featurestorebundle.metadata.reader.MetadataReader import MetadataReader
from featurestorebundle.target.reader.TargetsReader import TargetsReader
from featurestorebundle.delta.feature.filter.FeaturesFilteringManager import FeaturesFilteringManager
from featurestorebundle.delta.feature.NullHandler import NullHandler
from featurestorebundle.delta.target.TargetsFilteringManager import TargetsFilteringManager
from featurestorebundle.feature.FeatureListFactory import FeatureListFactory
from featurestorebundle.delta.metadata.filter import get_metadata_for_entity, get_metadata_for_features


# pylint: disable=too-many-instance-attributes
class FeatureStore:
    def __init__(
        self,
        features_reader: FeaturesReader,
        metadata_reader: MetadataReader,
        targets_reader: TargetsReader,
        features_filtering_manager: FeaturesFilteringManager,
        targets_filtering_manager: TargetsFilteringManager,
        feature_list_factory: FeatureListFactory,
        null_handler: NullHandler,
        entity_getter: EntityGetter,
    ):
        self.__features_reader = features_reader
        self.__metadata_reader = metadata_reader
        self.__targets_reader = targets_reader
        self.__features_filtering_manager = features_filtering_manager
        self.__targets_filtering_manager = targets_filtering_manager
        self.__feature_list_factory = feature_list_factory
        self.__null_handler = null_handler
        self.__entity_getter = entity_getter

    # pylint: disable=too-many-locals
    def get_latest_attributes(
        self,
        entity_name: str,
        timestamp: Optional[datetime] = None,
        lookback: Optional[str] = None,
        attributes: Optional[List[str]] = None,
        skip_incomplete_rows: bool = False,
    ) -> DataFrame:
        relevant_attribute_list = self.__get_relevant_attribute_list(entity_name, attributes)

        def wrapper(feature_store_info: FeatureStoreStorageInfo, feature_list: FeatureList) -> DataFrame:
            selected_timestamp = timestamp or feature_list.get_max_last_compute_date()
            look_back_days = timedelta(days=int(lookback[:-1])) if lookback is not None else selected_timestamp - datetime.min

            return self.__features_filtering_manager.get_latest(
                feature_store_info.feature_store,
                feature_list,
                selected_timestamp,
                look_back_days,
                skip_incomplete_rows,
            )

        return self.__get(relevant_attribute_list, wrapper)

    # pylint: disable=too-many-locals
    def get_latest_features(
        self,
        entity_name: str,
        timestamp: Optional[datetime] = None,
        lookback: Optional[str] = None,
        features: Optional[List[str]] = None,
        skip_incomplete_rows: bool = False,
    ) -> DataFrame:
        relevant_feature_list = self.__get_relevant_feature_list(entity_name, features)

        def wrapper(feature_store_info: FeatureStoreStorageInfo, feature_list: FeatureList) -> DataFrame:
            selected_timestamp = timestamp or feature_list.get_max_last_compute_date()
            look_back_days = timedelta(days=int(lookback[:-1])) if lookback is not None else selected_timestamp - datetime.min

            return self.__features_filtering_manager.get_latest(
                feature_store_info.feature_store,
                feature_list,
                selected_timestamp,
                look_back_days,
                skip_incomplete_rows,
            )

        return self.__get(relevant_feature_list, wrapper)

    def get_latest(
        self,
        entity_name: str,
        timestamp: Optional[datetime] = None,
        lookback: Optional[str] = None,
        features: Optional[List[str]] = None,
        skip_incomplete_rows: bool = False,
    ) -> DataFrame:
        return self.get_latest_features(
            entity_name=entity_name,
            timestamp=timestamp,
            lookback=lookback,
            features=features,
            skip_incomplete_rows=skip_incomplete_rows,
        )

    # pylint: disable=too-many-locals
    def get_for_target(
        self,
        entity_name: str,
        target_name: str,
        target_date_from: Optional[datetime] = None,
        target_date_to: Optional[datetime] = None,
        time_diff: Optional[str] = None,
        features: Optional[List[str]] = None,
        skip_incomplete_rows: bool = False,
    ) -> DataFrame:
        targets = self.get_targets(entity_name, target_name, target_date_from, target_date_to, time_diff)

        relevant_feature_list = self.__get_relevant_feature_list(entity_name, features)

        def wrapper(feature_store_info: FeatureStoreStorageInfo, feature_list: FeatureList) -> DataFrame:
            return self.__features_filtering_manager.get_for_target(
                feature_store_info.feature_store,
                targets,
                feature_list,
                skip_incomplete_rows,
            )

        return self.__get(relevant_feature_list, wrapper)

    def get_targets(
        self,
        entity_name: str,
        target_name: str,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        time_diff: Optional[str] = None,
    ) -> DataFrame:
        entity = self.__entity_getter.get_by_name(entity_name)
        target_store = self.__targets_reader.read(entity_name)

        return self.__targets_filtering_manager.get_targets(entity, target_store, target_name, date_from, date_to, time_diff)

    def get_metadata(self, entity_name: Optional[str] = None, features: Optional[List[str]] = None) -> DataFrame:
        metadata = self.__metadata_reader.read(entity_name)

        if entity_name is not None:
            metadata = get_metadata_for_entity(metadata, entity_name)

        if features is not None:
            metadata = get_metadata_for_features(metadata, features)

        return metadata

    def __get_attribute_list(self, entity_name: str) -> FeatureList:
        entity = self.__entity_getter.get_by_name(entity_name)
        metadata = self.get_metadata(entity_name)

        return self.__feature_list_factory.create(entity, metadata)

    def __get_relevant_attribute_list(self, entity_name: str, attributes: Optional[List[str]]) -> FeatureList:
        attribute_list = self.__get_attribute_list(entity_name)

        attributes = attributes or attribute_list.get_names()
        attribute_list.check_features_registered(attributes)

        return attribute_list.filter(lambda attribute_instance: attribute_instance.name in attributes)

    def __get_relevant_feature_list(self, entity_name: str, features: Optional[List[str]]) -> FeatureList:
        attribute_list = self.__get_attribute_list(entity_name)
        feature_list = attribute_list.filter(lambda feature_instance: feature_instance.is_feature)

        features = features or feature_list.get_names()
        feature_list.check_features_registered(features)

        return feature_list.filter(lambda feature_instance: feature_instance.name in features)

    def __get(
        self, feature_list: FeatureList, get_dataframe_function: Callable[[FeatureStoreStorageInfo, FeatureList], DataFrame]
    ) -> DataFrame:
        feature_store_info_list = self.__features_reader.read(feature_list)

        dataframes = []
        for feature_store_info in feature_store_info_list:
            # pylint: disable=cell-var-from-loop
            relevant_location_feature_list = feature_list.filter(
                lambda feature_instance: feature_instance.template.location == feature_store_info.location
            )

            dataframes.append(get_dataframe_function(feature_store_info, relevant_location_feature_list))

        features_data = reduce(lambda df1, df2: df1.join(df2, on=feature_list.entity.get_primary_key(), how="outer"), dataframes)
        features_data = self.__null_handler.from_storage_format(features_data, feature_list)

        return features_data
