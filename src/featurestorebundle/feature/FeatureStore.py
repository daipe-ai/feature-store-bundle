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
from featurestorebundle.delta.metadata.filter.MetadataFilteringManager import MetadataFilteringManager
from featurestorebundle.delta.target.TargetsFilteringManager import TargetsFilteringManager
from featurestorebundle.feature.FeatureListFactory import FeatureListFactory


# pylint: disable=too-many-instance-attributes
class FeatureStore:
    def __init__(
        self,
        features_reader: FeaturesReader,
        metadata_reader: MetadataReader,
        targets_reader: TargetsReader,
        features_filtering_manager: FeaturesFilteringManager,
        metadata_filtering_manager: MetadataFilteringManager,
        targets_filtering_manager: TargetsFilteringManager,
        feature_list_factory: FeatureListFactory,
        null_handler: NullHandler,
        entity_getter: EntityGetter,
    ):
        self.__features_reader = features_reader
        self.__metadata_reader = metadata_reader
        self.__targets_reader = targets_reader
        self.__features_filtering_manager = features_filtering_manager
        self.__metadata_filtering_manager = metadata_filtering_manager
        self.__targets_filtering_manager = targets_filtering_manager
        self.__feature_list_factory = feature_list_factory
        self.__null_handler = null_handler
        self.__entity_getter = entity_getter

    # pylint: disable=too-many-locals
    def get_latest(
        self,
        entity_name: str,
        timestamp: Optional[datetime] = None,
        lookback: Optional[str] = None,
        features: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        time_windows: Optional[List[str]] = None,
        include_tags: Optional[List[str]] = None,
        exclude_tags: Optional[List[str]] = None,
        skip_incomplete_rows: bool = False,
    ) -> DataFrame:
        metadata = self.__metadata_reader.read_for_latest(entity_name)

        relevant_feature_list = self.__get_relevant_feature_list(
            metadata, entity_name, features, categories, time_windows, include_tags, exclude_tags
        )

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

    # pylint: disable=too-many-locals
    def get_for_target(
        self,
        entity_name: str,
        target_name: str,
        target_date_from: Optional[datetime] = None,
        target_date_to: Optional[datetime] = None,
        time_diff: Optional[str] = None,
        features: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        time_windows: Optional[List[str]] = None,
        include_tags: Optional[List[str]] = None,
        exclude_tags: Optional[List[str]] = None,
        skip_incomplete_rows: bool = False,
    ) -> DataFrame:
        metadata = self.__metadata_reader.read_for_target(entity_name, target_name)
        targets = self.get_targets(entity_name, target_name, target_date_from, target_date_to, time_diff)

        relevant_feature_list = self.__get_relevant_feature_list(
            metadata, entity_name, features, categories, time_windows, include_tags, exclude_tags
        )

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

    def get_metadata(
        self,
        entity_name: Optional[str] = None,
        features: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        time_windows: Optional[List[str]] = None,
        include_tags: Optional[List[str]] = None,
        exclude_tags: Optional[List[str]] = None,
    ) -> DataFrame:
        metadata = self.__metadata_reader.read(entity_name)
        metadata = self.__metadata_filtering_manager.filter(
            metadata, entity_name, features, categories, time_windows, include_tags, exclude_tags
        )

        return metadata

    def __get_relevant_feature_list(
        self,
        metadata: DataFrame,
        entity_name: str,
        features: Optional[List[str]],
        categories: Optional[List[str]],
        time_windows: Optional[List[str]],
        include_tags: Optional[List[str]],
        exclude_tags: Optional[List[str]],
    ) -> FeatureList:
        metadata = self.__metadata_filtering_manager.filter(
            metadata, entity_name, features, categories, time_windows, include_tags, exclude_tags
        )

        entity = self.__entity_getter.get_by_name(entity_name)
        feature_list = self.__feature_list_factory.create(entity, metadata)

        if feature_list.empty():
            raise Exception("Your filtering conditions did not match any features")

        return feature_list

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
