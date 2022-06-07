from typing import List, Optional
from functools import reduce
from datetime import datetime
from datetime import timedelta
from pyspark.sql import DataFrame
from featurestorebundle.entity.EntityGetter import EntityGetter
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
        entity = self.__entity_getter.get_by_name(entity_name)
        metadata = self.get_metadata(entity_name)
        attribute_list = self.__feature_list_factory.create(metadata)
        attributes = attributes or attribute_list.get_names()
        attribute_list.check_features_registered(attributes)
        relevant_attribute_list = attribute_list.filter(lambda feature_instance: feature_instance.name in attributes)
        timestamp = timestamp or attribute_list.get_max_last_compute_date()
        look_back_days = timedelta(days=int(lookback[:-1])) if lookback is not None else timestamp - datetime.min
        feature_store_info_list = self.__features_reader.read(attribute_list)
        latest_dataframes = []

        for feature_store_info in feature_store_info_list:
            # pylint: disable=cell-var-from-loop
            relevant_location_attribute_list = relevant_attribute_list.filter(
                lambda feature_instance: feature_instance.template.location == feature_store_info.location
            )

            latest_dataframes.append(
                self.__features_filtering_manager.get_latest(
                    feature_store_info.feature_store,
                    relevant_location_attribute_list,
                    timestamp,
                    look_back_days,
                    skip_incomplete_rows,
                )
            )

        attributes_data = reduce(lambda df1, df2: df1.join(df2, on=entity.id_column, how="outer"), latest_dataframes)
        attributes_data = self.__null_handler.from_storage_format(attributes_data, attribute_list, entity)

        return attributes_data

    # pylint: disable=too-many-locals
    def get_latest_features(
        self,
        entity_name: str,
        timestamp: Optional[datetime] = None,
        lookback: Optional[str] = None,
        features: Optional[List[str]] = None,
        skip_incomplete_rows: bool = False,
    ) -> DataFrame:
        entity = self.__entity_getter.get_by_name(entity_name)
        metadata = self.get_metadata(entity_name)
        attribute_list = self.__feature_list_factory.create(metadata)
        feature_list = attribute_list.filter(lambda feature_instance: feature_instance.is_feature)
        features = features or feature_list.get_names()
        feature_list.check_features_registered(features)
        relevant_feature_list = feature_list.filter(lambda feature_instance: feature_instance.name in features)
        timestamp = timestamp or feature_list.get_max_last_compute_date()
        look_back_days = timedelta(days=int(lookback[:-1])) if lookback is not None else timestamp - datetime.min
        feature_store_info_list = self.__features_reader.read(relevant_feature_list)
        latest_dataframes = []

        for feature_store_info in feature_store_info_list:
            # pylint: disable=cell-var-from-loop
            relevant_location_feature_list = relevant_feature_list.filter(
                lambda feature_instance: feature_instance.template.location == feature_store_info.location
            )

            latest_dataframes.append(
                self.__features_filtering_manager.get_latest(
                    feature_store_info.feature_store,
                    relevant_location_feature_list,
                    timestamp,
                    look_back_days,
                    skip_incomplete_rows,
                )
            )

        features_data = reduce(lambda df1, df2: df1.join(df2, on=entity.id_column, how="outer"), latest_dataframes)
        features_data = self.__null_handler.from_storage_format(features_data, feature_list, entity)

        return features_data

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
        entity = self.__entity_getter.get_by_name(entity_name)
        metadata = self.get_metadata(entity_name)
        targets = self.get_targets(entity_name, target_name, target_date_from, target_date_to, time_diff)
        attribute_list = self.__feature_list_factory.create(metadata)
        feature_list = attribute_list.filter(lambda feature_instance: feature_instance.is_feature)
        features = features or feature_list.get_names()
        feature_list.check_features_registered(features)
        relevant_feature_list = feature_list.filter(lambda feature_instance: feature_instance.name in features)
        feature_store_info_list = self.__features_reader.read(feature_list)
        historical_dataframes = []

        for feature_store_info in feature_store_info_list:
            # pylint: disable=cell-var-from-loop
            relevant_location_feature_list = relevant_feature_list.filter(
                lambda feature_instance: feature_instance.template.location == feature_store_info.location
            )

            historical_dataframes.append(
                self.__features_filtering_manager.get_for_target(
                    feature_store_info.feature_store,
                    targets,
                    relevant_location_feature_list,
                    skip_incomplete_rows,
                )
            )

        features_data = reduce(lambda df1, df2: df1.join(df2, on=entity.get_primary_key(), how="outer"), historical_dataframes)
        features_data = self.__null_handler.from_storage_format(features_data, feature_list, entity)

        return features_data

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
        metadata = self.__metadata_reader.read()

        if entity_name is not None:
            metadata = get_metadata_for_entity(metadata, entity_name)

        if features is not None:
            metadata = get_metadata_for_features(metadata, features)

        return metadata
