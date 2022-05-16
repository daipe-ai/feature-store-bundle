from typing import List, Optional
from datetime import datetime
from datetime import timedelta
from pyspark.sql import DataFrame
from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface
from featurestorebundle.metadata.reader.MetadataReaderInterface import MetadataReaderInterface
from featurestorebundle.target.reader.TargetsReaderInterface import TargetsReaderInterface
from featurestorebundle.delta.feature.filter.FeaturesFilteringManager import FeaturesFilteringManager
from featurestorebundle.delta.feature.NullHandler import NullHandler
from featurestorebundle.delta.target.TargetsFilteringManager import TargetsFilteringManager
from featurestorebundle.feature.FeaturesManager import FeaturesManager
from featurestorebundle.feature.FeatureListFactory import FeatureListFactory
from featurestorebundle.delta.metadata.filter import get_metadata_for_entity, get_metadata_for_features


# pylint: disable=too-many-instance-attributes
class FeatureStore:
    def __init__(
        self,
        features_reader: FeaturesReaderInterface,
        metadata_reader: MetadataReaderInterface,
        targets_reader: TargetsReaderInterface,
        features_filtering_manager: FeaturesFilteringManager,
        targets_filtering_manager: TargetsFilteringManager,
        features_manager: FeaturesManager,
        feature_list_factory: FeatureListFactory,
        null_handler: NullHandler,
        entity_getter: EntityGetter,
    ):
        self.__features_reader = features_reader
        self.__metadata_reader = metadata_reader
        self.__targets_reader = targets_reader
        self.__features_filtering_manager = features_filtering_manager
        self.__targets_filtering_manager = targets_filtering_manager
        self.__features_manager = features_manager
        self.__feature_list_factory = feature_list_factory
        self.__null_handler = null_handler
        self.__entity_getter = entity_getter

    def get_latest(
        self,
        entity_name: str,
        timestamp: Optional[datetime] = None,
        lookback: Optional[str] = None,
        features: Optional[List[str]] = None,
        skip_incomplete_rows: bool = False,
        include_all: bool = False,
    ) -> DataFrame:
        entity = self.__entity_getter.get_by_name(entity_name)
        feature_store = self.__features_reader.read(entity_name)
        metadata = self.get_metadata(entity_name)
        feature_list = self.__feature_list_factory.create(metadata)

        if not include_all:
            all_feature_names = feature_list.get_names()
            feature_list = feature_list.remove_nonfeatures()
            nonfeature_names = list(set(all_feature_names) - set(feature_list.get_names()))
            feature_store = feature_store.drop(*nonfeature_names)

        features = features or self.__features_manager.get_registered_features(feature_store)

        timestamp = timestamp or feature_list.get_max_last_compute_date()
        look_back_days = timedelta(days=int(lookback[:-1])) if lookback is not None else timestamp - datetime.min

        self.__features_manager.check_features_registered(feature_store, features)

        features_data = self.__features_filtering_manager.get_latest(
            feature_store, feature_list, timestamp, look_back_days, features, skip_incomplete_rows
        )
        features_data = self.__null_handler.from_storage_format(features_data, feature_list, entity)

        return features_data

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
        feature_store = self.__features_reader.read(entity_name)
        metadata = self.get_metadata(entity_name)
        feature_list = self.__feature_list_factory.create(metadata)

        all_feature_names = feature_list.get_names()
        feature_list = feature_list.remove_nonfeatures()
        nonfeature_names = list(set(all_feature_names) - set(feature_list.get_names()))
        feature_store = feature_store.drop(*nonfeature_names)

        features = features or self.__features_manager.get_registered_features(feature_store)

        targets = self.get_targets(entity_name, target_name, target_date_from, target_date_to, time_diff)

        self.__features_manager.check_features_registered(feature_store, features)

        features_data = self.__features_filtering_manager.get_for_target(feature_store, targets, features, skip_incomplete_rows)
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
        metadata = self.__metadata_reader.read(entity_name)

        if entity_name is not None:
            metadata = get_metadata_for_entity(metadata, entity_name)

        if features is not None:
            metadata = get_metadata_for_features(metadata, features)

        return metadata
