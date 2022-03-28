from typing import List, Optional
from datetime import datetime
from pyspark.sql import DataFrame
from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface
from featurestorebundle.metadata.reader.MetadataReaderInterface import MetadataReaderInterface
from featurestorebundle.target.reader.TargetsReaderInterface import TargetsReaderInterface
from featurestorebundle.delta.feature.FeaturesFilteringManager import FeaturesFilteringManager
from featurestorebundle.delta.target.TargetsFilteringManager import TargetsFilteringManager
from featurestorebundle.feature.FeatureListFactory import FeatureListFactory


class FeatureStore:
    def __init__(
        self,
        features_reader: FeaturesReaderInterface,
        metadata_reader: MetadataReaderInterface,
        targets_reader: TargetsReaderInterface,
        features_filtering_manager: FeaturesFilteringManager,
        targets_filtering_manager: TargetsFilteringManager,
        feature_list_factory: FeatureListFactory,
        entity_getter: EntityGetter,
    ):
        self.__features_reader = features_reader
        self.__metadata_reader = metadata_reader
        self.__targets_reader = targets_reader
        self.__features_filtering_manager = features_filtering_manager
        self.__targets_filtering_manager = targets_filtering_manager
        self.__feature_list_factory = feature_list_factory
        self.__entity_getter = entity_getter

    def get_latest(
        self,
        entity_name: str,
        features: Optional[List[str]] = None,
        skip_incomplete_rows: bool = False,
    ) -> DataFrame:
        features = features or []
        feature_store = self.__features_reader.read(entity_name)
        metadata = self.__metadata_reader.read()
        feature_list = self.__feature_list_factory.create(metadata, entity_name, features)

        return self.__features_filtering_manager.get_latest(feature_store, feature_list, features, skip_incomplete_rows)

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
        features = features or []
        entity = self.__entity_getter.get_by_name(entity_name)
        feature_store = self.__features_reader.read(entity_name)
        target_store = self.__targets_reader.read(entity_name)
        targets = self.__targets_filtering_manager.get_targets(
            entity, target_store, target_name, target_date_from, target_date_to, time_diff
        )
        metadata = self.__metadata_reader.read()
        feature_list = self.__feature_list_factory.create(metadata, entity_name, features)

        return self.__features_filtering_manager.get_for_target(feature_store, targets, feature_list, features, skip_incomplete_rows)

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
    ) -> DataFrame:
        return self.__metadata_reader.read(entity_name)
