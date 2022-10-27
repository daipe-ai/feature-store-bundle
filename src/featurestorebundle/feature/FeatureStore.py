from typing import List, Optional
from datetime import datetime
from pyspark.sql import DataFrame

from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureListFactory import FeatureListFactory
from featurestorebundle.feature.getter.FeaturesGetterFacade import FeaturesGetterFacade
from featurestorebundle.metadata.getter.MetadataGetter import MetadataGetter
from featurestorebundle.target.getter.TargetsGetter import TargetsGetter


class FeatureStore:
    def __init__(
        self,
        features_getter: FeaturesGetterFacade,
        metadata_getter: MetadataGetter,
        targets_getter: TargetsGetter,
        feature_list_factory: FeatureListFactory,
        entity_getter: EntityGetter,
    ):
        self.__features_getter = features_getter
        self.__metadata_getter = metadata_getter
        self.__targets_getter = targets_getter
        self.__feature_list_factory = feature_list_factory
        self.__entity_getter = entity_getter

    # pylint: disable=too-many-locals
    def get_latest(
        self,
        entity_name: str,
        timestamp: Optional[datetime] = None,
        features: Optional[List[str]] = None,
        templates: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        time_windows: Optional[List[str]] = None,
        include_tags: Optional[List[str]] = None,
        exclude_tags: Optional[List[str]] = None,
    ) -> DataFrame:
        metadata = self.__metadata_getter.get_for_latest(
            entity_name, features, templates, categories, time_windows, include_tags, exclude_tags
        )

        feature_list = self.__get_feature_list(metadata, entity_name)

        return self.__features_getter.get_latest(feature_list, timestamp)

    # pylint: disable=too-many-locals
    def get_for_target(
        self,
        entity_name: str,
        target_name: str,
        target_date_from: Optional[datetime] = None,
        target_date_to: Optional[datetime] = None,
        time_diff: Optional[str] = None,
        features: Optional[List[str]] = None,
        templates: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        time_windows: Optional[List[str]] = None,
        include_tags: Optional[List[str]] = None,
        exclude_tags: Optional[List[str]] = None,
    ) -> DataFrame:
        metadata = self.__metadata_getter.get_for_target(
            entity_name, target_name, features, templates, categories, time_windows, include_tags, exclude_tags
        )

        feature_list = self.__get_feature_list(metadata, entity_name)

        return self.__features_getter.get_for_target(feature_list, target_name, target_date_from, target_date_to, time_diff)

    def get_targets(
        self,
        entity_name: str,
        target_name: str,
        target_date_from: Optional[datetime] = None,
        target_date_to: Optional[datetime] = None,
        time_diff: Optional[str] = None,
    ) -> DataFrame:
        entity = self.__entity_getter.get_by_name(entity_name)

        return self.__targets_getter.get_targets(entity, target_name, target_date_from, target_date_to, time_diff)

    def get_metadata(
        self,
        entity_name: str,
        features: Optional[List[str]] = None,
        templates: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        time_windows: Optional[List[str]] = None,
        include_tags: Optional[List[str]] = None,
        exclude_tags: Optional[List[str]] = None,
    ) -> DataFrame:
        return self.__metadata_getter.get_metadata(entity_name, features, templates, categories, time_windows, include_tags, exclude_tags)

    def __get_feature_list(self, metadata: DataFrame, entity_name: str) -> FeatureList:
        entity = self.__entity_getter.get_by_name(entity_name)
        feature_list = self.__feature_list_factory.create(entity, metadata)

        if feature_list.empty():
            raise Exception("Feature Store does not exists yet or your filtering conditions did not match any features")

        for feature in features or []:
            if feature not in feature_list.get_names():
                raise Exception(f"Feature '{feature}' not registered in Feature Store")

        return feature_list
