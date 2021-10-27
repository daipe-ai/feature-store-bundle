from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.EmptyFileCreator import EmptyFileCreator
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureManager import FeatureManager


class TablePreparer:
    def __init__(
        self,
        empty_file_creator: EmptyFileCreator,
        features_manager: FeatureManager,
    ):
        self.__empty_file_creator = empty_file_creator
        self.__features_manager = features_manager

    def prepare(self, table_identifier: str, path: str, entity: Entity, current_feature_list: FeatureList):
        self.__empty_file_creator.create(path, entity)
        self.__register_features(table_identifier, current_feature_list)

    def __register_features(self, table_identifier: str, current_feature_list: FeatureList):
        registered_feature_names = self.__features_manager.get_feature_names(table_identifier)
        unregistered_features = current_feature_list.get_unregistered(registered_feature_names)

        if not unregistered_features.empty():
            self.__features_manager.register(table_identifier, unregistered_features)
