from logging import Logger
from pyspark.sql import DataFrame
from featurestorebundle.delta.feature.FeaturesJoiner import FeaturesJoiner
from featurestorebundle.delta.feature.NullHandler import NullHandler
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.checkpoint.CheckpointGuard import CheckpointGuard
from featurestorebundle.checkpoint.CheckpointDirHandler import CheckpointDirHandler


class FeaturesPreparer:
    def __init__(
        self,
        logger: Logger,
        features_joiner: FeaturesJoiner,
        null_handler: NullHandler,
        checkpoint_guard: CheckpointGuard,
        checkpoint_dir_handler: CheckpointDirHandler,
    ):
        self.__logger = logger
        self.__features_joiner = features_joiner
        self.__null_handler = null_handler
        self.__checkpoint_guard = checkpoint_guard
        self.__checkpoint_dir_handler = checkpoint_dir_handler

    def prepare(self, features_storage: FeaturesStorage) -> DataFrame:
        features_data = self.__features_joiner.join(features_storage)
        features_data = self.__null_handler.fill_nulls(features_data, features_storage.feature_list)
        features_data = self.__null_handler.to_storage_format(features_data, features_storage.feature_list, features_storage.entity)

        if self.__checkpoint_guard.should_checkpoint_before_merge():
            self.__logger.info("Checkpointing features data before merge")

            self.__checkpoint_dir_handler.set_checkpoint_dir_if_necessary()

            features_data = features_data.checkpoint()

            self.__logger.info("Checkpointing done")

        return features_data
