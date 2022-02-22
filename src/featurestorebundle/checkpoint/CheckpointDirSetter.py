from logging import Logger
from pyspark.sql import SparkSession


class CheckpointDirSetter:
    def __init__(
        self,
        logger: Logger,
        default_checkpoint_dir: str,
        spark: SparkSession,
    ):
        self.__logger = logger
        self.__default_checkpoint_dir = default_checkpoint_dir
        self.__spark = spark

    def set_checkpoint_dir_if_necessary(self):
        if not self.__spark.sparkContext.getCheckpointDir():
            self.__logger.info(f"Setting default checkpoint directory to {self.__default_checkpoint_dir}")
            self.__spark.sparkContext.setCheckpointDir(self.__default_checkpoint_dir)
