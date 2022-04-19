from logging import Logger
from pyspark.sql import SparkSession
from pysparkbundle.filesystem.FilesystemInterface import FilesystemInterface


class CheckpointDirHandler:
    def __init__(
        self,
        logger: Logger,
        default_checkpoint_dir: str,
        spark: SparkSession,
        filesystem: FilesystemInterface,
    ):
        self.__logger = logger
        self.__default_checkpoint_dir = default_checkpoint_dir
        self.__spark = spark
        self.__filesystem = filesystem

    def set_checkpoint_dir_if_necessary(self):
        if not self.__spark.sparkContext.getCheckpointDir():
            self.__logger.info(f"Setting default checkpoint directory to {self.__default_checkpoint_dir}")
            self.__spark.sparkContext.setCheckpointDir(self.__default_checkpoint_dir)

    def clean_checkpoint_dir(self):
        if self.__spark.sparkContext.getCheckpointDir():
            self.__logger.info(f"Cleaning checkpoint directory {self.__spark.sparkContext.getCheckpointDir()}")
            self.__filesystem.delete(self.__spark.sparkContext.getCheckpointDir(), recursive=True)
