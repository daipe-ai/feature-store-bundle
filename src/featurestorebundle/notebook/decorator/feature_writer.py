from daipecore.decorator.OutputDecorator import OutputDecorator
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.feature.writer import FeaturesWriterInterface
from featurestorebundle.feature.writer.FeaturesWriterInjector import FeaturesWriterInjector


class feature_writer(OutputDecorator):  # noqa: N801
    def __init__(self, *args, entity: Entity, category: str = None, features_storage: FeaturesStorage = None):
        self._args = args
        self.__entity = entity
        self.__category = category
        self.__features_storage = features_storage

    def process_result(self, result: DataFrame, container: ContainerInterface):
        self.__check_primary_key_columns(result)
        feature_list = self.__prepare_features(self._args)

        if self.__features_storage:
            self.__features_storage.add(result, feature_list)
        else:
            features_injector: FeaturesWriterInjector = container.get(FeaturesWriterInjector)
            features_writer: FeaturesWriterInterface = features_injector.get()
            features_writer.write(result, self.__entity, feature_list)

    def __check_primary_key_columns(self, result: DataFrame):
        if self.__entity.id_column not in result.columns and self.__entity.time_column not in result.columns:
            raise Exception(
                f"Output dataframe must contain both {self.__entity.id_column} and {self.__entity.time_column} primary key columns"
            )

        if self.__entity.id_column not in result.columns:
            raise Exception(f"{self.__entity.id_column} columns is missing in the output dataframe")

        if self.__entity.time_column not in result.columns:
            raise Exception(f"{self.__entity.time_column} columns is missing in the output dataframe")

    def __prepare_features(self, args: tuple):
        # @[foo]_feature_writer("Average delay in last 30 days", t.FloatType())
        if len(args) == 2 and isinstance(args[0], str) and isinstance(args[1], DataType):
            return FeatureList([Feature(self._function.__name__, args[0], args[1], category=self.__category)])

        """
        @[foo]_feature_writer(
            ("delayed_flights_pct_30d", "% of delayed flights in last 30 days", t.DecimalType()),
            ("early_flights_pct_30d", "% of flights landed ahead of time in last 30 days", t.DecimalType())
        )
        """
        return FeatureList([Feature(*arg, category=self.__category) for arg in args])
