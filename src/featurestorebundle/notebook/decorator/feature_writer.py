from daipecore.decorator.OutputDecorator import OutputDecorator
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureDataMerger import FeatureDataMerger
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.TablePreparer import TablePreparer


class feature_writer(OutputDecorator):  # noqa: N801
    def __init__(
        self,
        *args,
        entity: Entity,
        category: str = None,
    ):
        self._args = args
        self.__entity = entity
        self.__category = category

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

    def process_result(self, result: DataFrame, container: ContainerInterface):
        table_preparer: TablePreparer = container.get(TablePreparer)
        feature_data_manager: FeatureDataMerger = container.get(FeatureDataMerger)

        current_feature_list = self.__prepare_features(self._args)

        table_preparer.prepare(self.__entity, current_feature_list)

        feature_data_manager.merge(
            self.__entity,
            current_feature_list,
            result,
        )
