from typing import Optional, Tuple
from daipecore.decorator.OutputDecorator import OutputDecorator
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.ChangesCalculator import ChangesCalculator
from featurestorebundle.feature.FeatureTemplateMatcher import FeatureTemplateMatcher
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureWithChange import FeatureWithChange
from featurestorebundle.feature.FeatureWithChangeTemplate import FeatureWithChangeTemplate
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.metadata.MetadataHTMLDisplayer import MetadataHTMLDisplayer


# pylint: disable=invalid-name
class feature(OutputDecorator):  # noqa
    # pylint: disable=super-init-not-called
    def __init__(self, *args, entity: Entity, category: Optional[str] = None, features_storage: Optional[FeaturesStorage] = None):
        self._args = args
        self.__entity = entity
        self.__category = category
        self.__features_storage = features_storage
        self.__feature_list = None

    def modify_result(self, result, container: ContainerInterface):
        self.__check_primary_key_columns(result)

        feature_template_matcher: FeatureTemplateMatcher = container.get(FeatureTemplateMatcher)

        changes_calculator: ChangesCalculator = container.get(ChangesCalculator)
        feature_list = self.__prepare_features(feature_template_matcher, result, self._args)

        result, self.__feature_list = self.__process_changes(changes_calculator, feature_list, result)

        return result

    def process_result(self, result: DataFrame, container: ContainerInterface):
        spark: SparkSession = container.get(SparkSession)

        if not spark.sparkContext.getCheckpointDir():
            spark.sparkContext.setCheckpointDir("dbfs:/tmp/checkpoints")

        if container.get_parameters().featurestorebundle.metadata.display_in_notebook is True:
            metadata_html_displayer: MetadataHTMLDisplayer = container.get(MetadataHTMLDisplayer)
            metadata_html_displayer.display(self.__feature_list.get_metadata_dicts())

        if container.get_parameters().featurestorebundle.orchestration.checkpoint_feature_results is True:
            result = result.checkpoint()

        if self.__features_storage is not None:
            self.__features_storage.add(result, self.__feature_list)

    def __process_changes(
        self, changes_calculator: ChangesCalculator, feature_list: FeatureList, result: DataFrame
    ) -> Tuple[DataFrame, FeatureList]:

        change_master_features = feature_list.get_change_features()

        change_columns, change_feature_list = changes_calculator.get_changes(change_master_features, self.__entity.name)

        return result.select("*", *change_columns), feature_list.merge(change_feature_list)

    def __check_primary_key_columns(self, result: DataFrame):
        if self.__entity.id_column not in result.columns:
            raise Exception(f"{self.__entity.id_column} column is missing in the output dataframe")

        if self.__entity.time_column not in result.columns:
            raise Exception(f"{self.__entity.time_column} column is missing in the output dataframe")

    def __prepare_features(self, feature_template_matcher: FeatureTemplateMatcher, df: DataFrame, args: tuple) -> FeatureList:
        """
        @[foo]_feature(
            ("delayed_flights_pct_30d", "% of delayed flights in last 30 days"),
            ("early_flights_pct_30d", "% of flights landed ahead of time in last 30 days")
        )
        """
        feature_templates = [
            FeatureWithChangeTemplate(arg.name_template, arg.description_template, category=self.__category)
            if isinstance(arg, FeatureWithChange)
            else FeatureTemplate(*arg, category=self.__category)
            for arg in args
        ]

        return feature_template_matcher.get_features(self.__entity, feature_templates, df)
