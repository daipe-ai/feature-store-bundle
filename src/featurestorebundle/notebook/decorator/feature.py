from daipecore.decorator.OutputDecorator import OutputDecorator
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureTemplateMatcher import FeatureTemplateMatcher
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.metadata.MetadataHTMLDisplayer import MetadataHTMLDisplayer


class feature(OutputDecorator):  # noqa: N801
    def __init__(self, *args, entity: Entity, category: str = None, features_storage: FeaturesStorage = None):
        self._args = args
        self.__entity = entity
        self.__category = category
        self.__features_storage = features_storage

    def process_result(self, result: DataFrame, container: ContainerInterface):
        self.__check_primary_key_columns(result)

        if self.__features_storage:
            feature_template_matcher: FeatureTemplateMatcher = container.get(FeatureTemplateMatcher)

            feature_list = self.__prepare_features(feature_template_matcher, result, self._args)
            self.__features_storage.add(result, feature_list)

            metadata_html_displayer: MetadataHTMLDisplayer = container.get(MetadataHTMLDisplayer)
            metadata_html_displayer.display(feature_list.get_metadata_dicts())

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
        feature_templates = [FeatureTemplate(*arg, category=self.__category) for arg in args]
        return feature_template_matcher.get_features(self.__entity, feature_templates, df)
