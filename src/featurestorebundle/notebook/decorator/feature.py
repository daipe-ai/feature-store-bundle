from typing import Optional, Tuple, Iterable
from daipecore.decorator.OutputDecorator import OutputDecorator
from daipecore.widgets.Widgets import Widgets
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import DataFrame

from featurestorebundle.checkpoint.CheckpointGuard import CheckpointGuard
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.ChangesCalculator import ChangesCalculator
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureTemplateMatcher import FeatureTemplateMatcher
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.delta.feature.NullHandler import NullHandler
from featurestorebundle.metadata.MetadataHTMLDisplayer import MetadataHTMLDisplayer
from featurestorebundle.checkpoint.CheckpointDirSetter import CheckpointDirSetter
from featurestorebundle.orchestration.Serializator import Serializator


# pylint: disable=invalid-name
class feature(OutputDecorator):  # noqa
    # pylint: disable=super-init-not-called
    def __init__(self, *args: Feature, entity: Entity, category: Optional[str] = None, features_storage: Optional[FeaturesStorage] = None):
        self._args = args
        self.__entity = entity
        self.__category = category
        self.__features_storage = features_storage
        self.__feature_list = None

    def modify_result(self, result, container: ContainerInterface):
        self.__check_primary_key_columns(result)

        feature_template_matcher: FeatureTemplateMatcher = container.get(FeatureTemplateMatcher)
        changes_calculator: ChangesCalculator = container.get(ChangesCalculator)
        null_handler: NullHandler = container.get(NullHandler)

        feature_list = self.__prepare_features(feature_template_matcher, result, self._args)
        result, self.__feature_list = self.__process_changes(changes_calculator, feature_list, result)

        return null_handler.fill_nulls(result, self.__feature_list)

    def process_result(self, result: DataFrame, container: ContainerInterface):
        widgets: Widgets = container.get(Widgets)
        checkpoint_dir_setter: CheckpointDirSetter = container.get(CheckpointDirSetter)
        serializator: Serializator = container.get(Serializator)
        checkpoint_guard: CheckpointGuard = container.get(CheckpointGuard)

        if container.get_parameters().featurestorebundle.metadata.display_in_notebook is True:
            metadata_html_displayer: MetadataHTMLDisplayer = container.get(MetadataHTMLDisplayer)
            metadata_html_displayer.display(self.__feature_list.get_metadata_dicts())

        if checkpoint_guard.should_checkpoint_result() is True:
            checkpoint_dir_setter.set_checkpoint_dir_if_necessary()
            result = result.checkpoint()

        if self.__features_storage is not None:
            self.__features_storage.add(result, self.__feature_list)

        if self.__orchestration_id_widget_exists(widgets):
            orchestration_id = widgets.get_value("daipe_features_orchestration_id")
            serializator.serialize(result, self.__feature_list, orchestration_id)

    def __orchestration_id_widget_exists(self, widgets: Widgets) -> bool:
        try:
            widgets.get_value("daipe_features_orchestration_id")
            return True

        except Exception:  # noqa pylint: disable=broad-except
            return False

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

    def __prepare_features(
        self,
        feature_template_matcher: FeatureTemplateMatcher,
        df: DataFrame,
        features: Iterable[Feature],
    ) -> FeatureList:
        """
        @[foo]_feature(
            Feature("delayed_flights_pct_30d", "% of delayed flights in last 30 days", fillna_with=0),
            FeatureWithChange("early_flights_pct_30d", "% of flights landed ahead of time in last 30 days", fillna_with=0)
        )
        """
        feature_templates = [feature.create_template(self.__category) for feature in features]

        return feature_template_matcher.get_features(self.__entity, feature_templates, df)
