from typing import Optional, Tuple, Iterable
from logging import Logger
from daipecore.decorator.OutputDecorator import OutputDecorator
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import DataFrame

from featurestorebundle.widgets.WidgetsGetter import WidgetsGetter
from featurestorebundle.utils.DateParser import DateParser
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.ChangesCalculator import ChangesCalculator
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureTemplateMatcher import FeatureTemplateMatcher
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.delta.feature.NullHandler import NullHandler
from featurestorebundle.metadata.MetadataHTMLDisplayer import MetadataHTMLDisplayer
from featurestorebundle.checkpoint.CheckpointDirHandler import CheckpointDirHandler
from featurestorebundle.checkpoint.CheckpointGuard import CheckpointGuard
from featurestorebundle.frequency.FrequencyGuard import FrequencyGuard
from featurestorebundle.notebook.services.NotebookNameGetter import NotebookNameGetter
from featurestorebundle.feature.writer.FeaturesWriter import FeaturesWriter
from featurestorebundle.orchestration.Serializator import Serializator
from featurestorebundle.orchestration.CurrentNotebookDefinitionGetter import CurrentNotebookDefinitionGetter


# pylint: disable=invalid-name, too-many-instance-attributes
class feature(OutputDecorator):  # noqa
    # pylint: disable=super-init-not-called
    def __init__(
        self,
        *args: Feature,
        entity: Entity,
        category: Optional[str] = None,
        owner: Optional[str] = None,
        start_date: Optional[str] = None,
        frequency: Optional[str] = None,
    ):
        self._args = args
        self.__entity = entity
        self.__category = category
        self.__owner = owner
        self.__start_date = start_date
        self.__frequency = frequency
        self.__last_compute_date = None
        self.__feature_list = None

    def modify_result(self, result, container: ContainerInterface):
        self.__check_primary_key_columns(result)
        self.__set_feature_defaults(container)

        changes_calculator: ChangesCalculator = container.get(ChangesCalculator)
        null_handler: NullHandler = container.get(NullHandler)

        feature_list = self.__prepare_features(container, result, self._args)
        result, self.__feature_list = self.__process_changes(changes_calculator, feature_list, result)

        return null_handler.fill_nulls(result, self.__feature_list)

    def process_result(self, result: DataFrame, container: ContainerInterface):
        logger: Logger = container.get("featurestorebundle.logger")
        widgets_getter: WidgetsGetter = container.get(WidgetsGetter)
        checkpoint_dir_handler: CheckpointDirHandler = container.get(CheckpointDirHandler)
        serializator: Serializator = container.get(Serializator)
        date_parser: DateParser = container.get(DateParser)
        checkpoint_guard: CheckpointGuard = container.get(CheckpointGuard)
        frequency_guard: FrequencyGuard = container.get(FrequencyGuard)

        if container.get_parameters().featurestorebundle.metadata.display_in_notebook is True:
            metadata_html_displayer: MetadataHTMLDisplayer = container.get(MetadataHTMLDisplayer)
            metadata_html_displayer.display(self.__feature_list.get_metadata_dicts())

        if widgets_getter.timestamp_exists():
            start_date = date_parser.parse_date(self.__start_date)  # pyre-ignore[6]
            timestamp = date_parser.parse_date(widgets_getter.get_timestamp())

            if not frequency_guard.should_be_computed(start_date, timestamp, self.__frequency):  # pyre-ignore[6]
                logger.info(f"Features should not be computed for '{widgets_getter.get_timestamp()}', skipping...")
                return

        if checkpoint_guard.should_checkpoint_result() is True:
            checkpoint_dir_handler.set_checkpoint_dir_if_necessary()
            result = result.checkpoint()

        if widgets_getter.features_orchestration_id_exists():
            orchestration_id = widgets_getter.get_features_orchestration_id()
            serializator.serialize(result, self.__feature_list, orchestration_id)

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
        container: ContainerInterface,
        df: DataFrame,
        features: Iterable[Feature],
    ) -> FeatureList:
        """
        @[foo]_feature(
            Feature("delayed_flights_pct_30d", "% of delayed flights in last 30 days", fillna_with=0),
            FeatureWithChange("early_flights_pct_30d", "% of flights landed ahead of time in last 30 days", fillna_with=0)
        )
        """
        feature_template_matcher: FeatureTemplateMatcher = container.get(FeatureTemplateMatcher)
        date_parser: DateParser = container.get(DateParser)
        notebook_name_getter: NotebookNameGetter = container.get(NotebookNameGetter)
        features_writer: FeaturesWriter = container.get(FeaturesWriter)

        feature_templates = [
            feature_.create_template(
                location=features_writer.get_location(self.__entity.name),
                backend=features_writer.get_backend(),
                notebook=notebook_name_getter.get(),
                category=self.__category,
                owner=self.__owner,
                start_date=date_parser.parse_date(self.__start_date),  # pyre-ignore[6]
                frequency=self.__frequency,
                last_compute_date=date_parser.parse_date(self.__last_compute_date) if self.__last_compute_date is not None else None,
            )
            for feature_ in features
        ]

        return feature_template_matcher.get_features(self.__entity, feature_templates, df)

    def __set_feature_defaults(self, container: ContainerInterface):
        widgets_getter: WidgetsGetter = container.get(WidgetsGetter)
        current_notebook_definition_getter: CurrentNotebookDefinitionGetter = container.get(CurrentNotebookDefinitionGetter)
        notebook_definition = current_notebook_definition_getter.get()

        self.__category = self.__category or container.get_parameters().featurestorebundle.feature.defaults.category
        self.__owner = self.__owner or container.get_parameters().featurestorebundle.feature.defaults.owner
        self.__start_date = self.__start_date or notebook_definition.start_date
        self.__frequency = self.__frequency or notebook_definition.frequency
        self.__last_compute_date = widgets_getter.get_timestamp() if widgets_getter.timestamp_exists() else None
