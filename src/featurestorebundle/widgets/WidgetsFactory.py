from logging import Logger
from box import Box
from daipecore.widgets.Widgets import Widgets

from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.widgets.WidgetNames import WidgetNames
from featurestorebundle.utils.errors import MissingEntitiesError, MissingWidgetDefaultError
from featurestorebundle.target.reader.TargetsReader import TargetsReader
from featurestorebundle.delta.target.schema import get_target_id_column_name


# pylint: disable=too-many-instance-attributes
class WidgetsFactory:
    def __init__(
        self,
        logger: Logger,
        defaults: Box,
        entities: Box,
        stages: Box,
        entity_getter: EntityGetter,
        targets_reader: TargetsReader,
        widget_names: WidgetNames,
        widgets: Widgets,
    ):
        self.__logger = logger
        self.__defaults = defaults
        self.__entities_list = list(entities) if entities is not None else []
        self.__stages = stages
        self.__entity_getter = entity_getter
        self.__targets_reader = targets_reader
        self.__widget_names = widget_names
        self.__widgets = widgets

    def create(self):
        self.__widgets.remove_all()

        self.create_for_entity()

        self.create_target_name()

        if self.__widgets.get_value(self.__widget_names.target_name) == self.__widget_names.no_targets_placeholder:
            self.create_for_timestamp()
        else:
            self.create_for_target()

        self.create_for_sample()

    def create_for_entity(self):
        if not self.__entities_list:
            raise MissingEntitiesError("No entities are defined in config. Please set featurestorebundle.entities in config.yaml")

        if len(self.__entities_list) > 1:
            self.__widgets.add_select(self.__widget_names.entity_name, self.__entities_list, default_value=self.__entities_list[0])

    def create_for_timestamp(self):
        self.__check_default_exists(self.__widget_names.timestamp_name)

        self.__widgets.add_text(self.__widget_names.timestamp_name, self.__defaults.timestamp)

    def create_for_target(self):
        self.__check_default_exists(self.__widget_names.target_date_from_name)
        self.__check_default_exists(self.__widget_names.target_date_to_name)
        self.__check_default_exists(self.__widget_names.target_time_shift)

        self.__widgets.add_text(self.__widget_names.target_date_from_name, self.__defaults.target_date_from)

        self.__widgets.add_text(self.__widget_names.target_date_to_name, self.__defaults.target_date_to)

        self.__widgets.add_text(self.__widget_names.target_time_shift, self.__defaults.target_time_shift)

    def create_target_name(self):
        entity = self.__entity_getter.get()

        if not self.__targets_reader.enum_exists(entity.name):
            self.__logger.warning("Cannot load target store, target based computation will be unavailable")
            targets = []

        else:
            targets = [
                getattr(row, get_target_id_column_name())
                for row in self.__targets_reader.read_enum(entity.name).select(get_target_id_column_name()).collect()
            ]

        self.__widgets.add_select(
            self.__widget_names.target_name,
            [self.__widget_names.no_targets_placeholder] + targets,
            self.__widget_names.no_targets_placeholder,
        )

    def create_for_notebooks(self):
        stages = [self.__widget_names.all_notebooks_placeholder]
        for stage, notebook_definitions in self.__stages.items():
            for notebook_definition in notebook_definitions:
                # for orchestration backwards compatibility
                if isinstance(notebook_definition, str):
                    notebook_name = notebook_definition.split("/")[-1]
                    stages.append(f"{stage}: {notebook_name}")

                if isinstance(notebook_definition, Box):
                    notebook_name = notebook_definition.notebook.split("/")[-1]
                    stages.append(f"{stage}: {notebook_name}")

        self.__widgets.add_multiselect(self.__widget_names.notebooks_name, stages, [self.__widget_names.all_notebooks_placeholder])

    def create_for_sample(self):
        self.__widgets.add_select(
            self.__widget_names.sample_name, [self.__defaults.sample, self.__widget_names.sample_value], self.__defaults.sample
        )

    def __check_default_exists(self, widget_name: str):
        if widget_name not in self.__defaults:
            raise MissingWidgetDefaultError(widget_name)
