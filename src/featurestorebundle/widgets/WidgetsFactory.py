from box import Box
from daipecore.widgets.Widgets import Widgets

from featurestorebundle.utils.errors import MissingEntitiesError, MissingWidgetDefaultError
from featurestorebundle.target.reader.TargetsReader import TargetsReader
from featurestorebundle.delta.target.schema import get_target_id_column_name


class WidgetsFactory:
    all_notebooks_placeholder = "<all>"
    no_targets_placeholder = "<no target>"
    sample_value = "sample"

    entity_name = "entity_name"
    target_name = "target_name"
    timestamp_name = "timestamp"
    target_date_from_name = "target_date_from"
    target_date_to_name = "target_date_to"
    target_time_shift = "target_time_shift"
    notebooks_name = "notebooks"
    features_orchestration_id = "features_orchestration_id"
    sample_name = "sample_data"

    def __init__(self, defaults: Box, entities: Box, stages: Box, targets_reader: TargetsReader, widgets: Widgets):
        self.__defaults = defaults
        self.__entities_list = list(entities) if entities is not None else []
        self.__stages = stages
        self.__targets_reader = targets_reader
        self.__widgets = widgets

    def create(self):
        self.__widgets.remove_all()

        self.create_for_entity()

        self.create_target_name()

        if self.__widgets.get_value(WidgetsFactory.target_name) == WidgetsFactory.no_targets_placeholder:
            self.create_for_timestamp()
        else:
            self.create_for_target()

        self.create_for_sample()

    def create_for_entity(self):
        if not self.__entities_list:
            raise MissingEntitiesError("No entities are defined in config. Please set featurestorebundle.entities in config.yaml")

        if len(self.__entities_list) > 1:
            self.__widgets.add_select(WidgetsFactory.entity_name, self.__entities_list, default_value=self.__entities_list[0])

    def create_for_timestamp(self):
        self.__check_default_exists(WidgetsFactory.timestamp_name)

        self.__widgets.add_text(WidgetsFactory.timestamp_name, self.__defaults.timestamp)

    def create_for_target(self):
        self.__check_default_exists(WidgetsFactory.target_date_from_name)
        self.__check_default_exists(WidgetsFactory.target_date_to_name)
        self.__check_default_exists(WidgetsFactory.target_time_shift)

        self.__widgets.add_text(WidgetsFactory.target_date_from_name, self.__defaults.target_date_from)

        self.__widgets.add_text(WidgetsFactory.target_date_to_name, self.__defaults.target_date_to)

        self.__widgets.add_text(WidgetsFactory.target_time_shift, self.__defaults.target_time_shift)

    def create_target_name(self):
        targets = [
            getattr(row, get_target_id_column_name())
            for row in self.__targets_reader.read_enum().select(get_target_id_column_name()).collect()
        ]

        self.__widgets.add_select(
            WidgetsFactory.target_name,
            [WidgetsFactory.no_targets_placeholder] + targets,
            WidgetsFactory.no_targets_placeholder,
        )

    def create_for_notebooks(self):
        stages = [WidgetsFactory.all_notebooks_placeholder]
        for stage, notebook_definitions in self.__stages.items():
            for notebook_definition in notebook_definitions:
                # for orchestration backwards compatibility
                if isinstance(notebook_definition, str):
                    notebook_name = notebook_definition.split("/")[-1]
                    stages.append(f"{stage}: {notebook_name}")

                if isinstance(notebook_definition, Box):
                    notebook_name = notebook_definition.notebook.split("/")[-1]
                    stages.append(f"{stage}: {notebook_name}")

        self.__widgets.add_multiselect(WidgetsFactory.notebooks_name, stages, [WidgetsFactory.all_notebooks_placeholder])

    def create_for_sample(self):
        self.__widgets.add_select(WidgetsFactory.sample_name, [self.__defaults.sample, WidgetsFactory.sample_value], self.__defaults.sample)

    def __check_default_exists(self, widget_name: str):
        if widget_name not in self.__defaults:
            raise MissingWidgetDefaultError(widget_name)
