from box import Box
from daipecore.widgets.Widgets import Widgets

from featurestorebundle.target.reader.TargetsReaderInterface import TargetsReaderInterface
from featurestorebundle.delta.target.schema import get_target_id_column_name


class WidgetsFactory:
    all_notebooks_placeholder = "<all>"
    no_targets_placeholder = "<no target>"

    entity_name = "entity_name"
    target_name = "target_name"
    timestamp_name = "timestamp"
    target_date_from_name = "target_date_from"
    target_date_to_name = "target_date_to"
    target_time_shift = "target_time_shift"
    notebooks_name = "notebooks"

    def __init__(self, defaults: Box, entities: Box, stages: Box, targets_reader: TargetsReaderInterface, widgets: Widgets):
        self.__defaults = defaults
        self.__entities = entities
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

    def create_for_entity(self):
        entities_list = list(self.__entities)

        if len(entities_list) > 1:
            self.__widgets.add_select(WidgetsFactory.entity_name, entities_list, default_value=entities_list[0])

    def create_for_timestamp(self):
        self.__widgets.add_text(WidgetsFactory.timestamp_name, self.__defaults.timestamp)

    def create_for_target(self):
        self.__widgets.add_text(WidgetsFactory.target_date_from_name, self.__defaults.target_date_from)

        self.__widgets.add_text(WidgetsFactory.target_date_to_name, self.__defaults.target_date_to)

        self.__widgets.add_text(WidgetsFactory.target_date_from_name, self.__defaults.number_of_time_units)

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
        for stage, notebooks in self.__stages.items():
            stages.extend([f"{stage}: {notebook}" for notebook in notebooks])
        self.__widgets.add_multiselect(WidgetsFactory.notebooks_name, stages, [WidgetsFactory.all_notebooks_placeholder])
