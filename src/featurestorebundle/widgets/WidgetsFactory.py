from typing import Optional

from box import Box
from daipecore.widgets.Widgets import Widgets


class WidgetsFactory:
    def __init__(self, defaults: Box, entities: Box, widgets: Widgets):
        self.__defaults = defaults
        self.__entities = entities
        self.__widgets = widgets

    def create(self, default_time_window: Optional[str] = None):
        default_time_window = self.__defaults.time_windows if default_time_window is None else default_time_window

        self.__widgets.remove_all()

        self.create_for_entity()

        self.create_target_name()

        if self.__widgets.get_value("target_name") == "<no target>":
            self.create_for_timestamp(default_time_window)
        else:
            self.create_for_target(default_time_window)

    def create_for_entity(self):
        entities_list = list(self.__entities)

        if len(entities_list) > 1:
            self.__widgets.add_select("entity", entities_list, default_value=entities_list[0])

    def create_for_timestamp(self, default_time_window: Optional[str] = None):
        default_time_window = self.__defaults.time_windows if default_time_window is None else default_time_window

        self.__widgets.add_text("timestamp", self.__defaults.timestamp)

        self.create_time_window(default_time_window)

    def create_for_target(self, default_time_window: Optional[str] = None):
        default_time_window = self.__defaults.time_windows if default_time_window is None else default_time_window

        self.create_time_window(default_time_window)

        self.__widgets.add_text("target_date_from", self.__defaults.target_date_from)

        self.__widgets.add_text("target_date_to", self.__defaults.target_date_to)

        self.__widgets.add_text("number_of_time_units", self.__defaults.number_of_time_units)

    def create_target_name(self):
        self.__widgets.add_select(
            "target_name",
            [
                "<no target>",
                "Date of report of change of female name",
                "First mortgage payment",
                "First loan payment",
                "Date of opening the investment account",
                "Last payment date for the veterinarian",
                "First payment date of unemployment benefits",
                "First payment date of maternity leave",
                "First payment date of alimony",
                "First payment date of paternity leave",
            ],
            "<no target>",
        )

    def create_time_window(self, default_time_window: str):
        self.__widgets.add_text("time_window", default_time_window)
