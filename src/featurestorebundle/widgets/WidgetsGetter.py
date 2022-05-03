from daipecore.widgets.Widgets import Widgets
from featurestorebundle.widgets.WidgetsFactory import WidgetsFactory


class WidgetsGetter:
    def __init__(self, widgets: Widgets):
        self.__widgets = widgets

    def get_entity(self) -> str:
        return self.__widgets.get_value(WidgetsFactory.entity_name)

    def entity_exists(self) -> bool:
        return self.__widget_exists(WidgetsFactory.entity_name)

    def get_target(self) -> str:
        return self.__widgets.get_value(WidgetsFactory.target_name)

    def target_exists(self) -> bool:
        return self.__widget_exists(WidgetsFactory.target_name)

    def get_timestamp(self) -> str:
        return self.__widgets.get_value(WidgetsFactory.timestamp_name)

    def timestamp_exists(self) -> bool:
        return self.__widget_exists(WidgetsFactory.timestamp_name)

    def get_target_date_from(self) -> str:
        return self.__widgets.get_value(WidgetsFactory.target_date_from_name)

    def target_date_from_exists(self) -> bool:
        return self.__widget_exists(WidgetsFactory.target_date_from_name)

    def get_target_date_to(self) -> str:
        return self.__widgets.get_value(WidgetsFactory.target_date_to_name)

    def target_date_to_exists(self) -> bool:
        return self.__widget_exists(WidgetsFactory.target_date_to_name)

    def get_target_time_shift(self) -> str:
        return self.__widgets.get_value(WidgetsFactory.target_time_shift)

    def target_time_shift_exists(self) -> bool:
        return self.__widget_exists(WidgetsFactory.target_time_shift)

    def get_notebooks(self) -> str:
        return self.__widgets.get_value(WidgetsFactory.notebooks_name)

    def notebooks_exists(self) -> bool:
        return self.__widget_exists(WidgetsFactory.notebooks_name)

    def get_features_orchestration_id(self) -> str:
        return self.__widgets.get_value(WidgetsFactory.features_orchestration_id)

    def features_orchestration_id_exists(self) -> bool:
        return self.__widget_exists(WidgetsFactory.features_orchestration_id)

    def should_sample(self) -> bool:
        return self.__widget_exists(WidgetsFactory.sample_name) and (
            self.__widgets.get_value(WidgetsFactory.sample_name) == WidgetsFactory.sample_value
        )

    def __widget_exists(self, widget_name: str) -> bool:
        try:
            self.__widgets.get_value(widget_name)
            return True

        except Exception:  # noqa pylint: disable=broad-except
            return False
