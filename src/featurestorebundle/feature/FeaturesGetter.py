from logging import Logger
from typing import List, Optional

from daipecore.widgets.Widgets import Widgets

from featurestorebundle.utils.DateParser import DateParser
from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.feature.FeatureStore import FeatureStore
from featurestorebundle.widgets.WidgetNames import WidgetNames


class FeaturesGetter:
    def __init__(
        self,
        logger: Logger,
        date_parser: DateParser,
        entity_getter: EntityGetter,
        feature_store: FeatureStore,
        widget_names: WidgetNames,
        widgets: Widgets,
    ):
        self.__logger = logger
        self.__date_parser = date_parser
        self.__entity_getter = entity_getter
        self.__feature_store = feature_store
        self.__widget_names = widget_names
        self.__widgets = widgets

    def get_features(self, feature_names: Optional[List[str]] = None):
        feature_names = [] if feature_names is None else feature_names
        target_name = self.__widgets.get_value(self.__widget_names.target_name)

        return (
            self.__get_latest_features(feature_names)
            if target_name == self.__widget_names.no_targets_placeholder
            else self.__get_features_for_target(feature_names)
        )

    def __get_latest_features(self, feature_names: List[str]):
        entity = self.__entity_getter.get()

        self.__logger.info(f"Loading latest features for entity '{entity.name}'")

        return self.__feature_store.get_latest(entity.name, features=feature_names, skip_incomplete_rows=True)

    def __get_features_for_target(self, feature_names: List[str]):
        entity = self.__entity_getter.get()
        target_name = self.__widgets.get_value(self.__widget_names.target_name)
        target_date_from = self.__date_parser.parse_date(self.__widgets.get_value(self.__widget_names.target_date_from_name))
        target_date_to = self.__date_parser.parse_date(self.__widgets.get_value(self.__widget_names.target_date_to_name))
        target_time_shift = self.__widgets.get_value(self.__widget_names.target_time_shift)

        self.__logger.info(
            f"Getting entity '{entity.name}' features for target '{target_name}', from '{target_date_from}' to '{target_date_to}' "
            f"with a shift of '{target_time_shift}'"
        )

        return self.__feature_store.get_for_target(
            entity_name=entity.name,
            target_name=target_name,
            target_date_from=target_date_from,
            target_date_to=target_date_to,
            time_diff=target_time_shift,
            features=feature_names,
            skip_incomplete_rows=True,
        )
