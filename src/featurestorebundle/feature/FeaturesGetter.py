from logging import Logger
from typing import List, Optional
from datetime import timedelta

from featurestorebundle.utils.DateParser import DateParser
from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.feature.FeatureStore import FeatureStore
from featurestorebundle.widgets.WidgetsGetter import WidgetsGetter
from featurestorebundle.notebook.functions.time_windows import parse_time_window


class FeaturesGetter:
    def __init__(
        self,
        timestamp_shift: str,
        logger: Logger,
        date_parser: DateParser,
        entity_getter: EntityGetter,
        feature_store: FeatureStore,
        widgets_getter: WidgetsGetter,
    ):
        self.__timestamp_shift = parse_time_window(timestamp_shift)
        self.__logger = logger
        self.__date_parser = date_parser
        self.__entity_getter = entity_getter
        self.__feature_store = feature_store
        self.__widgets_getter = widgets_getter

    def get_features(self, feature_names: Optional[List[str]] = None):
        feature_names = [] if feature_names is None else feature_names
        target_name = self.__widgets_getter.get_target()

        return (
            self.__get_latest_features(feature_names)
            if target_name == self.__widgets_getter.timestamp_exists()
            else self.__get_features_for_target(feature_names)
        )

    def __get_latest_features(self, feature_names: List[str]):
        entity = self.__entity_getter.get()
        timestamp = self.__date_parser.parse_date(self.__widgets_getter.get_timestamp()) + timedelta(**self.__timestamp_shift)

        self.__logger.info(f"Loading latest features for entity '{entity.name}'")

        return self.__feature_store.get_latest(entity.name, features=feature_names, timestamp=timestamp, skip_incomplete_rows=True)

    def __get_features_for_target(self, feature_names: List[str]):
        entity = self.__entity_getter.get()
        target_name = self.__widgets_getter.get_target()
        target_date_from = self.__date_parser.parse_date(self.__widgets_getter.get_target_date_from())
        target_date_to = self.__date_parser.parse_date(self.__widgets_getter.get_target_date_to())
        target_time_shift = self.__widgets_getter.get_target_time_shift()

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
