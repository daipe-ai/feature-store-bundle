from datetime import datetime as dt
from logging import Logger

from featurestorebundle.utils.errors import DateFormatError


class DateParser:
    date_format = "%Y-%m-%d"
    legacy_date_format = "%Y%m%d"

    def __init__(self, logger: Logger):
        self.__logger = logger

    def parse_date(self, date_str: str) -> dt:
        try:
            result = dt.strptime(date_str, DateParser.date_format)
        except ValueError:
            result = self.__parse_legacy_date(date_str)
        return result

    def __parse_legacy_date(self, date_str: str) -> dt:
        try:
            timestamp = dt.strptime(date_str, DateParser.legacy_date_format)
            self.__logger.warning(
                f"Widget value `{date_str}` is in a deprecated date format please use `{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}` instead"
            )
            return timestamp
        except ValueError as value_error:
            raise DateFormatError(
                f"Widget value `{date_str}` does not match either `{DateParser.date_format}` or `{DateParser.legacy_date_format}` date formats"
            ) from value_error
