import datetime as dt

from featurestorebundle.utils.errors import DateFormatError


class DateParser:
    date_format = "%Y-%m-%d"

    def parse_date(self, date_str: str) -> dt.datetime:
        try:
            result = dt.datetime.strptime(date_str, DateParser.date_format)

        except ValueError as value_error:
            raise DateFormatError(f"Widget value `{date_str}` does not match `{DateParser.date_format}`") from value_error

        return result
