import re
from datetime import datetime


class FrequencyGuard:
    _friendly_frequencies = ["daily", "weekly", "monthly"]

    def should_be_computed(self, start_date: datetime, current_date: datetime, frequency: str) -> bool:
        if current_date < start_date:
            return False

        if frequency in self._friendly_frequencies:
            return self.__friendly_frequency_should_be_computed(start_date, current_date, frequency)

        return self.__other_frequency_should_be_computed(start_date, current_date, frequency)

    def __friendly_frequency_should_be_computed(self, start_date: datetime, current_date: datetime, frequency: str) -> bool:
        if frequency == "daily":
            return True

        if frequency == "weekly":
            if start_date.weekday() != 0:
                raise Exception("Weekly features can only start on monday")

            return current_date.weekday() == 0

        if frequency == "monthly":
            if start_date.day != 1:
                raise Exception("Monthly features can only start on first day of month")

            return current_date.day == 1

        return False

    def __other_frequency_should_be_computed(self, start_date: datetime, current_date: datetime, frequency: str) -> bool:
        matches = re.match(r"([1-9][0-9]*)(d)", frequency)

        if not matches:
            raise Exception(f"Invalid frequency format, allowed values are {self._friendly_frequencies} or e.g. '5d'")

        number_of_days = int(matches[1])

        return (current_date - start_date).days % number_of_days == 0
