from datetime import datetime
from featurestorebundle.frequency.Frequencies import Frequencies
from featurestorebundle.frequency.FrequencyChecker import FrequencyChecker


class FrequencyGuard:
    def __init__(self, frequency_checker: FrequencyChecker):
        self.__frequency_checker = frequency_checker

    def should_be_computed(self, start_date: datetime, current_date: datetime, frequency: str) -> bool:
        self.__frequency_checker.check_frequency_valid(frequency)

        if current_date < start_date:
            return False

        if frequency in Frequencies.friendly_frequencies:
            return self.__friendly_frequency_should_be_computed(start_date, current_date, frequency)

        return self.__other_frequency_should_be_computed(start_date, current_date, frequency)

    def __friendly_frequency_should_be_computed(self, start_date: datetime, current_date: datetime, frequency: str) -> bool:
        if frequency == Frequencies.daily:
            return True

        if frequency == Frequencies.weekly:
            if start_date.weekday() != 0:
                raise Exception("Weekly features can only start on monday")

            return current_date.weekday() == 0

        if frequency == Frequencies.monthly:
            if start_date.day != 1:
                raise Exception("Monthly features can only start on first day of month")

            return current_date.day == 1

        return False

    def __other_frequency_should_be_computed(self, start_date: datetime, current_date: datetime, frequency: str) -> bool:
        frequency_in_days = int(frequency[:-1])

        return (current_date - start_date).days % frequency_in_days == 0
