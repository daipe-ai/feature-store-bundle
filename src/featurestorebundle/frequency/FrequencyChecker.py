import re
from featurestorebundle.frequency.Frequencies import Frequencies


class FrequencyChecker:
    def check_frequency_valid(self, frequency: str):
        if frequency in Frequencies.friendly_frequencies:
            return

        matches = re.match(r"([1-9][0-9]*)(d)", frequency)

        if not matches:
            raise Exception(f"Invalid frequency format, allowed values are {Frequencies.friendly_frequencies} or e.g. '5d'")
