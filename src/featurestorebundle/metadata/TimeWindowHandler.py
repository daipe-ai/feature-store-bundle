from featurestorebundle.feature.FeaturePattern import FeaturePattern

PERIODS = {
    "h": "hours",
    "d": "days",
    "w": "weeks",
}


class TimeWindowFormatError(Exception):
    pass


class TimeWindowHandler:
    def is_valid(self, time_window: str, feature_name: str, feature_pattern: FeaturePattern):
        time_window_value = time_window[:-1]
        time_window_period = time_window[-1]

        if not time_window_value.isdigit():
            raise TimeWindowFormatError(
                f"Column '{feature_name}' has been matched by '{feature_pattern.feature_template.name_template}' and time_window={time_window_value} which is not a positive integer. Check that your templates adhere to the rules at https://docs.daipe.ai/feature-store/templates/"
            )
        if time_window_period not in PERIODS:
            raise TimeWindowFormatError(
                f"Column '{feature_name}' has been matched by '{feature_pattern.feature_template.name_template}' and time_window={time_window} with period '{time_window_period}' is not from supported periods: {', '.join(PERIODS.keys())}"
            )

    def to_text(self, time_window: str) -> str:
        n = int(time_window[:-1])
        period = time_window[-1]

        result = f"{n} {PERIODS[period]}"
        return result[:-1] if n == 1 else result
