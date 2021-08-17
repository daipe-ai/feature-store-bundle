PERIODS = {
    "h": "hours",
    "d": "days",
    "w": "weeks",
}


class TimeWindowHandler:
    def is_valid(self, time_window: str, feature_name: str):
        if not time_window[:-1].isdigit():
            raise Exception(f"In feature '{feature_name}', time_window={time_window[:-1]} is not a positive integer.")
        elif not time_window[-1] in PERIODS.keys():
            raise Exception(f"In feature '{feature_name}', time_window period '{time_window[-1]}' is not from {', '.join(PERIODS.keys())}")

    def to_text(self, time_window: str) -> str:
        n = int(time_window[:-1])
        period = time_window[-1]

        result = f"{n} {PERIODS[period]}"
        return result[:-1] if n == 1 else result
