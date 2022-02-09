PERIODS = {
    "h": "hours",
    "d": "days",
    "w": "weeks",
}


class DescriptionFiller:
    def format(self, key: str, value: str) -> str:
        if key == "time_window":
            return self.__format_time_window(value)

        return value.replace("_", " ")

    def __format_time_window(self, time_window: str):
        if "change" in time_window:
            _, low, high = time_window.split("_")
            return f"{low[:-1]} vs {high[:-1]} {PERIODS[high[-1]]} ratio"

        n = int(time_window[:-1])
        period = time_window[-1]

        result = f"{n} {PERIODS[period]}"
        return result[:-1] if n == 1 else result
