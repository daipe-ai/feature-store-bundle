from featurestorebundle.notebook.functions.time_windows import PERIOD_NAMES


class DescriptionFiller:
    def format(self, key: str, value: str) -> str:
        if key == "time_window":
            return self.__format_time_window(value)

        return value.replace("_", " ")

    def __format_time_window(self, time_window: str):
        if "change" in time_window:
            _, low, high = time_window.split("_")
            return f"{low[:-1]} vs {high[:-1]} {PERIOD_NAMES[high[-1]].lower()} ratio"

        n = int(time_window[:-1])
        period = time_window[-1]

        result = f"{n} {PERIOD_NAMES[period].lower()}"
        return result[:-1] if n == 1 else result
