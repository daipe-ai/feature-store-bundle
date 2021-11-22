import unittest
from datetime import datetime
import time
from featurestorebundle.windows.windowed_features import _is_past_time_window


def get_unix_timestamp(date_str: str):
    return time.mktime(datetime.strptime(date_str, "%Y-%m-%d").timetuple())


class WindowedFeaturesTest(unittest.TestCase):
    def test_past_time_windows(self):
        run_date = get_unix_timestamp("2021-11-18")
        arg_date = get_unix_timestamp("2021-10-17")

        self.assertFalse(_is_past_time_window(run_date, arg_date, "30d"))
        self.assertTrue(_is_past_time_window(run_date, arg_date, "60d"))
        self.assertTrue(_is_past_time_window(run_date, arg_date, "90d"))

    def test_wrong_order_time_windows(self):
        arg_date = get_unix_timestamp("2021-11-18")
        run_date = get_unix_timestamp("2021-10-17")

        self.assertFalse(_is_past_time_window(run_date, arg_date, "30d"))
        self.assertFalse(_is_past_time_window(run_date, arg_date, "60d"))
        self.assertFalse(_is_past_time_window(run_date, arg_date, "90d"))


if __name__ == "__main__":
    unittest.main()
