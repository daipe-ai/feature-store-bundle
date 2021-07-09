import unittest
from datetime import datetime

from featurestorebundle.windows.windowed_features import (
    with_windowed_columns_renamed,
    with_windowed_columns,
    drop_windowed_columns,
    get_windowed_column_names,
)
from featurestorebundle.windows.WindowedCol import get_windowed_columns
from featurestorebundle.windows.tests.Mocks import DataFrame_mock, WindowedCol_mock, WindowedColForAgg_mock, fcol, fsum, fwhen


class WindowedFeaturesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.__windows = ["30d", "60d"]
        self.__target_date = datetime(2021, 1, 1)
        data = {
            "cardtr_process_date": [datetime(2021, 12, 1), datetime(2021, 6, 1), datetime(2019, 12, 1)],
            "location": ["CZ", "CZ", "SK"],
            "amount": [100, 200, 301],
            "cardtr_czech_count_30d": [1, 0, 0],
            "cardtr_czech_count_60d": [1, 0, 0],
            "cardtr_czech_count_360d": [1, 1, 0],
        }
        self.df = DataFrame_mock(data)

    def test_one_column_for_agg(self):
        output = get_windowed_columns(
            [
                WindowedColForAgg_mock(
                    "cardtr_czech_count_{window}", fcol("cardtr_location").isin("CZ", "CZE").cast("integer"), agg_fun=fsum
                )
            ],
            self.__windows,
        )
        self.assertListEqual(
            [
                "sum(CAST ((cardtr_location) IN (CZ, CZE)) AS integer) AS `cardtr_czech_count_30d`",
                "sum(CAST ((cardtr_location) IN (CZ, CZE)) AS integer) AS `cardtr_czech_count_60d`",
            ],
            output,
        )

    def test_multiple_columns_for_agg(self):
        output = get_windowed_columns(
            [
                WindowedColForAgg_mock(
                    "cardtr_czech_count_{window}", fcol("cardtr_location").isin("CZ", "CZE").cast("integer"), agg_fun=fsum
                ),
                WindowedColForAgg_mock(
                    "cardtr_czech_volume_{window}",
                    fwhen(fcol("cardtr_location").isin("CZ", "CZE"), fcol("cardtr_amount")).otherwise(None),
                    agg_fun=fsum,
                ),
            ],
            self.__windows,
        )
        self.assertListEqual(
            [
                "sum(CAST ((cardtr_location) IN (CZ, CZE)) AS integer) AS `cardtr_czech_count_30d`",
                "sum(CAST ((cardtr_location) IN (CZ, CZE)) AS integer) AS `cardtr_czech_count_60d`",
                "sum(CASE WHEN (cardtr_location) IN (CZ, CZE) THEN cardtr_amount ELSE NULL) AS `cardtr_czech_volume_30d`",
                "sum(CASE WHEN (cardtr_location) IN (CZ, CZE) THEN cardtr_amount ELSE NULL) AS `cardtr_czech_volume_60d`",
            ],
            output,
        )

    def test_windowed_column_names(self):
        self.assertListEqual(
            ["test_afdsgs30dgds", "test_afdsgs60dgds"], get_windowed_column_names("test_afdsgs{window}gds", self.__windows)
        )
        self.assertListEqual(
            ["30dtest_afdsfdsfdsfgsgds", "60dtest_afdsfdsfdsfgsgds"],
            get_windowed_column_names("{window}test_afdsfdsfdsfgsgds", self.__windows),
        )
        self.assertListEqual(
            ["test_afdsfdsfdsfgsgds30d", "test_afdsfdsfdsfgsgds60d"],
            get_windowed_column_names("test_afdsfdsfdsfgsgds{window}", self.__windows),
        )

    def test_with_column(self):
        df = with_windowed_columns(
            self.df,
            [
                WindowedCol_mock(
                    "cardtr_czech_flag_{window}",
                    fcol("(cardtr_czech_count_{window} > 0).cast(long)"),
                ),
            ],
            self.__windows,
        )
        self.assertIn("cardtr_czech_flag_30d", df.data)
        self.assertIn("cardtr_czech_flag_60d", df.data)

    def test_with_column_renamed(self):
        self.assertIn("cardtr_czech_count_30d", self.df.data)
        self.assertIn("cardtr_czech_count_60d", self.df.data)

        df = with_windowed_columns_renamed(self.df, self.__windows, {"cardtr_czech_count_{window}": "cardtr_czech_new_count_{window}"})
        self.assertNotIn("cardtr_czech_count_30d", df.data)
        self.assertNotIn("cardtr_czech_count_60d", df.data)
        self.assertIn("cardtr_czech_count_360d", df.data)

        self.assertIn("cardtr_czech_new_count_30d", df.data)
        self.assertIn("cardtr_czech_new_count_60d", df.data)

    def test_drop_columns(self):
        df = drop_windowed_columns(self.df, self.__windows, "cardtr_czech_count_{window}")
        self.assertNotIn("cardtr_czech_count_30d", df.data)
        self.assertNotIn("cardtr_czech_count_60d", df.data)
        self.assertIn("cardtr_czech_count_360d", df.data)
