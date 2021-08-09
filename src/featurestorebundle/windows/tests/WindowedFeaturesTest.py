import unittest
from datetime import datetime

from featurestorebundle.windows.windowed_features import (
    get_windowed_column_names,
    get_windowed_columns_to_drop,
    get_windowed_mapping_for_renaming,
    get_aggregations,
)
from featurestorebundle.windows.tests.Mocks import DataFrameMock, WindowedColMock, fcol, fsum, fwhen, flit


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
        self.df = DataFrameMock(data)
        self.__is_windows = {
            window: fcol("window_col").cast("long") >= (flit(self.__target_date).cast("long") - f"{window[:-1]} * 24 * 60 * 60")
            for window in self.__windows
        }

    def test_one_column_for_agg(self):
        output = get_aggregations(
            [WindowedColMock("cardtr_czech_count_{agg_fun}_{window}", fcol("cardtr_location").isin("CZ", "CZE").cast("integer"), fsum)],
            self.__windows,
            self.__is_windows,
        )
        self.assertListEqual(
            [
                "Column<'sum((CASE WHEN (CAST (window_col) AS long) >= (CAST (2021-01-01 00:00:00) AS long) - 30 * 24 * 60 * 60)) THEN CAST ((cardtr_location) IN (CZ, CZE)) AS integer ELSE NULL)) AS `cardtr_czech_count_fsum_30d`'>",
                "Column<'sum((CASE WHEN (CAST (window_col) AS long) >= (CAST (2021-01-01 00:00:00) AS long) - 60 * 24 * 60 * 60)) THEN CAST ((cardtr_location) IN (CZ, CZE)) AS integer ELSE NULL)) AS `cardtr_czech_count_fsum_60d`'>",
            ],
            list(map(str, output)),
        )

    def test_multiple_columns_for_agg(self):
        output = get_aggregations(
            [
                WindowedColMock("cardtr_czech_count_{agg_fun}_{window}", fcol("cardtr_location").isin("CZ", "CZE").cast("integer"), fsum),
                WindowedColMock(
                    "cardtr_czech_{agg_fun}_volume_{window}",
                    fwhen(fcol("cardtr_location").isin("CZ", "CZE"), fcol("cardtr_amount")).otherwise(None),
                    fsum,
                ),
            ],
            self.__windows,
            self.__is_windows,
        )
        self.assertListEqual(
            [
                "Column<'sum((CASE WHEN (CAST (window_col) AS long) >= (CAST (2021-01-01 00:00:00) AS long) - 30 * 24 * 60 * 60)) THEN CAST ((cardtr_location) IN (CZ, CZE)) AS integer ELSE NULL)) AS `cardtr_czech_count_fsum_30d`'>",
                "Column<'sum((CASE WHEN (CAST (window_col) AS long) >= (CAST (2021-01-01 00:00:00) AS long) - 60 * 24 * 60 * 60)) THEN CAST ((cardtr_location) IN (CZ, CZE)) AS integer ELSE NULL)) AS `cardtr_czech_count_fsum_60d`'>",
                "Column<'sum((CASE WHEN (CAST (window_col) AS long) >= (CAST (2021-01-01 00:00:00) AS long) - 30 * 24 * 60 * 60)) THEN (CASE WHEN (cardtr_location) IN (CZ, CZE) THEN cardtr_amount ELSE NULL) ELSE NULL)) AS `cardtr_czech_fsum_volume_30d`'>",
                "Column<'sum((CASE WHEN (CAST (window_col) AS long) >= (CAST (2021-01-01 00:00:00) AS long) - 60 * 24 * 60 * 60)) THEN (CASE WHEN (cardtr_location) IN (CZ, CZE) THEN cardtr_amount ELSE NULL) ELSE NULL)) AS `cardtr_czech_fsum_volume_60d`'>",
            ],
            list(map(str, output)),
        )

    def test_windowed_column_names(self):
        self.assertListEqual(
            ["test_afdsgs30dgds", "test_afdsgs60dgds"], get_windowed_column_names(self.__windows, "test_afdsgs{window}gds")
        )
        self.assertListEqual(
            ["30dtest_afdsfdsfdsfgsgds", "60dtest_afdsfdsfdsfgsgds"],
            get_windowed_column_names(self.__windows, "{window}test_afdsfdsfdsfgsgds"),
        )
        self.assertListEqual(
            ["test_afdsfdsfdsfgsgds30d", "test_afdsfdsfdsfgsgds60d"],
            get_windowed_column_names(self.__windows, "test_afdsfdsfdsfgsgds{window}"),
        )

    def test_with_column(self):
        df = self.df
        for window in self.__windows:
            df = df.withColumn(f"cardtr_czech_flag_{window}", fcol("cardtr_location").isin("CZ", "CZE").cast("integer")).withColumn(
                f"cardtr_abroad_flag_{window}", fcol("cardtr_location").isin("SK").cast("integer")
            )

        self.assertIn("cardtr_czech_flag_30d", df.data)
        self.assertIn("cardtr_czech_flag_60d", df.data)
        self.assertIn("cardtr_abroad_flag_30d", df.data)
        self.assertIn("cardtr_abroad_flag_60d", df.data)

    def test_with_column_renamed(self):
        self.assertIn("cardtr_czech_count_30d", self.df.data)
        self.assertIn("cardtr_czech_count_60d", self.df.data)

        mapping = get_windowed_mapping_for_renaming(self.__windows, {"cardtr_czech_count_{window}": "cardtr_czech_new_count_{window}"})
        self.assertIn("cardtr_czech_count_30d", mapping.keys())
        self.assertIn("cardtr_czech_count_60d", mapping.keys())
        self.assertNotIn("cardtr_czech_count_360d", mapping.keys())

        self.assertIn("cardtr_czech_new_count_30d", mapping.values())
        self.assertIn("cardtr_czech_new_count_60d", mapping.values())

    def test_drop_columns(self):
        cols_to_drop = get_windowed_columns_to_drop(self.__windows, "cardtr_czech_count_{window}")
        df = self.df.drop(*cols_to_drop)
        self.assertNotIn("cardtr_czech_count_30d", df.data)
        self.assertNotIn("cardtr_czech_count_60d", df.data)
        self.assertIn("cardtr_czech_count_360d", df.data)
