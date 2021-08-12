import unittest
import pytest

import pyspark.sql.types as t

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.metadata.MetadataExtractor import MetadataExtractor


class MetadataTest(unittest.TestCase):
    def setUp(self):
        self.__metadata_extractor = MetadataExtractor()
        self.__entity = Entity("test", "test_id", t.IntegerType(), "run_date", t.IntegerType())
        self.__feature_list = FeatureList(
            [
                Feature(
                    "card_tr_location_czech_{agg_fun}_{time_window}",
                    "Card transaction {agg_fun} in czechia in {time_window}",
                    t.IntegerType(),
                    "category",
                ),
                Feature(
                    "card_tr_location_abroad_{agg_fun}_{time_window}",
                    "Card transaction {agg_fun} abroad in {time_window}",
                    t.IntegerType(),
                    "category",
                ),
            ]
        )
        self.__columns = [
            "card_tr_location_czech_count_90d",
            "card_tr_location_czech_count_60d",
            "card_tr_location_abroad_count_90d",
            "card_tr_location_abroad_count_60d",
            "card_tr_location_abroad_volume_90d",
            "card_tr_location_abroad_volume_60d",
            "test_id",
            "run_date",
        ]

    def test_happy(self):
        self.assertListEqual(
            [
                [
                    "card_tr_location_czech_count_90d",
                    "Card transaction count in czechia in 90 days",
                    "category",
                    "card_tr_location_czech_{agg_fun}_{time_window}",
                    {"agg_fun": "count", "time_window": "90d"},
                ],
                [
                    "card_tr_location_czech_count_60d",
                    "Card transaction count in czechia in 60 days",
                    "category",
                    "card_tr_location_czech_{agg_fun}_{time_window}",
                    {"agg_fun": "count", "time_window": "60d"},
                ],
                [
                    "card_tr_location_abroad_count_90d",
                    "Card transaction count abroad in 90 days",
                    "category",
                    "card_tr_location_abroad_{agg_fun}_{time_window}",
                    {"agg_fun": "count", "time_window": "90d"},
                ],
                [
                    "card_tr_location_abroad_count_60d",
                    "Card transaction count abroad in 60 days",
                    "category",
                    "card_tr_location_abroad_{agg_fun}_{time_window}",
                    {"agg_fun": "count", "time_window": "60d"},
                ],
                [
                    "card_tr_location_abroad_volume_90d",
                    "Card transaction volume abroad in 90 days",
                    "category",
                    "card_tr_location_abroad_{agg_fun}_{time_window}",
                    {"agg_fun": "volume", "time_window": "90d"},
                ],
                [
                    "card_tr_location_abroad_volume_60d",
                    "Card transaction volume abroad in 60 days",
                    "category",
                    "card_tr_location_abroad_{agg_fun}_{time_window}",
                    {"agg_fun": "volume", "time_window": "60d"},
                ],
            ],
            self.__metadata_extractor.get_metadata(self.__entity, self.__feature_list, self.__columns),
        )

    def test_different_order(self):
        feature_list = FeatureList(
            [
                Feature(
                    "card_tr{time_window}_location_{agg_fun}czech",
                    "Card transaction {agg_fun} in czechia in {time_window}",
                    t.IntegerType(),
                    "category",
                ),
                Feature(
                    "{time_window}card_tr_location_abroad_{agg_fun}",
                    "Card transaction {agg_fun} abroad in {time_window}",
                    t.IntegerType(),
                    "category",
                ),
            ]
        )
        columns = [
            "card_tr90d_location_countczech",
            "card_tr60d_location_countczech",
            "90hcard_tr_location_abroad_count",
            "60wcard_tr_location_abroad_count",
            "1dcard_tr_location_abroad_volume",
            "test_id",
            "run_date",
        ]

        self.assertListEqual(
            [
                [
                    "card_tr90d_location_countczech",
                    "Card transaction count in czechia in 90 days",
                    "category",
                    "card_tr{time_window}_location_{agg_fun}czech",
                    {"agg_fun": "count", "time_window": "90d"},
                ],
                [
                    "card_tr60d_location_countczech",
                    "Card transaction count in czechia in 60 days",
                    "category",
                    "card_tr{time_window}_location_{agg_fun}czech",
                    {"agg_fun": "count", "time_window": "60d"},
                ],
                [
                    "90hcard_tr_location_abroad_count",
                    "Card transaction count abroad in 90 hours",
                    "category",
                    "{time_window}card_tr_location_abroad_{agg_fun}",
                    {"agg_fun": "count", "time_window": "90h"},
                ],
                [
                    "60wcard_tr_location_abroad_count",
                    "Card transaction count abroad in 60 weeks",
                    "category",
                    "{time_window}card_tr_location_abroad_{agg_fun}",
                    {"agg_fun": "count", "time_window": "60w"},
                ],
                [
                    "1dcard_tr_location_abroad_volume",
                    "Card transaction volume abroad in 1 day",
                    "category",
                    "{time_window}card_tr_location_abroad_{agg_fun}",
                    {"agg_fun": "volume", "time_window": "1d"},
                ],
            ],
            self.__metadata_extractor.get_metadata(self.__entity, feature_list, columns),
        )

    def test_bad_column_name(self):
        with pytest.raises(Exception) as e:
            self.__metadata_extractor.get_metadata(self.__entity, self.__feature_list, self.__columns + ["UnmatchableColumnName60d"])
            self.assertEqual("Column 'UnmatchableColumnName60d' could not be matched by any template.", str(e))

    def test_bad_feature_name(self):
        feature_list = FeatureList(
            [
                Feature(
                    "card_tr{time_window}_location_{agg_fun}czech",
                    "Card transaction {agg_fun} in czechia in {time_window}",
                    t.IntegerType(),
                    "category",
                ),
            ]
        )
        with pytest.raises(Exception) as e:
            self.__metadata_extractor.get_metadata(self.__entity, feature_list, self.__columns)
            self.assertEqual("Column 'card_tr_location_czech_count_90d' could not be matched by any template.", str(e))

    def test_placeholder_in_description_missing(self):
        columns = ["card_tr_location_volume_czech_90d"]
        feature_list = FeatureList(
            [
                Feature(
                    "card_tr_location_{agg_fun}_czech_{time_window}",
                    "Card transaction {agg_fun} in czechia",
                    t.IntegerType(),
                    "category",
                ),
            ]
        )

        self.assertListEqual(
            [
                [
                    "card_tr_location_volume_czech_90d",
                    "Card transaction volume in czechia",
                    "category",
                    "card_tr_location_{agg_fun}_czech_{time_window}",
                    {"agg_fun": "volume", "time_window": "90d"},
                ],
            ],
            self.__metadata_extractor.get_metadata(self.__entity, feature_list, columns),
        )

    def test_placeholder_missing(self):
        columns = ["card_tr_location_volume_czech", "card_tr_location_flag_czech", "apple_user"]
        feature_list = FeatureList(
            [
                Feature(
                    "card_tr_location_{agg_fun}_czech",
                    "Card transaction {agg_fun} in czechia",
                    t.IntegerType(),
                    "category",
                ),
                Feature(
                    "apple_user",
                    "Is Apple user",
                    t.BooleanType(),
                    "category",
                ),
            ]
        )

        self.assertListEqual(
            [
                [
                    "card_tr_location_volume_czech",
                    "Card transaction volume in czechia",
                    "category",
                    "card_tr_location_{agg_fun}_czech",
                    {"agg_fun": "volume"},
                ],
                [
                    "card_tr_location_flag_czech",
                    "Card transaction flag in czechia",
                    "category",
                    "card_tr_location_{agg_fun}_czech",
                    {"agg_fun": "flag"},
                ],
                [
                    "apple_user",
                    "Is Apple user",
                    "category",
                    "apple_user",
                    {},
                ],
            ],
            self.__metadata_extractor.get_metadata(self.__entity, feature_list, columns),
        )

    def test_bad_time_window_number(self):
        feature_list = FeatureList(
            [
                Feature(
                    "card_tr_location_czech_{agg_fun}_{time_window}",
                    "Card transaction {agg_fun} in czechia in {time_window}",
                    t.IntegerType(),
                    "category",
                ),
            ]
        )
        columns = [
            "card_tr_location_czech_volume_90.1d",
            "test_id",
            "run_date",
        ]
        with pytest.raises(Exception) as e:
            self.__metadata_extractor.get_metadata(self.__entity, feature_list, columns)
            self.assertEqual("In feature 'card_tr_location_czech_volume_90.1d', time_window=90.1 is not a positive integer.", str(e))

    def test_bad_time_window_period(self):
        feature_list = FeatureList(
            [
                Feature(
                    "card_tr_location_czech_{agg_fun}_{time_window}",
                    "Card transaction {agg_fun} in czechia in {time_window}",
                    t.IntegerType(),
                    "category",
                ),
            ]
        )
        columns = [
            "card_tr_location_czech_volume_9k",
            "test_id",
            "run_date",
        ]
        with pytest.raises(Exception) as e:
            self.__metadata_extractor.get_metadata(self.__entity, feature_list, columns)
            self.assertEqual("In feature 'card_tr_location_czech_volume_9k', time_window period 'k' is not from h, d, w", str(e))

    def test_real_world(self):
        feature_list = FeatureList(
            [
                Feature(a, "test", t.IntegerType(), "test_cat")
                for a in [
                    "card_tr_location_{location}_{agg_fun}_{time_window}",
                    "card_tr_location_{location}_{agg_fun}_{time_window}",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                ]
            ]
        )
        columns = [
            "card_tr_location_czech_atm_volume_90d",
            "card_tr_location_czech_atm_volume_60d",
            "card_tr_location_czech_atm_volume_30d",
            "card_tr_location_abroad_atm_volume_90d",
            "card_tr_location_abroad_atm_volume_60d",
            "card_tr_location_abroad_atm_volume_30d",
            "card_tr_location_czech_pos_volume_90d",
            "card_tr_location_czech_pos_volume_60d",
            "card_tr_location_czech_pos_volume_30d",
            "card_tr_location_abroad_pos_volume_90d",
            "card_tr_location_abroad_pos_volume_60d",
            "card_tr_location_abroad_pos_volume_30d",
            "card_tr_location_czech_atm_count_90d",
            "card_tr_location_czech_atm_count_60d",
            "card_tr_location_czech_atm_count_30d",
            "card_tr_location_abroad_atm_count_90d",
            "card_tr_location_abroad_atm_count_60d",
            "card_tr_location_abroad_atm_count_30d",
            "card_tr_location_czech_pos_count_90d",
            "card_tr_location_czech_pos_count_60d",
            "card_tr_location_czech_pos_count_30d",
            "card_tr_location_abroad_pos_count_90d",
            "card_tr_location_abroad_pos_count_60d",
            "card_tr_location_abroad_pos_count_30d",
            "run_date",
            "test_id",
        ]
        self.assertListEqual(
            [
                [
                    "card_tr_location_czech_atm_volume_90d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "czech", "point": "atm", "agg_fun": "volume", "time_window": "90d"},
                ],
                [
                    "card_tr_location_czech_atm_volume_60d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "czech", "point": "atm", "agg_fun": "volume", "time_window": "60d"},
                ],
                [
                    "card_tr_location_czech_atm_volume_30d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "czech", "point": "atm", "agg_fun": "volume", "time_window": "30d"},
                ],
                [
                    "card_tr_location_abroad_atm_volume_90d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "abroad", "point": "atm", "agg_fun": "volume", "time_window": "90d"},
                ],
                [
                    "card_tr_location_abroad_atm_volume_60d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "abroad", "point": "atm", "agg_fun": "volume", "time_window": "60d"},
                ],
                [
                    "card_tr_location_abroad_atm_volume_30d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "abroad", "point": "atm", "agg_fun": "volume", "time_window": "30d"},
                ],
                [
                    "card_tr_location_czech_pos_volume_90d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "czech", "point": "pos", "agg_fun": "volume", "time_window": "90d"},
                ],
                [
                    "card_tr_location_czech_pos_volume_60d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "czech", "point": "pos", "agg_fun": "volume", "time_window": "60d"},
                ],
                [
                    "card_tr_location_czech_pos_volume_30d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "czech", "point": "pos", "agg_fun": "volume", "time_window": "30d"},
                ],
                [
                    "card_tr_location_abroad_pos_volume_90d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "abroad", "point": "pos", "agg_fun": "volume", "time_window": "90d"},
                ],
                [
                    "card_tr_location_abroad_pos_volume_60d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "abroad", "point": "pos", "agg_fun": "volume", "time_window": "60d"},
                ],
                [
                    "card_tr_location_abroad_pos_volume_30d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "abroad", "point": "pos", "agg_fun": "volume", "time_window": "30d"},
                ],
                [
                    "card_tr_location_czech_atm_count_90d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "czech", "point": "atm", "agg_fun": "count", "time_window": "90d"},
                ],
                [
                    "card_tr_location_czech_atm_count_60d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "czech", "point": "atm", "agg_fun": "count", "time_window": "60d"},
                ],
                [
                    "card_tr_location_czech_atm_count_30d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "czech", "point": "atm", "agg_fun": "count", "time_window": "30d"},
                ],
                [
                    "card_tr_location_abroad_atm_count_90d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "abroad", "point": "atm", "agg_fun": "count", "time_window": "90d"},
                ],
                [
                    "card_tr_location_abroad_atm_count_60d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "abroad", "point": "atm", "agg_fun": "count", "time_window": "60d"},
                ],
                [
                    "card_tr_location_abroad_atm_count_30d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "abroad", "point": "atm", "agg_fun": "count", "time_window": "30d"},
                ],
                [
                    "card_tr_location_czech_pos_count_90d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "czech", "point": "pos", "agg_fun": "count", "time_window": "90d"},
                ],
                [
                    "card_tr_location_czech_pos_count_60d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "czech", "point": "pos", "agg_fun": "count", "time_window": "60d"},
                ],
                [
                    "card_tr_location_czech_pos_count_30d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "czech", "point": "pos", "agg_fun": "count", "time_window": "30d"},
                ],
                [
                    "card_tr_location_abroad_pos_count_90d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "abroad", "point": "pos", "agg_fun": "count", "time_window": "90d"},
                ],
                [
                    "card_tr_location_abroad_pos_count_60d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "abroad", "point": "pos", "agg_fun": "count", "time_window": "60d"},
                ],
                [
                    "card_tr_location_abroad_pos_count_30d",
                    "test",
                    "test_cat",
                    "card_tr_location_{location}_{point}_{agg_fun}_{time_window}",
                    {"location": "abroad", "point": "pos", "agg_fun": "count", "time_window": "30d"},
                ],
            ],
            self.__metadata_extractor.get_metadata(self.__entity, feature_list, columns),
        )
