from typing import List
from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList


class FeaturesStorage:
    __entity: Entity
    __results: List[DataFrame]
    __feature_list: FeatureList

    def __init__(self, entity: Entity):
        self.__entity = entity
        self.__results = []
        self.__feature_list = FeatureList([])

    @property
    def entity(self):
        return self.__entity

    @property
    def results(self):
        return self.__results

    @property
    def feature_list(self):
        return self.__feature_list

    def add(self, result: DataFrame, feature_list: FeatureList):
        self.__results.append(result)
        self.__feature_list = self.__feature_list.merge(feature_list)
