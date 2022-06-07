from typing import List
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureStoreStorageInfo import FeatureStoreStorageInfo
from featurestorebundle.feature.reader.FeaturesReaderFactory import FeaturesReaderFactory


class FeaturesReader:
    def __init__(self, features_reader_factory: FeaturesReaderFactory):
        self.__features_reader_factory = features_reader_factory

    def read(self, feature_list: FeatureList) -> List[FeatureStoreStorageInfo]:
        locations_backends = {(feature.template.location, feature.template.backend) for feature in feature_list.get_all()}
        feature_store_info_list = []

        for location, backend in locations_backends:
            features_reader = self.__features_reader_factory.create(backend)
            feature_store = features_reader.read(location)
            feature_store_info_list.append(FeatureStoreStorageInfo(feature_store, location, backend))

        return feature_store_info_list
