from typing import List
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface


class FeaturesReaderFactory:
    def __init__(self, feature_readers: List[FeaturesReaderInterface]):
        self.__feature_readers = feature_readers

    def create(self, backend: str) -> FeaturesReaderInterface:
        for feature_reader in self.__feature_readers:
            if feature_reader.get_backend() == backend:
                return feature_reader

        raise Exception(f"Cannot find feature reader with backend {backend}")
