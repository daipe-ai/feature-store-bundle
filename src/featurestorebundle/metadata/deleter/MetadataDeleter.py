from typing import List
from featurestorebundle.metadata.deleter.MetadataDeleterInterface import MetadataDeleterInterface


class MetadataDeleter:
    def __init__(self, metadata_deleter: MetadataDeleterInterface):
        self.__metadata_deleter = metadata_deleter

    def delete(self, features: List[str]):
        self.__metadata_deleter.delete(features)
