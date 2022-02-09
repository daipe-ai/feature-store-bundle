from featurestorebundle.feature.writer.FeaturesWriterInterface import FeaturesWriterInterface
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage


class FeaturesWriter:
    def __init__(self, features_writer: FeaturesWriterInterface):
        self.__features_writer = features_writer

    def write(self, features_storage: FeaturesStorage):
        self.__features_writer.write(features_storage)
