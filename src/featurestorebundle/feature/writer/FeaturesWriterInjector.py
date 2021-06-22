from featurestorebundle.feature.writer.FeaturesWriterInterface import FeaturesWriterInterface


class FeaturesWriterInjector:
    def __init__(self, features_writer: FeaturesWriterInterface):
        self.__features_writer = features_writer

    def get(self):
        return self.__features_writer
