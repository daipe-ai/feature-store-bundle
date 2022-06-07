from featurestorebundle.metadata.writer.MetadataWriterInterface import MetadataWriterInterface
from featurestorebundle.feature.FeatureList import FeatureList


class MetadataWriter:
    def __init__(self, metadata_writer: MetadataWriterInterface):
        self.__metadata_writer = metadata_writer

    def write(self, feature_list: FeatureList):
        self.__metadata_writer.write(feature_list)
