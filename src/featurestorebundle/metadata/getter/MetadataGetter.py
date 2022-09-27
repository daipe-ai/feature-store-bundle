from typing import List, Optional
from pyspark.sql import DataFrame

from featurestorebundle.metadata.reader.MetadataTableReader import MetadataTableReader
from featurestorebundle.metadata.filter.MetadataFilterer import MetadataFilterer


class MetadataGetter:
    def __init__(self, metadata_reader: MetadataTableReader, metadata_filterer: MetadataFilterer):
        self.__metadata_reader = metadata_reader
        self.__metadata_filterer = metadata_filterer

    def get_metadata(
        self,
        entity_name: str,
        features: Optional[List[str]],
        templates: Optional[List[str]],
        categories: Optional[List[str]],
        time_windows: Optional[List[str]],
        include_tags: Optional[List[str]],
        exclude_tags: Optional[List[str]],
    ) -> DataFrame:
        metadata = self.__metadata_reader.read(entity_name)

        return self.__metadata_filterer.filter(
            metadata,
            entity_name,
            features,
            templates,
            categories,
            time_windows,
            include_tags,
            exclude_tags,
        )

    def get_for_latest(
        self,
        entity_name: str,
        features: Optional[List[str]],
        templates: Optional[List[str]],
        categories: Optional[List[str]],
        time_windows: Optional[List[str]],
        include_tags: Optional[List[str]],
        exclude_tags: Optional[List[str]],
    ) -> DataFrame:
        metadata = self.__metadata_reader.read_for_latest(entity_name)

        return self.__metadata_filterer.filter(
            metadata,
            entity_name,
            features,
            templates,
            categories,
            time_windows,
            include_tags,
            exclude_tags,
        )

    def get_for_target(
        self,
        entity_name: str,
        target_id: str,
        features: Optional[List[str]],
        templates: Optional[List[str]],
        categories: Optional[List[str]],
        time_windows: Optional[List[str]],
        include_tags: Optional[List[str]],
        exclude_tags: Optional[List[str]],
    ) -> DataFrame:
        metadata = self.__metadata_reader.read_for_target(entity_name, target_id)

        return self.__metadata_filterer.filter(
            metadata,
            entity_name,
            features,
            templates,
            categories,
            time_windows,
            include_tags,
            exclude_tags,
        )
