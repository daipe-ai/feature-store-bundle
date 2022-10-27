from typing import Optional
from datetime import datetime
from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.target.reader.TargetsTableReader import TargetsTableReader
from featurestorebundle.target.filter.TargetsFilterer import TargetsFilterer


class TargetsGetter:
    def __init__(self, targets_reader: TargetsTableReader, targets_filterer: TargetsFilterer):
        self.__targets_reader = targets_reader
        self.__targets_filterer = targets_filterer

    def get_targets(
        self,
        entity: Entity,
        target_id: str,
        target_date_from: Optional[datetime] = None,
        target_date_to: Optional[datetime] = None,
        time_diff: Optional[str] = None,
    ) -> DataFrame:
        targets = self.__targets_reader.read(entity.name)

        return self.__targets_filterer.filter(entity, targets, target_id, target_date_from, target_date_to, time_diff)
