import re
from typing import Optional
from datetime import date
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.delta.target.schema import get_target_id_column_name, get_id_column_name, get_time_column_name


class TargetsFilteringManager:
    def get_targets(
        self,
        entity: Entity,
        targets: DataFrame,
        target_id: str,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        time_diff: Optional[str] = None,
    ) -> DataFrame:
        id_column = get_id_column_name(entity)
        time_column = get_time_column_name(entity)
        target_id_column = get_target_id_column_name(entity)

        df = targets.filter(f.col(target_id_column) == target_id)

        if date_from:
            df = df.filter(f.col(time_column) >= date_from)

        if date_to:
            df = df.filter(f.col(time_column) <= date_to)

        if time_diff:
            df = self.__shift_time_column(df, time_column, time_diff)

        return df.select(id_column, time_column)

    def __shift_time_column(self, df: DataFrame, time_column: str, time_diff: str) -> DataFrame:
        matches = re.match(r"([+-]?[0-9]+)([smhdw])", time_diff)

        if not matches:
            raise Exception("Invalid time format try something like '7d' for seven days")

        periods = {
            "s": "SECONDS",
            "m": "MINUTES",
            "h": "HOURS",
            "d": "DAYS",
            "w": "WEEKS",
        }

        integer_part = matches[1]
        period_part = matches[2]

        return df.withColumn(time_column, f.col(time_column) + f.expr(f"INTERVAL {integer_part} {periods[period_part]}"))
