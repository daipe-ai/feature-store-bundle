from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from featurestorebundle.utils.errors import IncompleteRowsError


class IncompleteRowsHandler:
    def handle(self, features_data: DataFrame, skip_incomplete_rows: bool):
        if skip_incomplete_rows:
            return features_data.na.drop(how="any")

        has_incomplete_rows = (
            len(features_data.filter(f.greatest(*[f.col(i).isNull() for i in features_data.columns])).limit(1).collect()) == 1
        )

        if has_incomplete_rows:
            raise IncompleteRowsError("Features contain incomplete rows. Use skip_incomplete_rows=True")

        return features_data
