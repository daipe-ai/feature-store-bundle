from typing import List

from pyspark.sql import DataFrame

from featurestorebundle.utils.errors import MissingColumnError, WrongColumnTypeError


class ColumnChecker:
    def check_column_exists(self, df: DataFrame, column_name: str):
        if column_name not in df.columns:
            raise MissingColumnError(f"Column `{column_name}` is not present in the input DataFrame")

    def check_column_type(self, df: DataFrame, column_name: str, expected_types: List[str]):
        self.check_column_exists(df, column_name)

        dtypes_dict = {col["name"]: col["type"] for col in df.schema.jsonValue()["fields"]}
        if dtypes_dict[column_name] not in expected_types:
            types_str = ", ".join(expected_types)
            raise WrongColumnTypeError(
                f"Column `{column_name}` is of type `{dtypes_dict[column_name]}` but expected types are [{types_str}]. "
                f"Please convert it or use another appropriate column"
            )
