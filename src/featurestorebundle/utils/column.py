from pyspark.sql.column import Column


def get_column_name(col: Column) -> str:
    return str(col).replace("`", "").split("'")[-2].split(" AS ")[-1]
