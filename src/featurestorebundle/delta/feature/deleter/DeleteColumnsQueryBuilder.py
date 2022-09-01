from typing import List


class DeleteColumnsQueryBuilder:
    def build_delete_columns_query(self, table_identifier: str, features: List[str]) -> str:
        return f"ALTER TABLE {table_identifier} DROP COLUMNS ({','.join(features)})"
