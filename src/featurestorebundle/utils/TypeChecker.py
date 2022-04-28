from datetime import datetime
from numbers import Number

from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.utils.errors import WrongFillnaValueTypeError


class TypeChecker:
    def check(self, feature_template: FeatureTemplate, dtype: str, value):
        if self.is_none(value):
            return

        if self.is_feature_bool(dtype) and not self.is_value_bool(value):
            raise WrongFillnaValueTypeError(value, feature_template.name_template, dtype)

        if self.is_feature_numeric(dtype) and not self.is_value_numeric(value):
            raise WrongFillnaValueTypeError(value, feature_template.name_template, dtype)

        if self.is_feature_string(dtype) and not self.is_value_string(value):
            raise WrongFillnaValueTypeError(value, feature_template.name_template, dtype)

        if self.is_feature_datetime(dtype) and not self.is_value_datetime(value):
            raise WrongFillnaValueTypeError(value, feature_template.name_template, dtype)

    def is_none(self, value) -> bool:
        return value is None

    def is_value_bool(self, value) -> bool:
        return isinstance(value, bool)

    def is_feature_bool(self, dtype: str) -> bool:
        return dtype == "boolean"

    def is_value_numeric(self, value) -> bool:
        return isinstance(value, Number)

    def is_feature_numeric(self, dtype: str) -> bool:
        return dtype in ["byte", "short", "integer", "long", "float", "double"] or dtype.startswith("decimal")

    def is_value_string(self, value) -> bool:
        return isinstance(value, str)

    def is_feature_string(self, dtype: str) -> bool:
        return dtype == "string"

    def is_value_datetime(self, value) -> bool:
        return isinstance(value, datetime)

    def is_feature_datetime(self, dtype: str) -> bool:
        return dtype in ["date", "timestamp"]
