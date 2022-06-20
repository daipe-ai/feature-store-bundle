from datetime import datetime
from numbers import Number

from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.utils.errors import WrongFillnaValueTypeError
from featurestorebundle.utils.errors import WrongTypeError
from featurestorebundle.utils.types import CATEGORICAL, NUMERICAL, BINARY


class TypeChecker:
    _valid_types = [CATEGORICAL, NUMERICAL, BINARY]

    def check(self, feature_template: FeatureTemplate, dtype: str):
        self.check_fillna_valid(dtype, feature_template.fillna_value, feature_template)
        self.check_type_valid(dtype, feature_template.type, feature_template)  # pyre-ignore[6]

    def check_fillna_valid(self, dtype: str, value, feature_template: FeatureTemplate):
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

    def check_type_valid(self, dtype: str, type_: str, feature_template: FeatureTemplate):
        if type_ is None:
            return

        if not self.is_type_valid(type_):
            raise WrongTypeError(f"Invalid type '{type_}', allowed types are {self._valid_types}")

        if self.is_type_categorical(type_) and not self.is_feature_valid_categorical(dtype):
            raise WrongTypeError(f"Data type {dtype} for feature {feature_template.name_template} cannot be {CATEGORICAL}")

        if self.is_type_numerical(type_) and not self.is_feature_valid_numerical(dtype):
            raise WrongTypeError(f"Data type {dtype} for feature {feature_template.name_template} cannot be {NUMERICAL}")

        if self.is_type_binary(type_) and not self.is_feature_valid_binary(dtype):
            raise WrongTypeError(f"Data type {dtype} for feature {feature_template.name_template} cannot be {BINARY}")

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

    def is_type_valid(self, type_: str):
        return type_ in self._valid_types

    def is_type_categorical(self, type_: str):
        return type_ == CATEGORICAL

    def is_type_numerical(self, type_: str):
        return type_ == NUMERICAL

    def is_type_binary(self, type_: str):
        return type_ == BINARY

    def is_feature_valid_categorical(self, dtype: str):
        return dtype in ["boolean", "byte", "short", "integer", "long", "string"]

    def is_feature_valid_numerical(self, dtype: str):
        return self.is_feature_numeric(dtype)

    def is_feature_valid_binary(self, dtype: str):
        return dtype == "boolean"
