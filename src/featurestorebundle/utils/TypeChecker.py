from typing import Optional
from datetime import datetime
from numbers import Number

from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.utils.errors import WrongFillnaValueTypeError
from featurestorebundle.utils.errors import WrongTypeError
from featurestorebundle.utils.types import CATEGORICAL, NUMERICAL, BINARY


class TypeChecker:
    _valid_variable_types = [CATEGORICAL, NUMERICAL, BINARY]

    def check(self, feature_template: FeatureTemplate, dtype: str, variable_type: Optional[str]):
        self.check_fillna_valid(dtype, feature_template.fillna_value, feature_template)
        self.check_variable_type_valid(dtype, variable_type, feature_template)

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

    def check_variable_type_valid(self, dtype: str, variable_type: Optional[str], feature_template: FeatureTemplate):
        if variable_type is None:
            return

        if not self.is_variable_type_valid(variable_type):
            raise WrongTypeError(f"Invalid type '{variable_type}', allowed types are {self._valid_variable_types}")

        if self.is_variable_type_categorical(variable_type) and not self.is_feature_valid_categorical(dtype):
            raise WrongTypeError(f"Data type {dtype} for feature {feature_template.name_template} cannot be {CATEGORICAL}")

        if self.is_variable_type_numerical(variable_type) and not self.is_feature_valid_numerical(dtype):
            raise WrongTypeError(f"Data type {dtype} for feature {feature_template.name_template} cannot be {NUMERICAL}")

        if self.is_variable_type_binary(variable_type) and not self.is_feature_valid_binary(dtype):
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

    def is_variable_type_valid(self, variable_type: str):
        return variable_type in self._valid_variable_types

    def is_variable_type_categorical(self, variable_type: str):
        return variable_type == CATEGORICAL

    def is_variable_type_numerical(self, variable_type: str):
        return variable_type == NUMERICAL

    def is_variable_type_binary(self, variable_type: str):
        return variable_type == BINARY

    def is_feature_valid_categorical(self, dtype: str):
        return dtype in ["boolean", "byte", "short", "integer", "long", "string"]

    def is_feature_valid_numerical(self, dtype: str):
        return self.is_feature_numeric(dtype)

    def is_feature_valid_binary(self, dtype: str):
        return dtype == "boolean"
