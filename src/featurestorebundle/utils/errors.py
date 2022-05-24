class DateFormatError(Exception):
    pass


class IncompleteRowsError(Exception):
    pass


class MissingWidgetDefaultError(Exception):
    def __init__(self, widget_name: str):
        message = f"Widget default for {widget_name} not found. Please set featurestorebundle.widgets.defaults.{widget_name} in config.yaml"
        super().__init__(message)


class MissingEntitiesError(Exception):
    pass


class MissingColumnError(Exception):
    pass


class MissingTargetsTableError(Exception):
    pass


class MissingTargetsEnumTableError(Exception):
    pass


class TemplateMatchingError(Exception):
    pass


class TimeWindowFormatError(Exception):
    pass


class TimeShiftFormatError(Exception):
    pass


class UnsupportedChangeFeatureNameError(Exception):
    pass


class WrongColumnTypeError(Exception):
    pass


class WrongTypeError(Exception):
    pass


class WrongFillnaValueTypeError(Exception):
    def __init__(self, value: str, name_template: str, dtype: str):
        message = f"Value for fillna_with={value} is not compatible with feature {name_template} of the type {dtype}"
        super().__init__(message)
