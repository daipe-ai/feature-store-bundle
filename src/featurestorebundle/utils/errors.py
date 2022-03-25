class DateFormatError(Exception):
    pass


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


class UnsupportedChangeFeatureNameError(Exception):
    pass


class WrongColumnTypeError(Exception):
    pass
