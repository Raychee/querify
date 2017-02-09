class QuerifyError(Exception):
    pass


class InvalidQuery(QuerifyError):
    pass


class UnrecognizedExprType(InvalidQuery):
    pass


class InvalidFilter(QuerifyError):
    pass


class UnrecognizedJsonableClass(QuerifyError):
    pass