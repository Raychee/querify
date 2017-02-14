class QuerifyError(Exception):
    pass


class InvalidQuery(QuerifyError):
    pass


class UnrecognizedExprType(InvalidQuery):
    pass


class UnrecognizedJsonableClass(QuerifyError):
    pass