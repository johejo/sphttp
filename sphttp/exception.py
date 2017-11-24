class SphttpException(Exception):
    pass


class FileSizeError(SphttpException):
    pass


class StatusCodeError(SphttpException):
    pass


class InvalidStatusCode(SphttpException):
    pass


class IncompleteError(SphttpException):
    pass


class NoContentLength(SphttpException):
    pass


class NoAcceptRanges(SphttpException):
    pass


class SchemeError(SphttpException):
    pass


class DelayRequestAlgorithmError(SphttpException):
    pass
