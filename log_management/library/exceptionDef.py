class LMException(Exception):
    pass

class JobIsInProgress(LMException):
    pass

class FailedValidation(LMException):
    pass

class NotFound(LMException):
    pass

class FailedCreation(LMException):
    pass

class Conflict(LMException):
    pass
