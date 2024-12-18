# TASK EXCEPTIONS
class ProductNotProducedException(Exception):
    pass


class ProductNotUpdatedException(Exception):
    pass


class DependencyNotMetException(Exception):
    pass


class TaskRaisedExceptionException(Exception):
    pass


class UnknownStatusException(Exception):
    pass


# TASKHANDLER EXCEPTION
class TaskNotInQueueException(Exception):
    pass


class ProductAlreadyRegisteredException(Exception):
    pass


class DependencyNotAvailableException(Exception):
    pass
