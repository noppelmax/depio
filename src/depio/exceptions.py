

# TASK EXCEPTIONS
class ProductNotProducedException(Exception):
    pass


class ProductNotUpdatedException(Exception):
    pass


class DependencyNotMetException(Exception):
    pass


class TaskRaisedException(Exception):
    pass


# TASKHANDLER EXCEPTION
class TaskNotInQueueException(Exception):
    pass

class ProductAlreadyRegisteredException(Exception):
    pass

class DependencyNotAvailableException(Exception):
    pass