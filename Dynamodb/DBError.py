import Services.Error 

class DBInsertError(Exception):
     pass

class DBUpdateError(Exception):
     pass

class DBDeleteError(Exception):
    pass

class DBSelectError(Exception):
    pass

class DifferentProductError(Exception):
    pass

class NotExistError(Exception):
    pass

class AlreadyExistError(Exception):
    pass

class LoadQuantityError(Services.Error.LoadQuantityError):
    pass

class OffloadQuantityError(Services.Error.OffloadQuantityError):
    pass

class ValidationFailedError(Exception):
    pass