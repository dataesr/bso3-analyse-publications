from abc import ABCMeta


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

    @classmethod
    def clear_instance(cls):
        try:
            del Singleton._instances[cls]
        except KeyError:
            pass


class SingletonABCMeta(ABCMeta):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonABCMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

    @classmethod
    def clear_instance(cls):
        try:
            del SingletonABCMeta._instances[cls]
        except KeyError:
            pass
