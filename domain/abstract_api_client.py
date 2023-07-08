from utils.singleton import SingletonABCMeta
from abc import abstractmethod


class AbstractAPIClient(metaclass=SingletonABCMeta):
    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abstractmethod
    def download_publication(self):
        raise NotImplementedError
