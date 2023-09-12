from abc import ABCMeta, abstractmethod

class AbstractTask(metaclass=ABCMeta):
    """Abstract Task"""
    
    @staticmethod
    @abstractmethod
    def create(self):
        pass