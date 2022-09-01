from abc import ABC, abstractmethod

class Extract(ABC):
    @abstractmethod
    def build_connection_string(self):
        pass

    @abstractmethod
    def execute(self):
        pass