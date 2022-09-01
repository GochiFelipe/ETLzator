from abc import ABC, abstractmethod


class Load(ABC):
    @abstractmethod
    def build_connection_string(self):
        pass

    @abstractmethod
    def execute(self):
        return self
