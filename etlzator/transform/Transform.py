from abc import ABC, abstractmethod


class Transform(ABC):
    @abstractmethod
    def execute(self):
        pass
