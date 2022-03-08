from abc import ABC, abstractmethod


class PostActionInterface(ABC):
    @abstractmethod
    def run(self):
        pass
