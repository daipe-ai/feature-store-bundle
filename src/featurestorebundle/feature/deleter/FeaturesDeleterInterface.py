from typing import List
from abc import ABC, abstractmethod


class FeaturesDeleterInterface(ABC):
    @abstractmethod
    def delete(self, features: List[str]):
        pass
