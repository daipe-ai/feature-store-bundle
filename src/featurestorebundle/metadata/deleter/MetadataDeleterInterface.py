from typing import List
from abc import ABC, abstractmethod


class MetadataDeleterInterface(ABC):
    @abstractmethod
    def delete(self, features: List[str]):
        pass
