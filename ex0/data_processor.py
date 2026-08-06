from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self):
        self.data = []
        self.rank = 0

    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.data:
            raise Exception("No data available")
        value = self.data.pop(0)
        result = (self.rank, value)
        self.rank += 1
        return result


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
            if isinstance(data, list):
                return True
            if data is list[]:
                for num in data:
                    if data == isinstance(num, (list[int | float])):
                        return False
                return True


    def ingest(self, data: int | float | list[int | float]) -> None:
        pass