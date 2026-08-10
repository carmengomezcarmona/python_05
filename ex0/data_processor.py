from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.data: list[str] = []
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
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            for num in data:
                if not isinstance(num, (int, float)):
                    return False
            return True
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise Exception("Improper numeric data")
        if isinstance(data, list):
            for num in data:
                value = str(num)
                self.data.append(value)
        else:
            value = str(data)
            self.data.append(value)


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            for word in data:
                if not isinstance(word, (str)):
                    return False
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise Exception("Improper text data")
        if isinstance(data, list):
            for word in data:
                self.data.append(word)
        else:
            self.data.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    return False
            return True
        if isinstance(data, list):
            for log in data:
                for key, value in log.items():
                    if not isinstance(key, str) or not isinstance(value, str):
                        return False
            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")
        if isinstance(data, list):
            for log in data:
                value = log["log_level"] + ": " + log["log_message"]
                self.data.append(value)
        else:
            value = data["log_level"] + ": " + data["log_message"]
            self.data.append(value)


print("=== Code Nexus - Data Processor ===")
print()
print("Testing Numeric Processor...")
numeric = NumericProcessor()
print(" Trying to validate input '42':", numeric.validate(42))
print(" Trying to validate input 'Hello':", numeric.validate("Hello"))
print(" Test invalid ingestion of string 'foo' without prior validation:")
try:
    numeric.ingest("foo")
except Exception as e:
    print(" Got exception:", e)
print(" Processing data:", [1, 2, 3, 4, 5])
numeric.ingest([1, 2, 3, 4, 5])
print(" Extracting 3 values...")
for i in range(3):
    rank, value = numeric.output()
    print(f" Numeric value {rank}: {value}")
print()
print("Testing Text Processor...")
text = TextProcessor()
print(" Trying to validate input '42':", text.validate(42))
print(" Processing data:", ["Hello", "Nexus", "World"])
text.ingest(["Hello", "Nexus", "World"])
print(" Extracting 1 value...")
rank, value = text.output()
print(f" Text value {rank}: {value}")
print()
print("Testing Log Processor...")
log = LogProcessor()
print(" Trying to validate input 'Hello':", log.validate("Hello"))
log_data = [
    {
        "log_level": "NOTICE",
        "log_message": "Connection to server"
    },
    {
        "log_level": "ERROR",
        "log_message": "Unauthorized access!!"
    }
]
print(" Processing data:", log_data)
log.ingest(log_data)
print(" Extracting 2 values...")
for i in range(2):
    rank, value = log.output()
    print(f"Log entry {rank}: {value}")
