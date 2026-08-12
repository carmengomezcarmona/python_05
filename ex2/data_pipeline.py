from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.data: list[str] = []
        self.rank = 0
        self.total = 0

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
                self.total += 1
        else:
            value = str(data)
            self.data.append(value)
            self.total += 1


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
                self.total += 1
        else:
            self.data.append(data)
            self.total += 1


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
                self.total += 1
        else:
            value = data["log_level"] + ": " + data["log_message"]
            self.data.append(value)
            self.total += 1


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            found = False
            for processor in self.processors:
                if processor.validate(element):
                    processor.ingest(element)
                    found = True
                    break
            if not found:
                print("DataStream error - Can't process element in stream:",
                      element)

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for processor in self.processors:
            print(f"{processor.__class__.__name__}: total {processor.total}"
                  f" items processed, remaining {len(processor.data)} "
                  "on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self.processors:
            output_data: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    output_data.append(processor.output())
                except Exception:
                    break
            plugin.process_output(output_data)


class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values: list[str] = []
        for rank, value in data:
            values.append(value)
        print("CSV Output: ", ",".join(values))


class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        items: dict[str, str] = {}
        for rank, value in data:
            key = f"item_{rank}"
            items[key] = value
        parts: list[str] = []
        for key, value in items.items():
            parts.append(f'"{key}": "{value}"')
        result = ", ".join(parts)
        result = f"{{{result}}}"
        print("JSON Output:", result)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")
    print()
    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()
    print()
    print("Registering Processors")
    print()
    stream_data = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead"
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected"
            }
        ],
        42,
        ["Hi", "five"]
    ]
    print(f"Send first batch of data on stream: {stream_data}")
    print()
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    stream.register_processor(numeric)
    stream.register_processor(text)
    stream.register_processor(log)
    stream.process_stream(stream_data)
    stream.print_processors_stats()
    print()
    print("Send 3 processed data from each processor to a CSV plugin:")
    csv = CSVPlugin()
    stream.output_pipeline(3, csv)
    print()
    stream.print_processors_stats()
    print()
    stream_data_2 = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {
                "log_level": "ERROR",
                "log_message": "500 server crash"
            },
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days"
            }
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello"
    ]
    print(f"Send another batch of data: {stream_data_2}")
    print()
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    stream.process_stream(stream_data_2)
    stream.print_processors_stats()
    print()
    print("Send 5 processed data from each processor to a JSON plugin:")
    json = JSONPlugin()
    stream.output_pipeline(5, json)
    print()
    stream.print_processors_stats()
