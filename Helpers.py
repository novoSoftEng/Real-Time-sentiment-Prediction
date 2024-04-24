import csv
import time
from typing import Any, Dict, Generator
import pandas as pd
import pathlib
import json

class Reader:
    """Reader class."""

    def __init__(self, path: str) -> None:
        """Class constructor."""
        self._path = path

    def read(self) -> Generator[Dict[str, Any], None, None]:
        """Read data from a .csv file and return it as a generator.

        Yields:
            Generator[Dict[str, Any], None, None]: Data generator.
        """
        # Manually define field names and their types
        fieldnames = ["Tweet ID", "Entity", "Sentiment", "Tweet content"]
        fieldtypes = [int, str, str, str]

        with open(self._path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile, fieldnames=fieldnames)
            
            # Skip the header row
            next(reader)

            for row in reader:
                data = {}
                for field, fieldtype in zip(fieldnames, fieldtypes):
                    # Convert the value to the specified type
                    data[field] = fieldtype(row[field])
                yield data


    def read_with_sleep(self) -> Generator[Dict[str, Any], None, None]:
        """Read data from a .csv file with simulated delay and return it as a generator.

        Yields:
            Generator[Dict[str, Any], None, None]: Data generator.
        """
        for data in self.read():
            # Simulates a sample rate of 1 second for a sensor
            time.sleep(1)
            yield data


class Serializer:
    """Serializer class."""

    def serialize(self, data: Any) -> bytes:
        """Serialize any object to bytes."""
        if isinstance(data, dict):
            return json.dumps(data).encode('utf-8')
        elif isinstance(data, str):
            return data.encode('utf-8')
        elif isinstance(data, bytes):
            return data
        else:
            # For other types, you may implement custom serialization logic
            raise ValueError(f"Unsupported data type for serialization: {type(data)}")

