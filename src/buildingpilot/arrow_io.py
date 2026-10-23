"""Normalize Windows file URIs at the PyIceberg FileIO boundary only."""

import os
import re

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.typedef import EMPTY_DICT, Properties


class PortableArrowFileIO(PyArrowFileIO):
    @staticmethod
    def parse_location(location: str, properties: Properties = EMPTY_DICT) -> tuple[str, str, str]:
        scheme, netloc, path = PyArrowFileIO.parse_location(location, properties)
        if os.name == "nt" and scheme == "file" and re.match(r"^/[A-Za-z]:/", path):
            path = path[1:]
        return scheme, netloc, path
