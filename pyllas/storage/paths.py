from __future__ import annotations

from typing import Callable

import re
from copy import deepcopy
from datetime import datetime


class Path:

    def __init__(self, path: str = '', parts=None):
        parts = parts or []
        if path:
            parts.append(path)

        self.parts = parts

    def resolve(self, prefix: str | Path) -> Path:
        parts = deepcopy(self.parts)

        if type(prefix) is Path:
            parts.extend(prefix.parts)
        else:
            parts.append(prefix)

        return Path(parts=parts)

    def format(self, pattern_prefix: str, value: datetime) -> Path:
        return self.resolve(value.strftime(pattern_prefix))

    def map(self, func: Callable[[str], str]) -> Path:
        return Path(func(self.as_str()))

    def drop_tail_slash(self) -> Path:
        return self.map(lambda path: path.removesuffix('/'))

    def as_str(self) -> str:
        parts = deepcopy(self.parts)
        if len(parts) > 1:
            first = parts.pop(0).removesuffix('/')
            last = parts.pop().removeprefix('/')

            parts = (part.strip('/') for part in parts)
            parts = list(filter(lambda part: len(part) > 0, parts))

            parts.insert(0, first)
            parts.append(last)

        return '/'.join(parts)

    @staticmethod
    def root():
        return Path('/')

    def __truediv__(self, prefix: str) -> Path:
        """
        Syntax sugar for `resolve` method.
        """
        return self.resolve(prefix)

    def __mod__(self, value_tuple: tuple[str, datetime]) -> Path:
        """
        Syntax sugar for `format` method.
        """
        (pattern_prefix, value) = value_tuple

        return self.format(pattern_prefix, value)

    def __eq__(self, other: Path) -> bool:
        return type(other) is Path and self.as_str() == other.as_str()

    def __str__(self) -> str:
        return self.as_str()

    def __repr__(self) -> str:
        return f"Path('{self.as_str()}')"


class S3Path:
    EXTRACT_PATTERN = 's3a?://(?P<bucket>[a-z\\d.-]*)/?(?P<prefix>.*)'

    def __init__(self, path: str):
        result = re.search(S3Path.EXTRACT_PATTERN, path)
        if not result:
            raise ValueError(f"Provided path('{path}') can't be parsed correctly!")

        self.path = Path(path)
        self.bucket = result.group('bucket')
        self.prefix = result.group('prefix')

    def drop_tail_slash(self) -> S3Path:
        return S3Path(str(self.path.drop_tail_slash()))

    def resolve(self, prefix: str | Path) -> S3Path:
        path = self.path.resolve(prefix)

        return S3Path(str(path))

    def format(self, pattern_prefix: str, value: datetime) -> S3Path:
        path = self.path.format(pattern_prefix, value)

        return S3Path(str(path))

    def map(self, func: Callable[[str], str]) -> S3Path:
        return S3Path(str(self.path.map(func)))

    def as_str(self) -> str:
        return str(self.path)

    def __truediv__(self, prefix: str | Path) -> S3Path:
        """
        Syntax sugar for `resolve` method.
        """
        return self.resolve(prefix)

    def __mod__(self, value_tuple: tuple[str, datetime]) -> S3Path:
        """
        Syntax sugar for `format` method.
        """
        (pattern_prefix, value) = value_tuple

        return self.format(pattern_prefix, value)

    def __eq__(self, other: S3Path) -> bool:
        return type(other) is S3Path and self.bucket == other.bucket and self.prefix == other.prefix

    def __str__(self) -> str:
        return self.as_str()

    def __repr__(self) -> str:
        return f"S3Path('{self.path.as_str()}')"
