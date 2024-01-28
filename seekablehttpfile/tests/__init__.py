import doctest
from unittest import TestLoader, TestSuite

from .core import SeekableHttpFileTest
from .live import LiveTests


def load_tests(loader: TestLoader, tests: TestSuite, _pattern: None) -> TestSuite:
    tests.addTests(doctest.DocFileSuite("../../README.md"))
    return tests


__all__ = [
    "SeekableHttpFileTest",
    "LiveTests",
    "load_tests",
]
