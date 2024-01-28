import os
import unittest
import urllib.error

import requests.exceptions

from seekablehttpfile import SeekableHttpFile
from seekablehttpfile.core import get_range_requests

SAMPLE_FILE = "https://files.pythonhosted.org/packages/86/ea/f27b648330abff7d07faf03f2dbe8070630d2a14b79185f165d555447071/seekablehttpfile-0.0.4-py3-none-any.whl"


class LiveTests(unittest.TestCase):
    """These tests all require internet access."""

    def test_live_synthetic(self) -> None:
        f = SeekableHttpFile("http://timhatch.com/projects/http-tests/sequence_100.txt")
        self.assertEqual(0, f.pos)
        self.assertEqual(292, f.length)
        self.assertEqual(b"1\n", f.read(2))
        f.seek(-4, 2)
        self.assertEqual(b"100\n", f.read(4))
        f.seek(-4, 2)
        self.assertEqual(b"100\n", f.read())
        self.assertEqual(292, f.tell())
        self.assertTrue(f.seekable())
        self.assertEqual(b"", f.read(0))
        f.seek(1, 0)
        self.assertEqual(1, f.pos)
        f.seek(2, 1)
        self.assertEqual(3, f.pos)

        # tests the read doing a fetch
        f.seek(-4, 2)
        f.end_cache_start = f.length
        self.assertEqual(b"100\n", f.read(4))

        # errors
        with self.assertRaises(ValueError):
            f.seek(0, 99)

    def test_live_404(self) -> None:
        with self.assertRaises(urllib.error.HTTPError):
            SeekableHttpFile(
                "http://timhatch.com/projects/http-tests/response/?code=404"
            )

    def test_live_404_requests(self) -> None:
        with self.assertRaises(requests.exceptions.HTTPError):
            SeekableHttpFile(
                "http://timhatch.com/projects/http-tests/response/?code=404",
                get_range=get_range_requests,
            )

    def test_live_pypi(self) -> None:
        f = SeekableHttpFile(SAMPLE_FILE)
        f.seek(0, os.SEEK_SET)
        f.read(12)
        f.seek(-10, os.SEEK_END)
        f.read(10)
        self.assertEqual(b"", f.read(2))
        f.seek(-10, os.SEEK_END)
        f.read(12)
        self.assertEqual(4, f.stats["satisfied_from_cache"])
        self.assertEqual(0, f.stats["lazy_bytes_read"])

    def test_live_pypi_redirect(self) -> None:
        f = SeekableHttpFile("http://httpbin.org/redirect-to?url=" + SAMPLE_FILE)
        self.assertEqual(SAMPLE_FILE, f.url)

    def test_live_requests_pypi(self) -> None:
        _ = SeekableHttpFile(SAMPLE_FILE, get_range=get_range_requests)

    def test_live_requests_pypi_redirect(self) -> None:
        f = SeekableHttpFile(
            "http://httpbin.org/redirect-to?url=" + SAMPLE_FILE,
            get_range=get_range_requests,
        )
        self.assertEqual(SAMPLE_FILE, f.url)
