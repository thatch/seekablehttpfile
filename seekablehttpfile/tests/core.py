import os
import unittest
import urllib.error
from typing import Optional

from seekablehttpfile import SeekableHttpFile
from seekablehttpfile.core import GeneralizedResponse, get_range_requests

SAMPLE_FILE = "https://files.pythonhosted.org/packages/86/ea/f27b648330abff7d07faf03f2dbe8070630d2a14b79185f165d555447071/seekablehttpfile-0.0.4-py3-none-any.whl"


class Fixture:
    def __init__(
        self, redir_url: Optional[str] = None, etag: Optional[str] = None
    ) -> None:
        self.x = b"foo"
        self.should_raise_on_open_ended = False
        self.redir_url = redir_url
        self.etag = etag

    def get_range(
        self, url: str, t: Optional[str], method: Optional[str] = "GET"
    ) -> GeneralizedResponse:
        if t is None:
            assert method == "HEAD"
            return GeneralizedResponse(
                self.redir_url or url,
                str(len(self.x)),
                None,
                self.etag,
                b"",
            )
        else:
            t = t[6:]  # strip 'bytes='
            if t[0] == "-":
                if self.should_raise_on_open_ended:
                    raise urllib.error.HTTPError(
                        code=501,
                        url="",
                        msg="",
                        hdrs=None,  # type: ignore
                        fp=None,
                    )
                start = max(0, len(self.x) - int(t[1:]))
                end = len(self.x)
            else:
                start, end = map(int, t.split("-"))
                end += 1  # Python half-open
            t = f"bytes {start}-{end}/{len(self.x)}"

            return GeneralizedResponse(
                self.redir_url or url,
                str(end - start + 1),
                t,
                self.etag,
                self.x[start:end],
            )


class SeekableHttpFileTest(unittest.TestCase):
    def test_smoke(self) -> None:
        r = Fixture()
        f = SeekableHttpFile("", get_range=r.get_range)
        self.assertEqual(0, f.pos)
        self.assertEqual(3, f.length)
        self.assertEqual(b"f", f.read(1))
        self.assertEqual(1, f.stats["num_requests"])
        self.assertEqual(3, f.stats["optimistic_bytes_read"])
        self.assertEqual(0, f.stats["lazy_bytes_read"])
        self.assertEqual(b"foo", f.end_cache)  # _optimistic_first_read

    def test_partially_cached(self) -> None:
        # edge case where it's only partially in end_cache
        r = Fixture()
        f = SeekableHttpFile("", get_range=r.get_range, precache=2)
        self.assertEqual(0, f.pos)
        self.assertEqual(3, f.length)
        self.assertEqual(b"fo", f.read(2))
        self.assertEqual(2, f.stats["num_requests"])
        self.assertEqual(2, f.stats["optimistic_bytes_read"])
        self.assertEqual(2, f.stats["lazy_bytes_read"])
        self.assertEqual(b"oo", f.end_cache)  # _optimistic_first_read

    def test_pessimist(self) -> None:
        r = Fixture()
        r.should_raise_on_open_ended = True
        f = SeekableHttpFile("", get_range=r.get_range, precache=0)
        self.assertEqual(0, f.pos)
        self.assertEqual(3, f.length)
        self.assertEqual(1, f.stats["num_requests"])
        self.assertEqual(b"f", f.read(1))
        self.assertEqual(2, f.stats["num_requests"])
        self.assertEqual(0, f.stats["optimistic_bytes_read"])
        self.assertEqual(1, f.stats["lazy_bytes_read"])
        self.assertEqual(b"", f.end_cache)

    def test_short_read(self) -> None:
        r = Fixture()
        r.should_raise_on_open_ended = True
        f = SeekableHttpFile("", get_range=r.get_range, precache=2)
        self.assertEqual(0, f.pos)
        self.assertEqual(3, f.length)
        r.x = b""
        with self.assertRaises(ValueError):
            f.read(3)
        self.assertEqual(4, f.stats["num_requests"])
        self.assertEqual(2, f.stats["optimistic_bytes_read"])
        self.assertEqual(3, f.stats["lazy_bytes_read"])
        self.assertEqual(b"oo", f.end_cache)  # _head

    def test_live_synthetic(self) -> None:
        # This test requires internet access.
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

    # Tests past this point require Internet access.

    def test_live_404(self) -> None:
        with self.assertRaises(urllib.error.HTTPError):
            SeekableHttpFile(
                "http://timhatch.com/projects/http-tests/response/?code=404"
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
