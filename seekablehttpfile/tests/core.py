import unittest
import urllib.error
from http.client import HTTPResponse
from io import BytesIO
from typing import Any, Optional

from seekablehttpfile import SeekableHttpFile


class FakeSocket:
    def __init__(self, io: BytesIO):
        self.io = io

    def makefile(self, _: Any) -> BytesIO:
        return self.io


class Fixture:
    def __init__(self) -> None:
        self.x = b"foo"
        self.should_raise_on_open_ended = False

    def get_range(
        self, url: str, t: Optional[str], method: Optional[str] = "GET"
    ) -> HTTPResponse:
        if t is None:
            data = f"HTTP/1.0 200 OK\nContent-Length: {len(self.x)}\n\n".encode()
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
            t = f"{start}-{end}/{len(self.x)}"

            data = (
                f"HTTP/1.0 206 Partial Content\nContent-Range: bytes {t}\n\n".encode()
                + self.x[start:end]
            )

        resp = HTTPResponse(FakeSocket(BytesIO(data)))  # type: ignore
        resp.begin()
        return resp


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

    def test_live(self) -> None:
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

    def test_live_404(self) -> None:
        # This test requires internet access.
        with self.assertRaises(urllib.error.HTTPError):
            SeekableHttpFile(
                "http://timhatch.com/projects/http-tests/response/?code=404"
            )
