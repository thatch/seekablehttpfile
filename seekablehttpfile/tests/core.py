import unittest
import urllib.error
from typing import Optional
from unittest.mock import Mock

import requests.exceptions

from seekablehttpfile import SeekableHttpFile
from seekablehttpfile.core import EtagChangedError, GeneralizedResponse


class Fixture:
    def __init__(
        self, redir_url: Optional[str] = None, etag: Optional[str] = None
    ) -> None:
        self.x = b"foo"
        self.should_raise_on_open_ended: Optional[str] = None
        self.redir_url = redir_url
        self.etag = etag
        self.last_fetched_url: Optional[str] = None

    def get_range(
        self, url: str, t: Optional[str], method: Optional[str] = "GET"
    ) -> GeneralizedResponse:
        self.last_fetched_url = url
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
                if self.should_raise_on_open_ended == "urllib":
                    raise urllib.error.HTTPError(
                        code=501,
                        url="",
                        msg="",
                        hdrs=None,  # type: ignore
                        fp=None,
                    )
                elif self.should_raise_on_open_ended == "requests":
                    raise requests.exceptions.HTTPError(
                        request=Mock(),
                        response=Mock(status_code=501),
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
        r.should_raise_on_open_ended = "urllib"
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

    def test_short_read_requests(self) -> None:
        r = Fixture()
        r.should_raise_on_open_ended = "requests"
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

    def test_url_redirect_on_first_req(self) -> None:
        r = Fixture(redir_url="z")
        f = SeekableHttpFile("", get_range=r.get_range, precache=0)
        f.read(1)
        self.assertEqual("z", f.url)
        f.read(1)
        self.assertEqual("z", r.last_fetched_url)

    def test_etag_changes(self) -> None:
        r = Fixture(etag="x")
        f = SeekableHttpFile("", get_range=r.get_range, precache=0)
        f.read(1)
        r.etag = "y"
        with self.assertRaisesRegex(
            EtagChangedError, "Previous etag was 'x', new one is 'y'"
        ):
            f.read(1)

    def test_etag_changes_ok(self) -> None:
        r = Fixture(etag="x")
        f = SeekableHttpFile("", get_range=r.get_range, precache=0, check_etag=False)
        f.read(1)
        r.etag = "y"
        f.read(1)
        # Implementation detail that I'm not tied to, we don't bother even
        # setting the new etag.
        self.assertEqual("x", f.etag)

    def test_etag_goes_away(self) -> None:
        r = Fixture(etag="x")
        f = SeekableHttpFile("", get_range=r.get_range, precache=0)
        f.read(1)
        r.etag = None
        # This is not an error currently, and we will not clear the saved value.
        f.read(1)
        self.assertEqual("x", f.etag)

    def test_etag_appears(self) -> None:
        r = Fixture(etag=None)
        f = SeekableHttpFile("", get_range=r.get_range, precache=0)
        f.read(1)
        r.etag = "x"
        f.read(1)
        # Not an error
        self.assertEqual("x", f.etag)
