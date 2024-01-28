import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar, Union
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import requests.sessions

# This is a little strange in order to get mypy to be happy with either.
F = TypeVar("F", bound=Callable[..., Any])


def ktrace(*trace_args: str, shortname: Union[str, bool] = False) -> Callable[[F], F]:
    def inner(func: F) -> F:
        return func

    return inner


try:
    from keke import ktrace  # type: ignore[no-redef,unused-ignore]  # noqa: F811
except ImportError:
    pass


LOG = logging.getLogger(__name__)

CONTENT_RANGE_RE = re.compile(r"bytes (\d+)-(\d+)/(\d+)")


class EtagChangedError(Exception):
    pass


@dataclass
class GeneralizedResponse:
    url: str
    content_length: Optional[str] = None
    content_range: Optional[str] = None
    etag: Optional[str] = None
    content: Optional[bytes] = None


@ktrace("content_range", "method")
def get_range_urlopen(
    url: str, content_range: Optional[str], method: Optional[str] = None
) -> GeneralizedResponse:
    method = method or "GET"
    headers = {"Range": content_range} if content_range is not None else {}
    # This is expected to raise an exception with .code
    resp = urlopen(Request(url, headers=headers, method=method))
    return GeneralizedResponse(
        resp.url,
        resp.headers["content-length"],
        resp.headers["content-range"],
        resp.headers.get("etag"),
        resp.read(),
    )


DEFAULT_SESSION = requests.sessions.Session()


@ktrace("content_range", "method")
def get_range_requests(
    url: str,
    content_range: Optional[str],
    method: Optional[str] = None,
    session: Optional[requests.sessions.Session] = None,
) -> GeneralizedResponse:
    method = method or "GET"
    if not session:
        session = DEFAULT_SESSION
    headers = {"Range": content_range} if content_range is not None else {}

    resp = session.request(method=method, url=url, headers=headers)
    resp.raise_for_status()
    return GeneralizedResponse(
        resp.url,
        resp.headers["content-length"],
        resp.headers.get("content-range"),
        resp.headers.get("etag"),
        resp.content,
    )


class SeekableHttpFile:
    def __init__(
        self,
        url: str,
        get_range: Callable[..., GeneralizedResponse] = get_range_urlopen,
        precache: int = 256_000,
        check_etag: bool = True,
    ) -> None:
        self.url = url
        self.get_range = get_range
        self.stats = {
            "num_requests": 0,
            "optimistic_bytes_read": 0,
            "lazy_bytes_read": 0,
            "satisfied_from_cache": 0,
        }
        self.pos = 0
        self.length = -1
        self.precache = precache
        self.check_etag = check_etag
        self.etag: Optional[str] = None

        self.end_cache: bytes = b""
        self.end_cache_start: Optional[int] = None

        if self.precache:
            try:
                # Try to read the length and satisfy initial zipfile reads with one
                # request.  Varnish (used on public pypi) does not support this, but
                # Apache and probably nginx does.
                self._optimistic_first_read()
                return
            except HTTPError as e:
                if e.code != 501:  # Unsupported range
                    raise
            except requests.exceptions.HTTPError as e:
                if e.response.status_code != 501:  # Unsupported range
                    raise

        # Just read the length if not precaching or being optimistic didn't work
        self._head()

    @ktrace()
    def _optimistic_first_read(self) -> None:
        """
        Read (up to) some length, using suffix-length.

        This lets us find the last bytes in the file (which we're sure to need
        if it's a zip) as well as figure out the total length using a single
        request.
        """
        LOG.debug("_optimistic_first_read")
        # Optimistically read the last few KB, which can satisfy both finding
        # the length and the first couple of reads (2 bytes from the end and 22
        # bytes from the end).  The default value was chosen looking at scipy
        # and saves another half-second for me.
        h = "bytes=-%d" % self.precache
        self.stats["num_requests"] += 1
        resp = self.get_range(self.url, h)

        assert resp.content_range is not None
        match = CONTENT_RANGE_RE.match(resp.content_range)
        assert match is not None, resp.content_range
        start, end, length = match.groups()
        self.length = int(length)
        assert resp.content is not None
        self.end_cache = resp.content
        self.stats["optimistic_bytes_read"] = len(self.end_cache)
        self.end_cache_start = int(start)
        # print(type(self.end_cache), self.end_cache_start, url)
        assert self.end_cache_start >= 0

        if resp.url != self.url:
            LOG.debug("Redirected %s -> %s", self.url, resp.url)
            self.url = resp.url
        if resp.etag:
            assert self.etag is None
            self.etag = resp.etag

    @ktrace()
    def _head(self) -> None:
        """
        Issue a HEAD request to find the length, then precache if desired.
        """
        LOG.debug("_head")
        self.stats["num_requests"] += 1
        resp = self.get_range(self.url, None, method="HEAD")

        assert resp.content_length is not None
        self.length = int(resp.content_length)
        self.end_cache_start = max(0, self.length - self.precache)
        if self.precache:
            self.stats["num_requests"] += 1
            resp = self.get_range(
                self.url, "bytes=%d-%d" % (self.end_cache_start, self.length - 1)
            )
            assert resp.content is not None
            self.end_cache = resp.content
            self.stats["optimistic_bytes_read"] = len(self.end_cache)

        if resp.url != self.url:
            LOG.debug("Redirected %s -> %s", self.url, resp.url)
            self.url = resp.url
        if resp.etag:
            assert self.etag is None
            self.etag = resp.etag

    def seek(self, pos: int, whence: int = 0) -> None:
        LOG.debug(f"seek {pos} {whence}")
        # TODO clamp/error
        if whence == os.SEEK_SET:
            self.pos = pos
        elif whence == os.SEEK_CUR:
            self.pos += pos
        elif whence == os.SEEK_END:
            self.pos = self.length + pos
        else:
            raise ValueError(f"Invalid value for whence: {whence!r}")

    def tell(self) -> int:
        LOG.debug("tell")
        return self.pos

    @ktrace("self.pos", "n")
    def read(self, n: int = -1) -> bytes:
        LOG.debug(f"read {n} @ {self.length - self.pos}")
        if n == -1:
            n = self.length - self.pos
        if n == 0:
            return b""

        assert self.end_cache_start is not None
        p = self.pos - self.end_cache_start
        if p >= 0:
            self.stats["satisfied_from_cache"] += 1
            self.pos += n
            return self.end_cache[p : p + n]

        self.stats["num_requests"] += 1
        resp = self.get_range(self.url, "bytes=%d-%d" % (self.pos, self.pos + n - 1))
        assert resp.content is not None
        data = resp.content

        self.stats["lazy_bytes_read"] += n
        self.pos += n
        if len(data) != n:
            raise ValueError("Truncated read", len(data), n)

        if resp.url != self.url:
            LOG.debug("Redirected on subsequent read %s -> %s", self.url, resp.url)
            self.url = resp.url
        if resp.etag:
            # It's a little weird to find out an etag on subsequent request, but
            # possible I suppose.
            if self.etag is None:
                self.etag = resp.etag
            elif self.check_etag and self.etag != resp.etag:
                raise EtagChangedError(
                    f"Previous etag was {self.etag!r}, new one is {resp.etag!r}"
                )

        return data

    def seekable(self) -> bool:
        return True
