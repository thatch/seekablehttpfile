import logging
import re
from http.client import HTTPResponse
from typing import Callable, Optional
from urllib.error import HTTPError
from urllib.request import Request, urlopen

LOG = logging.getLogger(__name__)

CONTENT_RANGE_RE = re.compile(r"bytes (\d+)-(\d+)/(\d+)")


def get_range_urlopen(
    url: str, content_range: Optional[str], method: Optional[str] = None
) -> HTTPResponse:
    if method is None:
        method = "GET"
    headers = {"Range": content_range} if content_range is not None else {}
    return urlopen(Request(url, headers=headers, method=method))  # type: ignore


class SeekableHttpFile:
    def __init__(
        self,
        url: str,
        get_range: Callable[..., HTTPResponse] = get_range_urlopen,
    ) -> None:
        self.url = url
        self.get_range = get_range
        self.pos = 0
        self.length = -1

        self.end_cache: bytes = b""
        self.end_cache_start: Optional[int] = None

        try:
            # Try to read the length and satisfy initial zipfile reads with one
            # request.  Public PyPI supports this.
            self._optimistic_first_read()
        except HTTPError as e:
            if e.code != 501:  # Unsupported range
                raise
            # Just read the length
            self._head()

    def _optimistic_first_read(self) -> None:
        LOG.debug("_optimistic_first_read")
        # Optimistically read the last few KB, which can satisfy both finding
        # the length and the first couple of reads (2 bytes from the end and 22
        # bytes from the end).  This value was chosen looking at scipy and saves
        # another half-second for me.
        optimistic = 256000
        h = "bytes=-%d" % optimistic
        with self.get_range(self.url, h) as resp:
            match = CONTENT_RANGE_RE.match(resp.headers["Content-Range"])
            assert match is not None, resp.headers["Content-Range"]
            start, end, length = match.groups()
            self.length = int(length)
            LOG.debug(resp.headers["Content-Range"])
            self.end_cache = resp.read()
            self.end_cache_start = int(start)
            # print(type(self.end_cache), self.end_cache_start, url)
            assert self.end_cache_start >= 0

            # TODO verify ETag/Last-Modified don't change.
            # TODO if there was a redirect, save new url

    def _head(self) -> None:
        LOG.debug("_head")
        with self.get_range(self.url, None, method="HEAD") as resp:
            self.length = int(resp.headers["Content-Length"])
            self.end_cache_start = self.length

    def seek(self, pos: int, whence: int = 0) -> None:
        LOG.debug(f"seek {pos} {whence}")
        # TODO clamp/error
        if whence == 0:
            self.pos = pos
        elif whence == 1:
            self.pos += pos
        elif whence == 2:
            self.pos = self.length + pos
        else:
            raise ValueError(f"Invalid value for whence: {whence!r}")

    def tell(self) -> int:
        LOG.debug("tell")
        return self.pos

    def read(self, n: int = -1) -> bytes:
        LOG.debug(f"read {n} @ {self.length-self.pos}")
        if n == -1:
            n = self.length - self.pos
        if n == 0:
            return b""

        assert self.end_cache_start is not None
        p = self.pos - self.end_cache_start
        if p >= 0:
            LOG.debug(f"  satisfied from cache @ {p}")
            self.pos += n
            return self.end_cache[p : p + n]

        with self.get_range(
            self.url, "bytes=%d-%d" % (self.pos, self.pos + n - 1)
        ) as resp:
            data: bytes = resp.read()

        self.pos += n
        if len(data) != n:
            raise ValueError("Truncated read", len(data), n)

        return data

    def seekable(self) -> bool:
        return True
