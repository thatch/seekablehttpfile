import logging
import re
from urllib.request import Request, urlopen

LOG = logging.getLogger(__name__)

CONTENT_RANGE_RE = re.compile(r"bytes (\d+)-(\d+)/(\d+)")


class SeekableHttpFile:
    def __init__(self, url: str) -> None:
        self.url = url
        self.pos = 0
        self.length = -1
        LOG.debug("head")
        # Optimistically read the last few KB, which can satisfy both finding
        # the length and the first couple of reads (2 bytes from the end and 22
        # bytes from the end).  This value was chosen looking at scipy and saves
        # another half-second for me.
        optimistic = 256000
        h = "bytes=-%d" % optimistic
        with urlopen(Request(url, headers={"Range": h})) as resp:
            match = CONTENT_RANGE_RE.match(resp.headers["Content-Range"])
            assert match is not None
            start, end, length = match.groups()
            self.length = int(length)
            LOG.debug(resp.headers["Content-Range"])
            self.end_cache: bytes = resp.read()
            self.end_cache_start = int(start)
            # print(type(self.end_cache), self.end_cache_start, url)
            assert self.end_cache_start >= 0

            # TODO verify ETag/Last-Modified don't change.

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

        p = self.pos - self.end_cache_start
        if p >= 0:
            LOG.debug(f"  satisfied from cache @ {p}")
            self.pos += n
            return self.end_cache[p : p + n]

        with urlopen(
            Request(
                self.url,
                headers={"Range": "bytes=%d-%d" % (self.pos, self.pos + n - 1)},
            )
        ) as resp:
            data: bytes = resp.read()

        self.pos += n
        if len(data) != n:
            raise ValueError("Truncated read", len(data), n)

        return data

    def seekable(self) -> bool:
        return True
