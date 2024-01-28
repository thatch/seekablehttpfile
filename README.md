# seekablehttpfile

This project provides a file-like object that fetches parts of a file using HTTP
range requests as needed.

```pycon
>>> from seekablehttpfile import SeekableHttpFile
>>> from zipfile import ZipFile

>>> f = SeekableHttpFile("https://files.pythonhosted.org/packages/cb/90/599c79a248dcae6935331113649de5d75427e320efde21b583648b498584/tensorflow_intel-2.14.0-cp310-cp310-win_amd64.whl")  # 284MB
>>> # use as normal, for example with ZipFile
>>> z = ZipFile(f)
>>> len(z.namelist())
9414
>>> # find out how much we actually read
>>> f.stats
{'num_requests': 4, 'optimistic_bytes_read': 256000, 'lazy_bytes_read': 1078669, 'satisfied_from_cache': 2}

```

# Version Compatibility

Users of this library should be able to use Python 3.7 or above.  This is
validated by tests, and is the version encoded in `Requires-Python`.

Development requires Python 3.12 (or 3.13), realistically.  CI validates that it
works on 3.10 and above, but you will get less checking in flake8.  Expect older
versions to be dropped as soon as there is a version of mypy, black, etc that is
not simultaneously compatible with all development versions.

Notably, this means that while this project might function (and pass tests) on
Python 3.7, it's likely that you can't typecheck against it.

# License

seekablehttpfile is copyright [Tim Hatch](https://timhatch.com/), and licensed under
the MIT license.  I am providing code in this repository to you under an open
source license.  This is my personal repository; the license you receive to
my code is from me and not from my employer. See the `LICENSE` file for details.
