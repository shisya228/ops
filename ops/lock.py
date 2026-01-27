from __future__ import annotations

import os
import time
from pathlib import Path
from typing import IO

from .errors import IOError

try:
    import fcntl  # type: ignore
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore

try:
    import msvcrt  # type: ignore
except ImportError:  # pragma: no cover
    msvcrt = None  # type: ignore


class FileLock:
    def __init__(self, path: Path, timeout: float | None = 10.0, poll_interval: float = 0.1):
        self.path = path
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._handle: IO[str] | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        self._handle = handle
        start = time.monotonic()
        while True:
            try:
                self._lock(handle)
                return
            except BlockingIOError:
                if self.timeout is not None and (time.monotonic() - start) >= self.timeout:
                    raise IOError(f"Timeout acquiring lock: {self.path}")
                time.sleep(self.poll_interval)

    def release(self) -> None:
        if not self._handle:
            return
        try:
            self._unlock(self._handle)
        finally:
            self._handle.close()
            self._handle = None

    def _lock(self, handle: IO[str]) -> None:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        if msvcrt is not None:
            fd = handle.fileno()
            try:
                os.lseek(fd, 0, os.SEEK_SET)
            except OSError:
                pass
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise BlockingIOError(str(exc)) from exc
            return
        raise IOError("No supported file locking backend available")

    def _unlock(self, handle: IO[str]) -> None:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            return
        if msvcrt is not None:
            fd = handle.fileno()
            try:
                os.lseek(fd, 0, os.SEEK_SET)
            except OSError:
                pass
            try:
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
            return

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
