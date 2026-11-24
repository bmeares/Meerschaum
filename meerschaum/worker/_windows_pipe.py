"""Duplex byte-stream client for the Rust worker host's Windows named pipe."""

import _overlapped
import _winapi
from asyncio.windows_utils import PipeHandle

CHUNK_BYTES = 1024 * 1024


class WorkerPipe(PipeHandle):
    """Give the waiting reader and the dispatcher's writes independent completion events."""

    def read(self, size: int) -> bytes:
        operation = _overlapped.Overlapped()
        try:
            operation.ReadFile(self.fileno(), min(size, CHUNK_BYTES))
            return operation.getresult(True)
        except BrokenPipeError:
            return b''

    def write(self, data: bytes) -> int:
        remaining = memoryview(data)
        while remaining:
            operation = _overlapped.Overlapped()
            operation.WriteFile(self.fileno(), remaining[:CHUNK_BYTES])
            count = operation.getresult(True)
            if not count:
                raise BrokenPipeError('The worker host closed the named pipe.')
            remaining = remaining[count:]
        return len(data)

    def flush(self) -> None:
        """Writes have already completed when write() returns."""


def connect_pipe(address: str) -> WorkerPipe:
    """Open the host's named pipe for concurrent reading and writing."""
    return WorkerPipe(_winapi.CreateFile(
        address, _winapi.GENERIC_READ | _winapi.GENERIC_WRITE, 0, 0,
        _winapi.OPEN_EXISTING, _winapi.FILE_FLAG_OVERLAPPED, 0,
    ))
