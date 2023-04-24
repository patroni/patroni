import io
from typing import Any, Iterator, Optional, Union
from ._collections import HTTPHeaderDict
from .connection import HTTPConnection
class HTTPResponse(io.IOBase):
    headers: HTTPHeaderDict
    status: int
    reason: Optional[str]
    def release_conn(self) -> None: ...
    @property
    def data(self) -> Union[bytes, Any]: ...
    @property
    def connection(self) -> Optional[HTTPConnection]: ...
    def read_chunked(self, amt: Optional[int] = None, decode_content: Optional[bool] = None) -> Iterator[bytes]: ...
