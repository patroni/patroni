import socket
from typing import Any, Union, Tuple
class ConnectionHandler:
    _socket: socket.socket
    def _connect(self, *args: Any) -> Tuple[Union[int, float], Union[int, float]]:
        ...
