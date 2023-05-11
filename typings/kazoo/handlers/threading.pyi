import socket
from kazoo.handlers import utils
from typing import Any

class AsyncResult(utils.AsyncResult):
    ...

class SequentialThreadingHandler:
    def select(self, *args: Any, **kwargs: Any) -> Any:
        ...

    def create_connection(self, *args: Any, **kwargs: Any) -> socket.socket:
        ...
