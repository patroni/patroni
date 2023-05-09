from typing import Any, Callable, Collection, Optional
from .node import TCPNode
from .syncobj import SyncObj
from .tcp_connection import CONNECTION_STATE
__all__ = ['CONNECTION_STATE', 'TCPTransport']
class Transport:
    def setOnUtilityMessageCallback(self, message: str, callback: Callable[[Any, Callable[..., Any]], Any]) -> None: ...
class TCPTransport(Transport):
    def __init__(self, syncObj: SyncObj, selfNode: Optional[TCPNode], otherNodes: Collection[TCPNode]) -> None: ...
    def _connectIfNecessarySingle(self, node: TCPNode) -> bool: ...
