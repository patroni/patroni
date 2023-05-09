from typing import Any, Optional
class Timeout:
    DEFAULT_TIMEOUT: Any
    def __init__(self, total: Optional[float] = None, connect: Optional[float] = None, read: Optional[float] = None) -> None: ...
