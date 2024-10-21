from typing import Dict, Optional
class Check:
    @classmethod
    def http(klass, url: str, interval: str, timeout: Optional[str] = None, deregister: Optional[str] = None) -> Dict[str, str]: ...

