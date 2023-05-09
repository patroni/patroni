from typing import Optional, Union, Dict, List
def make_headers(
    keep_alive: Optional[bool] = None,
    accept_encoding: Union[bool, List[str], str, None] = None,
    user_agent: Optional[str] = None,
    basic_auth: Optional[str] = None,
    proxy_basic_auth: Optional[str] = None,
    disable_cache: Optional[bool] = None,
) -> Dict[str, str]: ...
