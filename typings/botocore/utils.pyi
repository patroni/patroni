from typing import Any, Callable, Dict, Optional
DEFAULT_METADATA_SERVICE_TIMEOUT = 1
METADATA_BASE_URL = 'http://169.254.169.254/'
class AWSResponse:
    status_code: int
    @property
    def text(self) -> str: ...
class IMDSFetcher:
    def __init__(self, timeout: float = DEFAULT_METADATA_SERVICE_TIMEOUT, num_attempts: int = 1, base_url: str = METADATA_BASE_URL, env: Optional[Dict[str, str]] = None, user_agent: Optional[str] = None, config: Optional[Dict[str, Any]] = None) -> None: ...
    def _fetch_metadata_token(self) -> Optional[str]: ...:
    def _get_request(self, url_path: str, retry_func: Optional[Callable[[AWSResponse], bool]] = None, token: Optional[str] = None) -> AWSResponse: ...
