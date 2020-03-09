from typing import Any, Dict, Optional
from collections import namedtuple


class BaseStitchApi:

    SendRequest = namedtuple('SendRequest', 'endpoint method payload')

    @classmethod
    def send_request(cls, endpoint: str, method: str = 'get',
                     payload: Optional[Dict[str, Any]] = None) -> namedtuple:
        return cls.SendRequest(endpoint, method, payload)
