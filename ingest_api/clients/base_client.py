from abc import ABC, abstractmethod

class BaseAPIClient(ABC):
    """BASE CLASS FOR API CLIENT"""

    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url

    @abstractmethod
    def fetch(self, endpoint, params, extra_fn):
        pass