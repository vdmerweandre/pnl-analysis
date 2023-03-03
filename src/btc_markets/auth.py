from abc import ABC, abstractmethod

from src.btc_markets.data_types import RESTRequest


class AuthBase(ABC):

    @abstractmethod
    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        ...