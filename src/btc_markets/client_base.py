from abc import ABC
from typing import Optional, Dict, Any

from src.btc_markets.data_types import RESTMethod
from src.btc_markets.rest_assistant import RESTAssistant
import src.btc_markets.btc_markets_constants as CONSTANTS
from src.btc_markets.auth import AuthBase
from src.btc_markets.connections_factory import ConnectionsFactory


class ClientBase(ABC):

    def __init__(
        self,
        auth: Optional[AuthBase] = None,
    ):
        self._auth = auth
        self._connections_factory = ConnectionsFactory()


    async def _api_get(self, *args, **kwargs):
        kwargs["method"] = RESTMethod.GET
        return await self._api_request(*args, **kwargs)

    async def _api_post(self, *args, **kwargs):
        kwargs["method"] = RESTMethod.POST
        return await self._api_request(*args, **kwargs)

    async def _api_put(self, *args, **kwargs):
        kwargs["method"] = RESTMethod.PUT
        return await self._api_request(*args, **kwargs)

    async def _api_delete(self, *args, **kwargs):
        kwargs["method"] = RESTMethod.DELETE
        return await self._api_request(*args, **kwargs)

    async def _api_request_url(self, path_url: str, is_auth_required: bool = False) -> str:
        if is_auth_required:
            url = CONSTANTS.REST_URLS
        else:
            url = CONSTANTS.REST_URLS

        return url

    async def _api_request(
            self,
            path_url,
            overwrite_url: Optional[str] = None,
            method: RESTMethod = RESTMethod.GET,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
            is_auth_required: bool = False,
            return_err: bool = False,
            limit_id: Optional[str] = None,
            **kwargs,
    ) -> Dict[str, Any]:

        rest_assistant = await self.get_rest_assistant()

        url = overwrite_url or await self._api_request_url(path_url=path_url, is_auth_required=is_auth_required)

        try:
            request_result = await rest_assistant.execute_request(
                url=url,
                params=params,
                data=data,
                method=method,
                is_auth_required=is_auth_required,
                return_err=return_err,
                throttler_limit_id=limit_id if limit_id else path_url,
            )

            return request_result
        except IOError as request_exception:
            raise request_exception
        
    async def get_rest_assistant(self) -> RESTAssistant:
        connection = await self._connections_factory.get_rest_connection()
        assistant = RESTAssistant(
            connection=connection,
            auth=self._auth
        )
        return assistant
