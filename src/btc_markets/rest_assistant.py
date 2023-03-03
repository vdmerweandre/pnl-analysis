import json
from asyncio import wait_for
from copy import deepcopy
from typing import Any, Dict, List, Optional, Union

from src.btc_markets.auth import AuthBase
from src.btc_markets.data_types import RESTMethod, RESTRequest, RESTResponse
from src.btc_markets.rest_connection import RESTConnection


class RESTAssistant:
    """A helper class to contain all REST-related logic.

    The class can be injected with additional functionality by passing a list of objects inheriting from
    the `RESTPreProcessorBase` and `RESTPostProcessorBase` classes. The pre-processors are applied to a request
    before it is sent out, while the post-processors are applied to a response before it is returned to the caller.
    """
    def __init__(
        self,
        connection: RESTConnection,
        auth: Optional[AuthBase] = None,
    ):
        self._connection = connection
        self._auth = auth

    async def execute_request(
            self,
            url: str,
            throttler_limit_id: str,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
            method: RESTMethod = RESTMethod.GET,
            is_auth_required: bool = False,
            return_err: bool = False,
            timeout: Optional[float] = None,
            headers: Optional[Dict[str, Any]] = None) -> Union[str, Dict[str, Any]]:

        headers = headers or {}

        local_headers = {
            "Content-Type": ("application/json" if method != RESTMethod.GET else "application/x-www-form-urlencoded")}
        local_headers.update(headers)

        data = json.dumps(data) if data is not None else data

        request = RESTRequest(
            method=method,
            url=url,
            params=params,
            data=data,
            headers=local_headers,
            is_auth_required=is_auth_required,
            throttler_limit_id=throttler_limit_id
        )

        response = await self.call(request=request, timeout=timeout)

        if 400 <= response.status:
            if return_err:
                error_response = await response.json()
                return error_response
            else:
                error_response = await response.text()
                error_text = "N/A" if "<html" in error_response else error_response
                raise IOError(f"Error executing request {method.name} {url}. HTTP status is {response.status}. "
                                f"Error: {error_text}")
        return await response.json()

    async def call(self, request: RESTRequest, timeout: Optional[float] = None) -> RESTResponse:
        request = deepcopy(request)
        request = await self._authenticate(request)
        resp = await wait_for(self._connection.call(request), timeout)
        return resp

    async def _authenticate(self, request: RESTRequest):
        if self._auth is not None and request.is_auth_required:
            request = await self._auth.rest_authenticate(request)
        return request
