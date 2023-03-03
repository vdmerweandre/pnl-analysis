import base64
import hashlib
import hmac
import time
from typing import Any, Dict

#from src.btc_markets.client_base import ClientBase
from src.abstract.httpRequest.base_rest_api import BaseRestApi
from src.btc_markets.data_types import RESTRequest, RESTMethod
import src.btc_markets.btc_markets_constants as CONSTANTS


class BtcMarketsClient(BaseRestApi):
    """
    Auth class required by btc_markets API
    Learn more at https://api.btcmarkets.net/doc/v3#section/Authentication/Authentication-process
    """
    def __init__(self, api_key: str, secret_key: str, url):
        super().__init__(key=api_key, secret=secret_key, url=url)

    def get_path_from_url(url: str) -> str:
        return url.replace(CONSTANTS.REST_URLS, '')
    
    @staticmethod
    def check_response_data(response_data):
        if response_data.status_code == 200:
            try:
                data = response_data.json()
            except ValueError:
                raise Exception(-1, response_data.content)
            else:
                if data and "code" in data:
                    if data.get("code") == 0:
                        if "data" in data:
                            return data["data"]
                        else:
                            return data
                    else:
                        raise Exception(response_data.status_code, response_data.text)
                else:
                    return data
        else:
            raise Exception(response_data.status_code, response_data.text)

    def get_my_trades(self, symbol, startTime, **kwargs):
        params = {"marketId": symbol, "limit": 100}
        if kwargs:
            params.update(kwargs)
        header_meta = {"path": f"{CONSTANTS.TRADES_URL}"}
        return self._request(
            "GET",
            CONSTANTS.TRADES_URL,
            params=params,
            header_meta=header_meta,
        )
    
    def get_balance(self, **kwargs):
        params = {}
        if kwargs:
            params.update(kwargs)
        header_meta = {"path": f"{CONSTANTS.BALANCE_URL}"}
        return self._request(
            "GET",
            CONSTANTS.BALANCE_URL,
            params=params,
            header_meta=header_meta,
        )
            
    # def list_current_orders(self, **kwargs):
    #     params = {}
    #     if kwargs:
    #         params.update(kwargs)
    #     header_meta = {"path": "order/hist/current"}
    #     account_category = "cash" if not params["account_category"] else params["account_category"]
    #     return self._request(
    #         "GET",
    #         f"{self.group}/api/pro/v1/{account_category}/order/hist/current",
    #         params=params,
    #         header_meta=header_meta,
    #     )

    def get_ticker(self, symbol, **kwargs):
        params = {}
        if kwargs:
            params.update(kwargs)
        path = CONSTANTS.TICKER_URL+f"/{symbol}/ticker"

        return self._request("GET", path, params=params, auth=False)
    
    def list_asset(self, **kwargs):
        params = {}
        if kwargs:
            params.update(kwargs)
        return self._request("GET", CONSTANTS.MARKETS_URL, params=params, auth=False)

    # def list_all_product(self, **kwargs):
    #     params = {}
    #     if kwargs:
    #         params.update(kwargs)
    #     return self._request("GET", "api/pro/v1/products", params=params, auth=False)

    def _headers(self, header_meta):
        now_time = self._timestamp_in_milliseconds()
        path = header_meta["path"]

        payload = f"GET/{path}{now_time}{''}"
        signature = self._generate_signature(payload)

        return self._generate_auth_headers(now_time, signature)

    def _generate_auth_headers(self, nonce: int, sig: str):
        """
        Generates HTTP headers
        """
        headers = {
            "Accept": "application/json",
            "Accept-Charset": "UTF-8",
            "Content-Type": "application/json",
            "BM-AUTH-APIKEY": self.key,
            "BM-AUTH-TIMESTAMP": str(nonce),
            "BM-AUTH-SIGNATURE": sig
        }

        return headers

    def _generate_signature(self, payload: str) -> str:
        """
        Generates a presigned signature
        :return: a signature of auth params
        """
        digest = base64.b64encode(hmac.new(
            base64.b64decode(self.secret), payload.encode("utf8"), digestmod=hashlib.sha512).digest())
        return digest.decode('utf8')
    
    def _timestamp_in_milliseconds(self) -> int:
        return int(self._time() * 1e3)

    def _time(self):
        return time.time()

    # async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
    #     """
    #     Adds the server time and the signature to the request, required for authenticated interactions. It also adds
    #     the required parameter in the request header.
    #     :param request: the request to be configured for authenticated interaction
    #     """
    #     now = self._timestamp_in_milliseconds()
    #     sig = self.get_signature(
    #         request.method.name,
    #         self.get_path_from_url(request.url),
    #         now,
    #         request.data if request.method == RESTMethod.POST else {}
    #     )

    #     headers = self._generate_auth_headers(now, sig)
    #     if request.headers is not None:
    #         headers.update(request.headers)
    #     request.headers = headers

    #     return request

    # def get_signature(
    #     self,
    #     method: str,
    #     path_url: str,
    #     nonce: int,
    #     data: Dict[str, Any] = None
    # ):
    #     """
    #     Generates authentication signature and return it in a dictionary along with other inputs
    #     :return: a dictionary of request info including the request signature
    #     """
    #     data = data or {}

    #     if data is None or data == {}:
    #         payload = f"{method}/{path_url}{nonce}{''}"
    #     else:
    #         bjson = str(data)
    #         payload = f"{method}/{path_url}{nonce}{bjson}"

    #     return self._generate_signature(payload)

    # def _generate_auth_headers(self, nonce: int, sig: str):
    #     """
    #     Generates HTTP headers
    #     """
    #     headers = {
    #         "Accept": "application/json",
    #         "Accept-Charset": "UTF-8",
    #         "Content-Type": "application/json",
    #         "BM-AUTH-APIKEY": self.api_key,
    #         "BM-AUTH-TIMESTAMP": str(nonce),
    #         "BM-AUTH-SIGNATURE": sig
    #     }

    #     return headers

    # def _generate_signature(self, payload: str) -> str:
    #     """
    #     Generates a presigned signature
    #     :return: a signature of auth params
    #     """
    #     digest = base64.b64encode(hmac.new(
    #         base64.b64decode(self.secret_key), payload.encode("utf8"), digestmod=hashlib.sha512).digest())
    #     return digest.decode('utf8')
    
    # async def _api_get(self, *args, **kwargs):
    #     kwargs["method"] = RESTMethod.GET
    #     return await self._api_request(*args, **kwargs)
    
    # async def get_my_trades(self, symbol, startTime):
    #     trades_info = await self._api_get(
    #         method=RESTMethod.Get,
    #         path_url=CONSTANTS.TRADES_URL,
    #         params={
    #             "marketId": symbol
    #         },
    #         is_auth_required=True,
    #         limit_id=CONSTANTS.TRADES_URL
    #     )

    #     return trades_info

    # def _timestamp_in_milliseconds(self) -> int:
    #     return int(self._time() * 1e3)

    # def _time(self):
    #     return time.time()