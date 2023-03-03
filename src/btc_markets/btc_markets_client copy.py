import base64
import hashlib
import hmac
import time
from typing import Any, Dict

#from src.btc_markets.client_base import ClientBase
from src.abstract.httpRequest.base_rest_api import BaseRestApi
from src.btc_markets.data_types import RESTRequest, RESTMethod
import src.btc_markets.btc_markets_constants as CONSTANTS


class BtcMarketsClient1(BaseRestApi):
    """
    Auth class required by btc_markets API
    Learn more at https://api.btcmarkets.net/doc/v3#section/Authentication/Authentication-process
    """
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    def get_path_from_url(url: str) -> str:
        return url.replace(CONSTANTS.REST_URLS, '')

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the server time and the signature to the request, required for authenticated interactions. It also adds
        the required parameter in the request header.
        :param request: the request to be configured for authenticated interaction
        """
        now = self._timestamp_in_milliseconds()
        sig = self.get_signature(
            request.method.name,
            self.get_path_from_url(request.url),
            now,
            request.data if request.method == RESTMethod.POST else {}
        )

        headers = self._generate_auth_headers(now, sig)
        if request.headers is not None:
            headers.update(request.headers)
        request.headers = headers

        return request

    def get_referral_code_headers(self):
        """
        Generates authentication headers required by BtcMarkets
        :return: a dictionary of auth headers
        """
        return {
            "referer": "Hummingbot"
        }

    def get_signature(
        self,
        method: str,
        path_url: str,
        nonce: int,
        data: Dict[str, Any] = None
    ):
        """
        Generates authentication signature and return it in a dictionary along with other inputs
        :return: a dictionary of request info including the request signature
        """
        data = data or {}

        if data is None or data == {}:
            payload = f"{method}/{path_url}{nonce}{''}"
        else:
            bjson = str(data)
            payload = f"{method}/{path_url}{nonce}{bjson}"

        return self._generate_signature(payload)

    def _generate_auth_headers(self, nonce: int, sig: str):
        """
        Generates HTTP headers
        """
        headers = {
            "Accept": "application/json",
            "Accept-Charset": "UTF-8",
            "Content-Type": "application/json",
            "BM-AUTH-APIKEY": self.api_key,
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
            base64.b64decode(self.secret_key), payload.encode("utf8"), digestmod=hashlib.sha512).digest())
        return digest.decode('utf8')
    
    async def _api_get(self, *args, **kwargs):
        kwargs["method"] = RESTMethod.GET
        return await self._api_request(*args, **kwargs)
    
    async def get_ticker(self, symbol):
        ticker_info = await self._api_get(
            method=RESTMethod.Get,
            path_url=CONSTANTS.TICKER_URL+f"/{symbol}/ticker",
            is_auth_required=False,
            limit_id=CONSTANTS.TICKER_URL
        )

        return ticker_info
    
    async def get_asset_balance(self, asset):
        account_info = await self._api_get(
            method=RESTMethod.Get,
            path_url=CONSTANTS.BALANCE_URL,
            is_auth_required=True,
            limit_id=CONSTANTS.BALANCE_URL
        )

        for balance_entry in account_info:
            if asset == balance_entry["assetName"]:
                return float(balance_entry["balance"])
            
        raise Exception("No balance for {asset} on btc markets")
    
    def combine_trading_pair(base: str, quote: str) -> str:
        trading_pair = f"{base}-{quote}"
        return trading_pair

    async def get_symbol_info(self, trading_pair):
        markets_info = await self._api_get(
            method=RESTMethod.Get,
            path_url=CONSTANTS.MARKETS_URL,
            is_auth_required=True,
            limit_id=CONSTANTS.MARKETS_URL
        )

        for trading_pair_info in markets_info:
            trade_pair = self.combine_trading_pair(
                base = trading_pair_info["baseAssetName"],
                quote = trading_pair_info["quoteAssetName"]
            )
            if trading_pair == trade_pair:
                return trading_pair_info

        raise Exception("Trading pair is not valid for btc markets")
    
    async def get_my_trades(self, symbol, startTime):
        trades_info = await self._api_get(
            method=RESTMethod.Get,
            path_url=CONSTANTS.TRADES_URL,
            params={
                "marketId": symbol
            },
            is_auth_required=True,
            limit_id=CONSTANTS.TRADES_URL
        )

        return trades_info

    def _timestamp_in_milliseconds(self) -> int:
        return int(self._time() * 1e3)

    def _time(self):
        return time.time()