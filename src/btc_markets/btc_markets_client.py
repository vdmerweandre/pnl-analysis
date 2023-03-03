import base64
import hashlib
import hmac
import time

from src.abstract.httpRequest.base_rest_api import BaseRestApi
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
