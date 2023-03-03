import time

import pandas as pd
import datetime as dt

from src.btc_markets.btc_markets_client import BtcMarketsClient
from src.abstract.exchange_client_wrapper import ExchangeClientWrapper
import src.btc_markets.btc_markets_constants as CONSTANTS


class BTCMarketsClientWrapper(ExchangeClientWrapper):
    @staticmethod
    def create_instance(api_key, api_secret):
        btc_markets_client = BtcMarketsClient(api_key, api_secret, CONSTANTS.REST_URLS)
        return BTCMarketsClientWrapper(btc_markets_client)
    
    def usd_price_for(self, asset):
        stable_coins = ["AUD"]
        if asset in stable_coins:
            return 1
        try:
            res = self.client.get_ticker(symbol=f"{asset}-AUD")
            return float(res["lastPrice"])
        except Exception:
            raise Exception("we couldn't find price for this asset")
        
    def get_current_asset_balance(self, trading_pair):
        base_asset, quote_asset = self.symbol_info(trading_pair)
        df = pd.DataFrame(columns=["marketId"], data=[base_asset, quote_asset])
        df["price"] = df["marketId"].apply(lambda x: self.usd_price_for(x))
        df["balance"] = df["marketId"].apply(lambda x: self.get_asset_balance(x))
        df["quote_value"] = df["price"] * df["balance"]
        df.set_index("marketId", inplace=True, drop=True)
        base_asset_price = df.at[base_asset, "price"]
        quote_asset_price = df.at[quote_asset, "price"]
        return df, base_asset, quote_asset, base_asset_price, quote_asset_price
            
    def get_asset_balance(self, asset):
        res = self.client.get_balance()

        for balance_entry in res:
            if asset == balance_entry["assetName"]:
                return float(balance_entry["balance"])
            
        return 0.0
    
    def get_all_asset_balances(self):
        res = self.client.get_balance()
        if len(res):
            df = pd.DataFrame(res)
            for field in ["balance", "available"]:
                df[field] = df[field].apply(float)
            return df
        return pd.DataFrame()

    def symbol_info(self, trading_pair):
        res = self.client.list_asset()
        
        for r in res:
            if r["marketId"] == trading_pair:
                trading_pair_info = r
                return trading_pair_info["baseAssetName"], trading_pair_info["quoteAssetName"]
        raise Exception("Trading pair is not valid for btc markets")

    def get_trades(self, symbol, start_date, end_date=round(time.time() * 1000)):
        df_trades = pd.DataFrame()
        pd.set_option('max_columns', None)
        if start_date <= end_date:
            print(f"start:{start_date} end:{end_date}")
            try:
                df_res = pd.DataFrame(self.client.get_my_trades(symbol=symbol, startTime=start_date))
                
                df_res["timestamp"] = pd.to_datetime(df_res["timestamp"], format="%Y-%m-%dT%H:%M:%S.%f000Z")
                #df_res["timestamp"] = pd.to_numeric(df_res["timestamp"])
                #df_res["timestamp"] = df_res.apply(pd.to_numeric)
                df_res["timestamp"] = (df_res['timestamp'] - dt.datetime(1970,1,1)).dt.total_seconds().multiply(1000).round()
                start = df_res.iloc[-1]["timestamp"]#float(dateparse(df_res.iloc[-1]["timestamp"]).timestamp())
                # if len(df_res) == 0 or df_res.empty:
                #     break
                # elif len(df_trades) == 0:
                #     start_date = start + 1000
                #     df_trades = df_res[df_res["timestamp"] <= end_date].sort_values(
                #         "timestamp", ascending=False, ignore_index=True
                #     )
                # else:
                #     start_date = start + 1000
                #     df_res = df_res[df_res["timestamp"] <= end_date].sort_values(
                #         "timestamp", ascending=False, ignore_index=True
                #     )
                #     df_trades = pd.concat([df_res, df_trades])
                df_res = df_res[df_res["timestamp"] <= end_date].sort_values(
                        "timestamp", ascending=False, ignore_index=True
                    )
                df_trades = pd.concat([df_res, df_trades])
            except Exception as err:
                if err.code == -1003:
                    print("exceed limit rate sleep for 1min 💤")
                    time.sleep(61)
                else:
                    print(f"error connecting to the exchange {err}")

        if len(df_trades) == 0:
            raise Exception(f"We couldn't fetch trades for this trading pair {symbol}")
        df_trades["date_time"] = pd.to_datetime(df_trades["timestamp"], unit="ms")

        df_trades.rename(
            columns={
                "amount": "qty",
                "valueInQuoteAsset": "commissionAssetUsdPrice",
                "fee": "commission"
            },
            inplace=True,
        )

        df_trades["quoteQty"] = 1
        df_trades["commissionAsset"] = "AUD"

        df_trades.set_index("id", inplace=True, drop=True)
        #float_columns = ["price", "amount", "valueInQuoteAsset"]
        float_columns = ["price", "qty", "quoteQty", "commission"]
        df_trades[float_columns] = df_trades[float_columns].apply(pd.to_numeric)
        return self.format_data(df_trades)

    def format_data(self, df):
        df.loc[(df["side"] == "Ask"), "side"] = "sell"
        df.loc[(df["side"] == "Bid"), "side"] = "buy"

        df = df.astype(
            {
                "price": "float64",
                "qty": "float64",
                "quoteQty": "float64",
                "commission": "float64",
                "commissionAssetUsdPrice": "float64",
            }
        )
        return df[
            [
                "price",
                "qty",
                "quoteQty",
                "commission",
                "commissionAsset",
                "side",
                "commissionAssetUsdPrice",
                "date_time",
            ]
        ]
