import json
import os
import datetime
import websocket
import requests

UPDATES_PER_FILE = 1024
PRINT_FREQUENCY = 256

class BinanceWebSocket:

    def __init__(self, symbol, depth):
        self.symbol = symbol
        self.depth = depth
        self.synced = False
        self.last_u = None
        self.lastUpdateId = None

        # initialize websocket
        websocket.enableTrace(True)        
        self.ws = websocket.WebSocketApp(
            f"wss://stream.binance.com:9443/ws/{self.symbol}@depth@100ms/{self.symbol}@trade/{self.symbol}@aggTrade",
            on_message = lambda ws,msg: self.on_message(ws, msg),
            on_error   = lambda ws,msg: self.on_error(ws, msg),
            on_close   = lambda ws:     self.on_close(ws),
            on_open    = lambda ws:     self.on_open(ws)
            )

        # Initialize data structures
        self.orderbook_updates = []
        self.trades = []
        self.aggTrades = []

        # Initialize file paths
        self.path = self.get_path()
        self.orderbook_path = f"{self.path}/orderbook/"
        self.trades_path = f"{self.path}/trades/"
        self.aggTrades_path = f"{self.path}/aggTrades/"

        os.makedirs(self.orderbook_path, exist_ok=True)
        os.makedirs(self.trades_path, exist_ok=True)
        os.makedirs(self.aggTrades_path, exist_ok=True)

    def get_path(self):
        now = datetime.datetime.now()
        date = now.strftime('%Y%m%d')
        path = f'data/{self.symbol}/{date}/'
        os.makedirs(path, exist_ok=True)
        take = f"take_{len(os.listdir(path)) + 1}"
        return f"{path}{take}"

    def get_orderbook(self):
        now = datetime.datetime.now()
        timestamp = now.strftime('%H%M%S%f')
        path = f'{self.orderbook_path}{timestamp}_ob.json'
        print(f"Getting order book: {timestamp}")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as file:
            response = requests.get(f'https://api.binance.com/api/v3/depth?symbol={self.symbol.upper()}&limit={self.depth}')
            self.lastUpdateId = response.json()["lastUpdateId"] + 1
            json.dump(response.json(), file)

    def append_data(self, data, data_list, data_type, path_template):
        data_list.append(data)

        if len(data_list) % PRINT_FREQUENCY == 0:
            print(f'Current {data_type} queue: {len(data_list)}')

        if len(data_list) == UPDATES_PER_FILE:
            now = datetime.datetime.now()
            timestamp = now.strftime('%H%M%S%f')
            path = f'{path_template}{timestamp}.json'
            print(f"{data_type} updating time: {timestamp}")
            with open(path, 'w') as file:
                json.dump(data_list, file)
            data_list.clear()

    def append_update(self, data):
        if not self.synced:
            now = datetime.datetime.now()
            timestamp = now.strftime('%H%M%S%f')
            if (data["U"] <= self.lastUpdateId) & (self.lastUpdateId <= data["u"]):
                self.synced = True
                self.last_u = data["u"]
                self.append_data(data, self.orderbook_updates, "orderbook", self.orderbook_path)
                print(f"First update event time: {timestamp}")
                return True
            else:
                print(f"Dropping event time: {timestamp}")

        if self.synced:
            if data['U'] != (self.last_u + 1):
                print("Out of sync. Stopping process.")
                raise ValueError(f"Value does not match expected value: U: {data['U']} != last_u+1: {self.last_u + 1}")

            self.last_u = data['u']
            self.append_data(data, self.orderbook_updates, "orderbook", self.orderbook_path)

    def append_trade(self, data):
        self.append_data(data, self.trades, "trades", self.trades_path)

    def append_aggTrade(self, data):
        self.append_data(data, self.aggTrades, "aggTrades", self.aggTrades_path)

    def on_message(self, ws, message):
        
        data = json.loads(message)

        if data["e"] == "depthUpdate":
            self.append_update(data)
        elif data["e"] == "trade":
            self.append_trade(data)
        elif data["e"] == "aggTrade":
            self.append_aggTrade(data)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("Connection closed")
        self.get_orderbook()
        
    def on_open(self, ws):
        print("Connection opened")
        self.get_orderbook()

    def run(self):
        self.ws.run_forever()

