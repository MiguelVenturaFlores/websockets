from websockets.binance import BinanceWebSocket

if __name__ == "__main__":
    socket = BinanceWebSocket("BTCUSDT", 100)
    socket.run()