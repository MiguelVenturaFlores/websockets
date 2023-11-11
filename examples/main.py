from websockets.binance import BinanceWebSocket

if __name__ == "__main__":

    while True:
        print("\n\n\nStarting new websocket:")
        socket = BinanceWebSocket("btcusdt", 10)
        socket.run()