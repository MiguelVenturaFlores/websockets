from websockets.binance import BinanceWebSocket
import sys

if __name__ == "__main__":

    pair = sys.argv[1]
    depth = int(sys.argv[2])

    while True:
        print(f"Starting new websocket {pair} with depth {depth}:")
        socket = BinanceWebSocket(pair, depth)
        socket.run()