from websockets.binance import BinanceWebSocket
import threading
import time

def run_websocket(socket):
    """
    Function to run on parallel thread
    """
    print("start websocket...\n")
    socket.run()

if __name__ == "__main__":
    """
    This script simulates a close connection on the websocket.
    The goal is to test the resiliency of the BinanceWebSocket class when the connection is closed by the binance server.
    """

    while True:

        # initialize websocket object
        socket = BinanceWebSocket("btcusdt", 10)
        
        # start and run websocket on parallel thread
        ws_thread = threading.Thread(target=run_websocket, args=(socket,))
        ws_thread.start()

        # let the websocket gather some data on parallel thread
        print("start sleep...\n")
        time.sleep(10)

        # close connection
        print("close websocket...\n")
        socket.ws.close()

        # wait for websocket thread to finish properly
        ws_thread.join()