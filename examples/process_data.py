from websockets.data_processing import *

if __name__ == "__main__":

    # input
    depth = 2
    base_path = "."
    pair = "btcusdt"
    date = "20231113"

    process_data(depth, pair, date, base_path)