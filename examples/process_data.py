from websockets.data_processing import *

PAIRS = [
    "adausdt",
    "bnbusdt",
    "btcusdt",
    "dogeusdt",
    "ethusdt",
    "solusdt",
    "trxusdt",
    "xrpusdt"
]

if __name__ == "__main__":

    # input
    depth = 5
    base_path = "."
    DATES = ["20231115", "20231116", "20231117"]

    for date in DATES:

        for pair in PAIRS:

            print(f"\n\nProcessing pair {pair}...")

            # process LOB
            process_data(depth, pair, date, base_path)

            # validate LOB
            snapshots_path = f"raw_data/{pair}/{date}/take_1/orderbook/"
            true_ob = get_snapshots(snapshots_path, depth)
            ob_path = f"preprocessed_data/{pair}/{date}/orderbook/take_1.csv"
            computed_ob = pd.read_csv(ob_path).drop(columns=["Unnamed: 0"])
            compare_orderbook(true_ob, computed_ob)

            # process trades
            process_trades(pair, date, base_path=".", trades="aggTrades")
            process_trades(pair, date, base_path=".", trades="trades")