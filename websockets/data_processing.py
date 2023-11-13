import json
import os
import itertools
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# get files
def get_files(path):
    files = os.listdir(path)
    snapshot_files = [f"{path}{f}" for f in files if "ob" in f]
    updates_files = [f"{path}{f}" for f in files if "ob" not in f]
    snapshot_files.sort()
    updates_files.sort()
    return snapshot_files, updates_files

# get orderbook
def get_orderbook(snapshot_file):
    f = open(snapshot_file)
    orderbook_raw = json.load(f)
    orderbook = {
        "u" : orderbook_raw["lastUpdateId"],
        "E" : 0,
        "bids" : {float(b[0]) : float(b[1]) for b in orderbook_raw["bids"]},
        "asks" : {float(a[0]) : float(a[1]) for a in orderbook_raw["asks"]}
    }
    return orderbook

# get updates
def get_updates(updates_file):
    f = open(updates_file)
    updates_raw = json.load(f)
    updates = []
    for u in updates_raw:
        updates.append({
            "u" : u["u"],
            "E" : u["E"],
            "bids" : {float(b[0]) : float(b[1]) for b in u["b"]},
            "asks" : {float(a[0]) : float(a[1]) for a in u["a"]}
            })
    return updates

# update orderbook
def update_side(updates, orderbook, side):
    for u in updates:
        if (updates[u] != 0.0):
            orderbook[u] = updates[u]
        elif (u in orderbook):
            del orderbook[u]
    orderbook = dict(sorted(orderbook.items(), reverse=(side=="bids")))
    return orderbook

def update_orderbook(updates, orderbook):
    orderbook["u"] = updates["u"]
    orderbook["E"] = updates["E"]
    for side in ["bids", "asks"]:
        orderbook[side] = update_side(updates[side], orderbook[side], side)
    return orderbook

# create and update pandas dataframe
def get_processed_orderbook(rows, depth):
    columns = get_columns(depth)
    df = pd.DataFrame(rows, columns=columns)
    return df

def get_columns(depth):
    column_generator = lambda side : [col for cols in [[f"{side}{i+1}", f"v{side}{i+1}"] for i in range(depth)] for col in cols]
    columns = ["u"] + ["E"] + column_generator("b") + column_generator("a")
    return columns

def get_row(orderbook, depth):
    asks = dict(itertools.islice(orderbook["asks"].items(), depth))
    bids = dict(itertools.islice(orderbook["bids"].items(), depth))
    list_generator = lambda side : [col for cols in [[s, side[s]] for s in side] for col in cols]
    row = [orderbook["u"], orderbook["E"]] + list_generator(bids) + list_generator(asks)
    return row

def get_processed_orderbook(path, depth):

    # initialize orderbook
    snapshot_files, updates_files = get_files(path)
    orderbook = get_orderbook(snapshot_files[0])

    print(f"Total updates files= {len(updates_files)}")

    # store all orderbook updates
    rows = []
    rows.append(get_row(orderbook, depth)) # first update
    for file in tqdm(updates_files):
        updates = get_updates(file)
        for u in updates:
            orderbook = update_orderbook(u, orderbook)
            rows.append(get_row(orderbook, depth))

    # store data as dataframe
    columns = get_columns(depth)
    df = pd.DataFrame(rows, columns=columns)
    return df

def process_data(depth, pair, date, base_path="."):

    # define input path
    input_path = f"{base_path}/data/{pair}/{date}"

    # define output path
    output_path = f"{base_path}/preprocessed_data/{pair}/{date}/orderbook"
    Path(output_path).mkdir(parents=True, exist_ok=True)

    # get all takes paths
    takes = [x[0] for x in os.walk(input_path) if len(x[0].split("/")) == (len(input_path.split("/")) + 1)]

    # store data for each take
    for take_path in takes:

        t = take_path.split("/")[-1]
        print(f"Processing {t}...")
        path = f"{take_path}/orderbook/"
        orderbook = get_processed_orderbook(path, depth)

        print("Processing finished. Storing csv...")
        orderbook.to_csv(f"{output_path}/{t}.csv")