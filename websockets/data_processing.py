import json
import os
import itertools
import pandas as pd
import numpy as np
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

    # get data from file
    with open(snapshot_file, mode='r') as f:
        orderbook_raw = json.load(f)
    
    # parse orderbook
    orderbook = {
        "u" : orderbook_raw["lastUpdateId"],
        "E" : 0,
        "bids" : {float(b[0]) : float(b[1]) for b in orderbook_raw["bids"]},
        "asks" : {float(a[0]) : float(a[1]) for a in orderbook_raw["asks"]}
    }

    return orderbook

# get updates
def get_updates(updates_file):

    # get data from file
    with open(updates_file, mode='r') as f:
        updates_raw = json.load(f)

    # parse all updates
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
    print(f"Total updates files= {len(updates_files)}")

    # initialize df
    columns = get_columns(depth)
    df = pd.DataFrame(np.zeros((len(updates_files)*1024 + 1, len(columns))), columns=columns)

    # first update
    orderbook = get_orderbook(snapshot_files[0])
    df.iloc[0] = get_row(orderbook, depth)

    # add all updates
    i = 1
    for file in tqdm(updates_files):
        updates = get_updates(file)
        for u in updates:
            orderbook = update_orderbook(u, orderbook)
            df.iloc[i] = get_row(orderbook, depth)
            i+=1

    return df

def process_data(depth, pair, date, base_path="."):

    # define input path
    input_path = f"{base_path}/raw_data/{pair}/{date}"

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

# data validation
def get_snapshots(path, depth):
    snapshot_files, _ = get_files(path)
    columns = get_columns(depth)
    snapshots = pd.DataFrame(np.zeros((len(snapshot_files), len(columns))), columns=columns)
    for i, file in enumerate(snapshot_files):
        orderbook = get_orderbook(file)
        snapshots.iloc[i] = get_row(orderbook, depth)
    return snapshots

def compare_orderbook(true_ob, computed_ob):

    # intersect values of u found on both dataframmes
    A = true_ob["u"].tolist()
    B = computed_ob[computed_ob["u"].isin(A)]["u"].tolist()
    u_intersect = list(set(A) & set(B))

    # check if u_intersect is empty:
    if not u_intersect:
        print("No values to compare.")
    else:
        print(f"Comparing against {len(u_intersect)} snapshots: {u_intersect}")

    # comparing dataframes
    c_ob = computed_ob[computed_ob["u"].isin(u_intersect)].drop(columns=["E"]).reset_index(drop=True)
    t_ob = true_ob[true_ob["u"].isin(u_intersect)].drop(columns=["E"]).reset_index(drop=True)
    diff = c_ob.compare(t_ob, align_axis=0)

    if diff.shape == (0,0):
        print("Validation successful.")
    else:
        print(f"Dataframes not equal. Showing differences:")
        print(diff)