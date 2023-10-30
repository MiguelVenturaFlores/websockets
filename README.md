# WebSocket Python Library

This Python library provides a simple interface for accessing real-time data from the Binance API through websockets. It allows you to subscribe to and capture data streams related to order book updates, trades, and aggregated trades for a specified trading pair on Binance.

## Getting Started

### Prerequisites

To use this library, you need to have Python installed on your system. Additionally, you should have the following Python packages installed:

- `websocket-client`: You can install it using `pip`:
```shell
pip install websocket-client
```
- `requests`: You can install it using `pip`:
```shell
pip install requests
```


### Usage

Here's how you can use this library in your Python project:

1. Import the `BinanceWebSocket` class:

 ```python
from binance_websocket import BinanceWebSocket

socket = BinanceWebSocket("BTCUSDT", 1000)

socket.run()
```
A value of 1000 for the initial order book snapshot is considered to be large enough to capture the evolution of the order book for a few hours.

### Folder structure

#### Root Directory:
data/: The root directory where all data is stored.

#### Data Storage Structure:

- data/{symbol}/{date}/take_{number}/: For each day, there is a subdirectory named "take_{number}" to store data for different capture sessions or takes. The "take_{number}" represents a unique session, and each session stores data captured during that session.

- data/{symbol}/{date}/take_{number}/orderbook/: A subdirectory under each "take_{number}" directory that stores order book data.

- data/{symbol}/{date}/take_{number}/trades/: A subdirectory under each "take_{number}" directory that stores trade data.

- data/{symbol}/{date}/take_{number}/aggTrades/: A subdirectory under each "take_{number}" directory that stores aggregated trade data.

## References
[Binance API Documentation](https://binance-docs.github.io/apidocs/spot/en/#change-log): Official documentation for Binance's REST and WebSocket APIs.

[WebSocket API](https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams): Information about Binance's WebSocket market streams.

[REST API](https://binance-docs.github.io/apidocs/spot/en/#order-book): Information about Binance's REST API, including order book depth.