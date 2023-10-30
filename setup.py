from setuptools import setup

setup(
    name="websockets",
    version="0.1",
    description="Websockets to download order book data.",
    author="Miguel Ventura",
    packages=["websockets"],
    install_requires=["requests==2.28.1", "websocket-client==0.58.0"]
)
