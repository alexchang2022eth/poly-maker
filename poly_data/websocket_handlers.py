import asyncio                      # Asynchronous I/O
import json                         # JSON handling
import traceback                    # Exception handling
import os                           # Environment variables

import aiohttp                      # HTTP/WebSocket client with proxy support
from aiohttp import ClientTimeout, WSMsgType

from poly_data.data_processing import process_data, process_user_data
import poly_data.global_state as global_state

async def connect_market_websocket(chunk):
    """
    Connect to Polymarket's market WebSocket API and process market updates.

    Manages its own reconnect loop and captures handshake-time exceptions to
    prevent cross-cancellation with sibling tasks.
    """
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    proxy = os.getenv("WS_PROXY")  # e.g., http://127.0.0.1:7890
    timeout = ClientTimeout(total=None, connect=60)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                print(f"Attempting market websocket connect: uri={uri}, tokens={len(chunk)}, proxy={bool(proxy)}")
                ws = await session.ws_connect(
                    uri,
                    proxy=proxy,
                    heartbeat=5,
                    timeout=60,
                )

                message = {"assets_ids": chunk}
                await ws.send_str(json.dumps(message))

                print("\n")
                print(f"Sent market subscription message: {message}")

                while True:
                    msg = await ws.receive()
                    if msg.type == WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                            if isinstance(payload, list):
                                events = payload
                            elif isinstance(payload, dict):
                                events = [payload]
                            else:
                                print(f"Ignoring non-JSON-event market message: {payload!r}")
                                continue

                            process_data(events)
                        except json.JSONDecodeError:
                            print("Failed to decode market message JSON")
                            print(traceback.format_exc())
                        except Exception:
                            print("Failed to process market message")
                            print(traceback.format_exc())
                    elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
                        print("Market websocket closed by server")
                        break
                    elif msg.type == WSMsgType.ERROR:
                        print("Market websocket error message")
                        break
            except asyncio.CancelledError:
                print("Market websocket task cancelled")
                raise
            except (aiohttp.ClientConnectorError, aiohttp.ClientProxyConnectionError, TimeoutError, asyncio.TimeoutError, OSError) as e:
                print(f"Market websocket connection error: {e}")
                print(traceback.format_exc())
            except Exception as e:
                print(f"Market websocket handshake failed: {e}")
                print(traceback.format_exc())
            finally:
                await asyncio.sleep(5)

async def connect_user_websocket():
    """
    Connect to Polymarket's user WebSocket API and process order/trade updates.

    Manages its own reconnect loop and captures handshake-time exceptions,
    sending auth before entering the recv loop.
    """
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    proxy = os.getenv("WS_PROXY")
    timeout = ClientTimeout(total=None, connect=60)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                print(f"Attempting user websocket connect: uri={uri}, proxy={bool(proxy)}")
                ws = await session.ws_connect(
                    uri,
                    proxy=proxy,
                    heartbeat=5,
                    timeout=60,
                )

                message = {
                    "type": "user",
                    "auth": {
                        "apiKey": global_state.client.client.creds.api_key,
                        "secret": global_state.client.client.creds.api_secret,
                        "passphrase": global_state.client.client.creds.api_passphrase,
                    },
                }

                await ws.send_str(json.dumps(message))

                print("\n")
                print("Sent user subscription message")

                while True:
                    msg = await ws.receive()
                    if msg.type == WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                            if isinstance(payload, list):
                                rows = payload
                            elif isinstance(payload, dict):
                                rows = [payload]
                            else:
                                print(f"Ignoring non-JSON-event user message: {payload!r}")
                                continue

                            process_user_data(rows)
                        except json.JSONDecodeError:
                            print("Failed to decode user message JSON")
                            print(traceback.format_exc())
                        except Exception:
                            print("Failed to process user message")
                            print(traceback.format_exc())
                    elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
                        print("User websocket closed by server")
                        break
                    elif msg.type == WSMsgType.ERROR:
                        print("User websocket error message")
                        break
            except asyncio.CancelledError:
                print("User websocket task cancelled")
                raise
            except (aiohttp.ClientConnectorError, aiohttp.ClientProxyConnectionError, TimeoutError, asyncio.TimeoutError, OSError) as e:
                print(f"User websocket connection error: {e}")
                print(traceback.format_exc())
            except Exception as e:
                print(f"User websocket handshake failed: {e}")
                print(traceback.format_exc())
            finally:
                await asyncio.sleep(5)