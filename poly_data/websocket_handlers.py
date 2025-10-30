import asyncio                      # Asynchronous I/O
import json                         # JSON handling
import websockets                   # WebSocket client
import traceback                    # Exception handling

from poly_data.data_processing import process_data, process_user_data
import poly_data.global_state as global_state

async def connect_market_websocket(chunk):
    """
    Connect to Polymarket's market WebSocket API and process market updates.

    Manages its own reconnect loop and captures handshake-time exceptions to
    prevent cross-cancellation with sibling tasks.
    """
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    while True:
        try:
            print(f"Attempting market websocket connect: uri={uri}, tokens={len(chunk)}")
            async with websockets.connect(
                uri,
                ping_interval=5,
                ping_timeout=20,
                open_timeout=60,
                close_timeout=10,
            ) as websocket:
                message = {"assets_ids": chunk}
                await websocket.send(json.dumps(message))

                print("\n")
                print(f"Sent market subscription message: {message}")

                try:
                    while True:
                        message = await websocket.recv()
                        json_data = json.loads(message)
                        process_data(json_data)
                except websockets.ConnectionClosed:
                    print("Connection closed in market websocket")
                    print(traceback.format_exc())
                except Exception as e:
                    print(f"Exception in market websocket: {e}")
                    print(traceback.format_exc())
        except asyncio.CancelledError:
            print("Market websocket task cancelled")
            raise
        except (TimeoutError, asyncio.TimeoutError, OSError) as e:
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
    while True:
        try:
            print(f"Attempting user websocket connect: uri={uri}")
            async with websockets.connect(
                uri,
                ping_interval=5,
                ping_timeout=20,
                open_timeout=60,
                close_timeout=10,
            ) as websocket:
                message = {
                    "type": "user",
                    "auth": {
                        "apiKey": global_state.client.client.creds.api_key,
                        "secret": global_state.client.client.creds.api_secret,
                        "passphrase": global_state.client.client.creds.api_passphrase,
                    },
                }

                await websocket.send(json.dumps(message))

                print("\n")
                print("Sent user subscription message")

                try:
                    while True:
                        message = await websocket.recv()
                        json_data = json.loads(message)
                        process_user_data(json_data)
                except websockets.ConnectionClosed:
                    print("Connection closed in user websocket")
                    print(traceback.format_exc())
                except Exception as e:
                    print(f"Exception in user websocket: {e}")
                    print(traceback.format_exc())
        except asyncio.CancelledError:
            print("User websocket task cancelled")
            raise
        except (TimeoutError, asyncio.TimeoutError, OSError) as e:
            print(f"User websocket connection error: {e}")
            print(traceback.format_exc())
        except Exception as e:
            print(f"User websocket handshake failed: {e}")
            print(traceback.format_exc())
        finally:
            await asyncio.sleep(5)