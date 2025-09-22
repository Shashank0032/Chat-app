'''
import asyncio
import websockets
import json

async def test_websocket():
    uri = "ws://localhost:8000/ws/test_user"
    async with websockets.connect(uri) as websocket:
        print(f"Connected to {uri}")
        while True:
            try:
                message = await websocket.recv()
                print(f"Received message: {message}")
            except websockets.exceptions.ConnectionClosed:
                print("Connection closed")
                break

if __name__ == "__main__":
    asyncio.run(test_websocket())
'''
