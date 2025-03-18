import asyncpg
import asyncio
import websockets
from websockets.exceptions import ConnectionClosed
from config.properties import db_config as db

clients = set()  # Track connected WebSocket clients
clients_lock = asyncio.Lock()


async def broadcast_message(message):
    async with clients_lock:
        for client in list(clients):
            try:
                await client.send(message)
            except ConnectionClosed:
                print("❌ Could not send message, client disconnected.")
                clients.remove(client)


async def on_notify(conn, pid, channel, payload):
    print(f"📡 Received notification on channel '{channel}': {payload}")
    await broadcast_message(payload)


async def listen_to_db():
    """Listens to PostgreSQL NOTIFY events."""
    conn = await asyncpg.connect(db.url)  # ✅ Use db.url instead of manually formatting
    await conn.add_listener('weather_updates', on_notify)
    print("🔔 Listening for weather updates...")

    try:
        while True:
            await asyncio.sleep(1)  # Keep alive
    finally:
        # Clean up listener and close connection
        await conn.remove_listener('weather_updates', on_notify)
        await conn.close()


async def websocket_handler(websocket, path):
    async with clients_lock:
        clients.add(websocket)
    print(f"✅ Client connected: {websocket.remote_address}")

    try:
        while True:
            await asyncio.sleep(10)  # Keep connection alive
    except ConnectionClosed:
        async with clients_lock:
            clients.remove(websocket)
        print(f"❌ Client disconnected: {websocket.remote_address}")


async def main():
    # Run WebSocket server
    server = await websockets.serve(websocket_handler, "0.0.0.0", 8000)
    await asyncio.gather(server.wait_closed(), listen_to_db())
    print("🚀 WebSocket Server Started at ws://0.0.0.0:8000")


if __name__ == "__main__":
    asyncio.run(main())
