import functools
import json

import asyncpg
import asyncio
import requests
import websockets
from websockets.exceptions import ConnectionClosed
from config.properties import db_config as db, airflow_db_config as af

print = functools.partial(print, flush=True)

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


def trigger_dag_api(dag_id):
    url = f"http://airflow-webserver:8080/api/v1/dags/{dag_id}/dagRuns"
    response = requests.post(url, auth=(af.user, af.password), json={})
    if response.status_code == 200:
        print(f"✅ Triggered DAG {dag_id}")
    else:
        print(f"❌ Failed to trigger DAG {dag_id}: {response.text}")


async def on_notify(conn, pid, channel, payload):
    print(f"📡 Received notification on channel '{channel}': {payload}")
    if channel == 'raw_updates' and payload == 'new_data':
        trigger_dag_api("WeatherStagingUpdate")
    message = json.dumps({
        "channel": channel,
        "payload": payload
    })
    await broadcast_message(message)


async def listen_to_db():
    """Listens to PostgreSQL NOTIFY events."""
    conn = await asyncpg.connect(db.url)
    await conn.add_listener('raw_updates', on_notify)
    await conn.add_listener('staging_updates', on_notify)
    print("🔔 Listening for raw and staging updates...")

    try:
        while True:
            await asyncio.sleep(1)  # Keep alive
    finally:
        # Clean up listener and close connection
        await conn.remove_listener('raw_updates', on_notify)
        await conn.remove_listener('staging_updates', on_notify)
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
