import websockets
import asyncio
import json
import requests
import os

GBP_ID = "53831996"

PREDICTIONS_TOPIC = f"predictions-channel-v1.{GBP_ID}"
GAME_CHANGE_TOPIC = f"broadcast-settings-update.{GBP_ID}"

{
  "channel_id": "31758516",
  "type": "broadcast_settings_update",
  "channel": "king20333",
  "old_status": ".",
  "status": ".",
  "old_game": "IRL",
  "game": "Games + Demos",
  "old_game_id": 494717,
  "game_id": 66082
}


def fetch_access_token():
    params = {
        "client_id": os.environ["client_id"],
        "client_secret": os.environ["client_secret"],
        "grant_type": "client_credentials",
        "scope": "user:read:email",
    }
    return requests.post("https://id.twitch.tv/oauth2/token", params=params).json()["access_token"]


background_tasks = set()
async def ping(websocket):
    await asyncio.sleep(4 * 60)
    print(json.dumps({"type":"PING"}))
    await websocket.send(json.dumps({"type":"PING"}))
    spawn_ping_task(websocket)


def spawn_ping_task(websocket):
    global background_tasks
    ping_task = asyncio.create_task(ping(websocket))
    background_tasks.add(ping_task)
    ping_task.add_done_callback(background_tasks.discard)


def handle_prediction_message(data):
    print("Received prediction message")
    pass


def handle_game_change_message(data):
    print("Received game change message")
    pass


def handle_message(message):
    print(message)
    message = json.loads(message)
    if message["type"] == "MESSAGE":
        topic, data = message["data"]["topic"], message["data"]["message"]
        if topic == PREDICTIONS_TOPIC:
            handle_prediction_message(data)
        if topic == GAME_CHANGE_TOPIC:
            handle_game_change_message(data)


listen = {
  "type": "LISTEN",
  "data": {
    "topics": [
      PREDICTIONS_TOPIC,
      GAME_CHANGE_TOPIC
    ],
    "auth_token": fetch_access_token()
  }
}


async def test():
    async for websocket in websockets.connect("wss://pubsub-edge.twitch.tv/v1"):
        try:
            spawn_ping_task(websocket)
            print(json.dumps(listen))
            await websocket.send(json.dumps(listen))
            while True:
                handle_message(await websocket.recv())
        except websockets.ConnectionClosed:
            continue

asyncio.run(test())
