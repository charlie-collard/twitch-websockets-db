import websockets
import asyncio
import json
import requests
import os
import sqlite3
import pandas as pd

from db import create_tables, insert_broadcast_settings_message, insert_prediction_message

KING20333_ID = "31758516"
GBP_ID = "53831996"
HIKARU_ID = "103268673"
CR1T_ID = "132230344"
BAHROO_ID = "40972890"
NL_ID = "14371185"
DAN_ID = "30923466"

LISTEN_ID = NL_ID

PREDICTIONS_TOPIC = f"predictions-channel-v1.{LISTEN_ID}"
GAME_CHANGE_TOPIC = f"broadcast-settings-update.{LISTEN_ID}"

test_broadcast_settings_message = {
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

test_predictions_message = {
  "type": "event-created",
  "data": {
    "timestamp": "2022-07-01T21:49:44.297583928Z",
    "event": {
      "id": "1f9292d3-de06-4cb0-85a9-e179422cf1d7",
      "channel_id": "53831996",
      "created_at": "2022-07-01T21:49:44.280353624Z",
      "created_by": {
        "type": "USER",
        "user_id": "41677803",
        "user_display_name": "Jullicent",
        "extension_client_id": None
      },
      "ended_at": None,
      "ended_by": None,
      "locked_at": None,
      "locked_by": None,
      "outcomes": [
        {
          "id": "da8948e9-38ff-4e78-a9be-747feca971da",
          "color": "BLUE",
          "title": "All 4",
          "total_points": 0,
          "total_users": 0,
          "top_predictors": [],
          "badge": {
            "version": "blue-1",
            "set_id": "predictions"
          }
        },
        {
          "id": "a958f32c-748b-4ced-81e2-70c3f04f2e3d",
          "color": "BLUE",
          "title": "3 of them",
          "total_points": 0,
          "total_users": 0,
          "top_predictors": [],
          "badge": {
            "version": "blue-2",
            "set_id": "predictions"
          }
        },
        {
          "id": "6b931ce5-7e09-4da4-8f39-8ccedaad660e",
          "color": "BLUE",
          "title": "2 of them",
          "total_points": 0,
          "total_users": 0,
          "top_predictors": [],
          "badge": {
            "version": "blue-3",
            "set_id": "predictions"
          }
        },
        {
          "id": "29b9f3ec-36ad-4ddd-9685-ebf8bc3d8c1a",
          "color": "BLUE",
          "title": "Only 1",
          "total_points": 0,
          "total_users": 0,
          "top_predictors": [],
          "badge": {
            "version": "blue-4",
            "set_id": "predictions"
          }
        },
        {
          "id": "0587e9ee-ef8b-4e4d-b4df-5621eb0aaea4",
          "color": "BLUE",
          "title": "None :( ",
          "total_points": 0,
          "total_users": 0,
          "top_predictors": [],
          "badge": {
            "version": "blue-5",
            "set_id": "predictions"
          }
        }
      ],
      "prediction_window_seconds": 120,
      "status": "ACTIVE",
      "title": "How much of the squad will make the finals?",
      "winning_outcome_id": None
    }
  }
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
    if data['status'] in ['ACTIVE', 'LOCKED']:
        odds = pd.Series([
            0.047904,
            0.071856,
            0.083832,
            0.107784,
            0.089820,
            0.119760,
            0.083832,
            0.119760,
            0.077844,
            0.197605,
        ])
        # Try to predict last-minute betting + and my bet
        extra = 20000 if data['status'] == 'ACTIVE' else 0
        points = pd.Series(list(map(lambda x: x['total_points'] + extra, data['outcomes'])))
        payouts = 1/(points / points.sum())
        payouts.index = ['0-1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
        odds.index = ['0-1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
        print((odds * payouts).sort_values(ascending=False))


def handle_message(message, cursor):
    message = json.loads(message)
    if message["type"] == "MESSAGE":
        topic, data = message["data"]["topic"], json.loads(message["data"]["message"])
        if "predictions-channel-v1" in topic:
            handle_prediction_message(data['data']['event'])



def make_listen_message(channel_ids):
    topics = []
    for channel_id in channel_ids:
        topics.append(f"predictions-channel-v1.{channel_id}")
        topics.append(f"broadcast-settings-update.{channel_id}")

    return {
      "type": "LISTEN",
      "data": {
        "topics": topics,
        "auth_token": fetch_access_token()
      }
    }


async def run(cursor):
    global background_tasks
    async for websocket in websockets.connect("wss://pubsub-edge.twitch.tv/v1"):
        for task in background_tasks:
            task.cancel()
        background_tasks = set()
        try:
            spawn_ping_task(websocket)
            listen = make_listen_message([NL_ID])
            print(json.dumps(listen))
            await websocket.send(json.dumps(listen))
            while True:
                handle_message(await websocket.recv(), cursor)
        except websockets.ConnectionClosed:
            continue

asyncio.run(run(None))
