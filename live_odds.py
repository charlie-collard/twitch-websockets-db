import pandas as pd

from calculate_odds import calculate_odds, load_game_history, timestamp_to_game
from ws import twitch_websocket_runner, NL_ID
from datetime import datetime

import asyncio

CHANNELS = [NL_ID]
TOPICS = ["predictions-channel-v1"]
SPACER = "=============================="


odds = None
def message_handler(message, topic):
    global odds
    event_data = message["data"]["event"]
    if event_data["status"] in ['ACTIVE', 'LOCKED', 'CANCELED']:
        if odds is None:
            channel_id = event_data["channel_id"]
            assert channel_id in CHANNELS
            game_name = timestamp_to_game(load_game_history(channel_id))(datetime.now().isoformat())
            event_title = event_data["title"]
            outcomes = event_data["outcomes"]
            outcome_titles = [outcome["title"] for outcome in outcomes] + [None] * (10 - len(outcomes))
            samples, odds = calculate_odds(channel_id, game_name, event_title, outcome_titles)
            print(f"Found odds for game {game_name}, event title {event_title}, with {samples} samples:")
            print(odds.to_string())

        if odds is not None:
            # Try to predict last-second betting + my bet
            extra = 50000 if event_data["status"] == "ACTIVE" else 0
            points = pd.Series(map(lambda x: int(x['total_points']) + extra, event_data['outcomes']))
            payouts = points.sum() / points
            outcomes = event_data["outcomes"]
            outcome_titles = [outcome["title"] for outcome in outcomes] + [None] * (10 - len(outcomes))
            payouts.index = [title for title in outcome_titles if title is not None]
            print(SPACER)
            print((odds * payouts).sort_values().to_string())

        if event_data["status"] in ["LOCKED", "CANCELED"]:
            odds = None

asyncio.run(twitch_websocket_runner(CHANNELS, TOPICS, message_handler))
