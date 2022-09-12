from calculate_odds import calculate_odds, load_game_history, timestamp_to_game
from ws import twitch_websocket_runner, NL_ID
from datetime import datetime

import asyncio

CHANNELS = [NL_ID]
TOPICS = ["predictions-channel-v1"]


odds = None
def message_handler(message, topic):
    global odds
    event_data = message["data"]["event"]
    if event_data["status"] in ['ACTIVE', 'LOCKED']:
        if message["type"] == "event-created":
            channel_id = event_data["channel_id"]
            assert channel_id in CHANNELS
            game_name = timestamp_to_game(load_game_history(channel_id))(datetime.now().isoformat())
            event_title = event_data["title"]
            outcomes = event_data["outcomes"]
            outcome_titles = [outcome["title"] for outcome in outcomes] + [None] * (10 - len(outcomes))
            samples, odds = calculate_odds(channel_id, game_name, event_title, outcome_titles)
            print(f"Found odds for game {game_name}, event title {event_title}, with {samples} samples:", odds)

        if odds:
            # Try to predict last-second betting + my bet
            extra = 60000 if message['status'] == 'ACTIVE' else 0
            points = pd.Series(map(lambda x: int(x['total_points']) + extra, message['outcomes']))
            payouts = points.sum() / points
            payouts.index = [title for title in outcome_titles if title is not None]
            print((odds * payouts).sort_values())

asyncio.run(twitch_websocket_runner(CHANNELS, TOPICS, message_handler))
