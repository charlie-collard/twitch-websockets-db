from ws import twitch_websocket_runner, NL_ID

import asyncio

CHANNELS = [NL_ID]
TOPICS = ["predictions-channel-v1"]

def message_handler(message, topic):
    if "predictions-channel-v1" in topic and message['status'] in ['ACTIVE', 'LOCKED']:
        odds = pd.Series([
            0.333333,
            0.252874,
            0.356322,
            0.057471,
        ])
        # Try to predict last-minute betting + my bet
        extra = 60000 if message['status'] == 'ACTIVE' else 0
        points = pd.Series(map(lambda x: int(x['total_points']) + extra, message['outcomes']))
        payouts = points.sum() / points
        odds.index = ['Outside of top 20', '20-11', '10-2', '1']
        payouts.index = ['Outside of top 20', '20-11', '10-2', '1']
        print((odds * payouts).sort_values())

asyncio.run(twitch_websocket_runner(CHANNELS, TOPICS, message_handler))
