import pandas as pd
import sqlite3
import csv

channel_id = "14371185"
game_name = "Rumbleverse"
event_title = "we finish"

with sqlite3.connect('websockets.db') as connection:
    cursor = connection.cursor()
    query = cursor.execute(f"select * from prediction_messages where channelTwitchID = '{channel_id}' and eventStatus = 'RESOLVED';")
    cols = [column[0] for column in query.description]
    resolved_bets = pd.DataFrame(cursor.fetchall(), columns=cols)
    query = cursor.execute(f"select utcTimestamp, game from broadcast_settings_messages where channelTwitchID = '{channel_id}';")
    cols = [column[0] for column in query.description]
    games = pd.DataFrame(cursor.fetchall(), columns=cols)


def timestamp_to_game(timestamp):
    return games[games["utcTimestamp"] < timestamp].iloc[-1]["game"]

id_columns = resolved_bets.columns[resolved_bets.columns.str.contains("outcome\d+TwitchID", regex=True)]
def get_winning_outcome_index(row):
    return 1 + int(id_columns[row[id_columns] == row["eventWinningOutcomeTwitchId"]].str.extract("outcome(\d+)TwitchID")[0][0])


out = pd.DataFrame()
out["created_at"] = resolved_bets["eventCreatedAtUtc"]
out["game_name"] = resolved_bets["eventCreatedAtUtc"].apply(timestamp_to_game)
out["title"] = resolved_bets["eventTitle"]
out["window_seconds"] = resolved_bets["eventPredictionWindowSeconds"]
out["win_index"] = resolved_bets.apply(get_winning_outcome_index, axis=1)
for i in range(10):
    out[f"title_{i+1}"] = resolved_bets[f"outcome{i}Title"]
    out[f"points_{i+1}"] = resolved_bets[f"outcome{i}TotalPoints"]
    out[f"users_{i+1}"] = resolved_bets[f"outcome{i}TotalUsers"]

out = pd.concat([pd.read_csv("NorthernLion Predictions - predictions.csv"), out])
out = out[out["game_name"] == game_name] if game_name else out
out = out[out["title"].str.contains(event_title)] if event_title else out
out = out[out["created_at"] > "2022-09-01"]
print(out.win_index.value_counts(normalize=True).sort_index())
