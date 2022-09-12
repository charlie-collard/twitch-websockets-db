import pandas as pd
import sqlite3
import csv


def load_resolved_bets(channel_id):
    with sqlite3.connect('websockets.db') as connection:
        cursor = connection.cursor()
        query = cursor.execute(f"select * from prediction_messages where channelTwitchID = '{channel_id}' and eventStatus = 'RESOLVED';")
        cols = [column[0] for column in query.description]
        resolved_bets = pd.DataFrame(cursor.fetchall(), columns=cols)
    return resolved_bets


def load_game_history(channel_id):
    with sqlite3.connect('websockets.db') as connection:
        cursor = connection.cursor()
        query = cursor.execute(f"select utcTimestamp, game from broadcast_settings_messages where channelTwitchID = '{channel_id}';")
        cols = [column[0] for column in query.description]
        games = pd.DataFrame(cursor.fetchall(), columns=cols)
    return games


def timestamp_to_game(games):
    return lambda timestamp: games[games["utcTimestamp"] < timestamp].iloc[-1]["game"]


def get_winning_outcome_index(resolved_bets):
    id_columns = resolved_bets.columns[resolved_bets.columns.str.contains("outcome\d+TwitchID", regex=True)]
    return lambda row: 1 + int(id_columns[row[id_columns] == row["eventWinningOutcomeTwitchId"]].str.extract("outcome(\d+)TwitchID")[0][0])


def calculate_odds(channel_id, game_name, event_title, outcome_titles):
    resolved_bets, games = load_resolved_bets(channel_id), load_game_history(channel_id)

    out = pd.DataFrame()
    out["created_at"] = resolved_bets["eventCreatedAtUtc"]
    out["game_name"] = resolved_bets["eventCreatedAtUtc"].apply(timestamp_to_game(games))
    out["title"] = resolved_bets["eventTitle"]
    out["window_seconds"] = resolved_bets["eventPredictionWindowSeconds"]
    out["win_index"] = resolved_bets.apply(get_winning_outcome_index(resolved_bets), axis=1)
    for i in range(10):
        out[f"title_{i+1}"] = resolved_bets[f"outcome{i}Title"]
        out[f"points_{i+1}"] = resolved_bets[f"outcome{i}TotalPoints"]
        out[f"users_{i+1}"] = resolved_bets[f"outcome{i}TotalUsers"]

    out = pd.concat([pd.read_csv("NorthernLion Predictions - predictions.csv"), out])
    out = out[out["game_name"] == game_name]
    out = out[out["title"] == event_title]
    for i in range(10):
        outcome_title = outcome_titles[i]
        out = out[out[f"title_{i+1}"] == outcome_title] if outcome_title is not None else out[out[f"title_{i+1}"].isnull()]

    odds = out.win_index.value_counts(normalize=True).sort_index()
    odds.index = [title for title in outcome_titles if title is not None]
    return len(out), odds
