from ws import NL_ID
from datetime import datetime, timedelta

import pandas as pd
import sqlite3
import csv

KNOWN_SYNONYMS = [
    # Super Auto Pets
    set([
        "How many wins will we get?",
        "How many wins will NL get?",
        "How many wins will get get?",
        "How many wins will we get this time?",
        "How many wins will NL get this time?",
        "How many wins will we get on this run?",
        ]),
    # Games + Demos (Zombs)
    set([
        "Where will NL finish?",
        "Where will we finish?",
        ])
]


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


def get_synonyms(event_title):
    for synonyms in KNOWN_SYNONYMS:
        if event_title in synonyms:
            return synonyms
    return set([event_title])


def normalize_outcome_title(outcome_title):
    return outcome_title.lower() \
        .replace("of the", "of") \
        .replace("-", " to ") \
        .replace("win this one", "win this run")


def calculate_history(channel_id):
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

    if channel_id == NL_ID:
        out = pd.concat([pd.read_csv("NorthernLion Predictions - predictions.csv"), out])
    return out


def history_for_prediction(channel_id, game_name, event_title, outcome_titles, days=60):
    history = calculate_history(channel_id)

    synonyms = get_synonyms(event_title)
    history = history[history["game_name"] == game_name]
    history = history[history["title"].isin(synonyms)]
    history = history[history["created_at"].astype('datetime64') > (datetime.now() - timedelta(days=days))]

    # Only select historical bets which have the same outcomes
    assert len(outcome_titles) == 10
    for i, outcome_title in enumerate(outcome_titles):
        history = history[history[f"title_{i+1}"].apply(normalize_outcome_title) == normalize_outcome_title(outcome_title)] if outcome_title is not None else history[history[f"title_{i+1}"].isnull()]

    return history


def calculate_odds(channel_id, game_name, event_title, outcome_titles):
    history = history_for_prediction(channel_id, game_name, event_title, outcome_titles)
    odds = history['win_index'].value_counts(normalize=True)

    non_null_outcomes = [title for title in outcome_titles if title is not None]
    # Fill in any outcomes which have never happened
    for i in range(len(non_null_outcomes)):
        if not odds.get(i+1):
            odds[i+1] = 0

    odds = odds.sort_index()
    odds.index = non_null_outcomes
    return len(history), odds
