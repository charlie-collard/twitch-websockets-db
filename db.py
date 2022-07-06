import sqlite3
from datetime import datetime

TABLES = [
    """
    create table if not exists broadcast_settings_messages (
        id integer primary key autoincrement,
        utcTimestamp datetime not null,
        channelTwitchID text not null,
        channelName text not null,
        oldStatus text not null,
        status text not null,
        oldGame text not null,
        game text not null,
        oldGameTwitchID text not null,
        gameTwitchID text not null
    );
    """,
    """
    create table if not exists prediction_messages (
        id integer primary key autoincrement,
        utcTimestamp datetime not null,
        channelTwitchID text not null,

        eventTwitchID text not null,
        eventType text not null,
        eventStatus text not null,
        eventTitle text not null,
        eventPredictionWindowSeconds integer not null,
        eventCreatedAtUtc datetime not null,
        eventCreatedByType text not null,
        eventCreatedByUserTwitchID text not null,
        eventCreatedByUserDisplayName text not null,
        eventEndedAtUtc datetime,
        eventEndedByType text,
        eventEndedByUserTwitchID text,
        eventEndedByUserDisplayName text,
        eventLockedAtUtc datetime,
        eventLockedByType text,
        eventLockedByUserTwitchID text,
        eventLockedByUserDisplayName text,
        eventWinningOutcomeTwitchId text,

        outcome0TwitchID text not null,
        outcome0Title text not null,
        outcome0Color text not null,
        outcome0TotalPoints integer not null,
        outcome0TotalUsers integer not null,
        outcome0BadgeVersion text not null,
        outcome0BadgeSetTwitchID text not null,

        outcome1TwitchID text not null,
        outcome1Title text not null,
        outcome1Color text not null,
        outcome1TotalPoints integer not null,
        outcome1TotalUsers integer not null,
        outcome1BadgeVersion text not null,
        outcome1BadgeSetTwitchID text not null,

        outcome2TwitchID text,
        outcome2Title text,
        outcome2Color text,
        outcome2TotalPoints integer,
        outcome2TotalUsers integer,
        outcome2BadgeVersion text,
        outcome2BadgeSetTwitchID text,

        outcome3TwitchID text,
        outcome3Title text,
        outcome3Color text,
        outcome3TotalPoints integer,
        outcome3TotalUsers integer,
        outcome3BadgeVersion text,
        outcome3BadgeSetTwitchID text,

        outcome4TwitchID text,
        outcome4Title text,
        outcome4Color text,
        outcome4TotalPoints integer,
        outcome4TotalUsers integer,
        outcome4BadgeVersion text,
        outcome4BadgeSetTwitchID text,

        outcome5TwitchID text,
        outcome5Title text,
        outcome5Color text,
        outcome5TotalPoints integer,
        outcome5TotalUsers integer,
        outcome5BadgeVersion text,
        outcome5BadgeSetTwitchID text,

        outcome6TwitchID text,
        outcome6Title text,
        outcome6Color text,
        outcome6TotalPoints integer,
        outcome6TotalUsers integer,
        outcome6BadgeVersion text,
        outcome6BadgeSetTwitchID text,

        outcome7TwitchID text,
        outcome7Title text,
        outcome7Color text,
        outcome7TotalPoints integer,
        outcome7TotalUsers integer,
        outcome7BadgeVersion text,
        outcome7BadgeSetTwitchID text,

        outcome8TwitchID text,
        outcome8Title text,
        outcome8Color text,
        outcome8TotalPoints integer,
        outcome8TotalUsers integer,
        outcome8BadgeVersion text,
        outcome8BadgeSetTwitchID text,

        outcome9TwitchID text,
        outcome9Title text,
        outcome9Color text,
        outcome9TotalPoints integer,
        outcome9TotalUsers integer,
        outcome9BadgeVersion text,
        outcome9BadgeSetTwitchID text
    );
    """
]

def to_timestamp(timestamp):
    i = timestamp.find(".")
    j = timestamp.find("Z")
    if i == -1:
        return datetime.fromisoformat(timestamp[:j])

    fractional = float(timestamp[i:j])
    formatted = timestamp[:i] + f'{fractional:.6f}'[1:]
    return datetime.fromisoformat(formatted)


def get_nullable(event_data, key, post_process=lambda x: x):
    return post_process(event_data[key]) if event_data.get(key) else None


def insert_prediction_message(cursor, data):
    event_data = data["data"]["event"]
    to_insert = {
        "utcTimestamp": to_timestamp(data["data"]["timestamp"]),
        "channelTwitchID": event_data["channel_id"],
        "eventTwitchID": event_data["id"],
        "eventType": data["type"],
        "eventStatus": event_data["status"],
        "eventTitle": event_data["title"],
        "eventPredictionWindowSeconds": event_data["prediction_window_seconds"],
        "eventCreatedAtUtc": to_timestamp(event_data["created_at"]),
        "eventCreatedByType": event_data["created_by"]["type"],
        "eventCreatedByUserTwitchID": event_data["created_by"]["user_id"],
        "eventCreatedByUserDisplayName": event_data["created_by"]["user_display_name"],
        "eventEndedAtUtc": get_nullable(event_data, "ended_at", to_timestamp),
        "eventEndedByType": get_nullable(event_data, "ended_by", lambda x: x["type"]),
        "eventEndedByUserTwitchID": get_nullable(event_data, "ended_by", lambda x: x["user_id"]),
        "eventEndedByUserDisplayName": get_nullable(event_data, "ended_by", lambda x: x["user_display_name"]),
        "eventLockedAtUtc": get_nullable(event_data, "locked_at", to_timestamp),
        "eventLockedByType": get_nullable(event_data, "locked_by", lambda x: x["type"]),
        "eventLockedByUserTwitchID": get_nullable(event_data, "locked_by", lambda x: x["user_id"]),
        "eventLockedByUserDisplayName": get_nullable(event_data, "locked_by", lambda x: x["user_display_name"]),
        "eventWinningOutcomeTwitchId": get_nullable(event_data, "winning_outcome_id"),
    }

    for i in range(10):
        outcome = event_data["outcomes"][i] if len(event_data["outcomes"]) > i else None
        to_insert |= {
            f"outcome{i}TwitchID": outcome["id"],
            f"outcome{i}Title": outcome["title"],
            f"outcome{i}Color": outcome["color"],
            f"outcome{i}TotalPoints": outcome["total_points"],
            f"outcome{i}TotalUsers": outcome["total_users"],
            f"outcome{i}BadgeVersion": outcome["badge"]["version"],
            f"outcome{i}BadgeSetTwitchID": outcome["badge"]["set_id"],
        } if outcome else {
            f"outcome{i}TwitchID": None,
            f"outcome{i}Title": None,
            f"outcome{i}Color": None,
            f"outcome{i}TotalPoints": None,
            f"outcome{i}TotalUsers": None,
            f"outcome{i}BadgeVersion": None,
            f"outcome{i}BadgeSetTwitchID": None,
        }


    cursor.execute("""
    insert into prediction_messages (
        utcTimestamp,
        channelTwitchID,
        eventTwitchID,
        eventType,
        eventStatus,
        eventTitle,
        eventPredictionWindowSeconds,
        eventCreatedAtUtc,
        eventCreatedByType,
        eventCreatedByUserTwitchID,
        eventCreatedByUserDisplayName,
        eventEndedAtUtc,
        eventEndedByType,
        eventEndedByUserTwitchID,
        eventEndedByUserDisplayName,
        eventLockedAtUtc,
        eventLockedByType,
        eventLockedByUserTwitchID,
        eventLockedByUserDisplayName,
        eventWinningOutcomeTwitchId,
        outcome0TwitchID,
        outcome0Title,
        outcome0Color,
        outcome0TotalPoints,
        outcome0TotalUsers,
        outcome0BadgeVersion,
        outcome0BadgeSetTwitchID,
        outcome1TwitchID,
        outcome1Title,
        outcome1Color,
        outcome1TotalPoints,
        outcome1TotalUsers,
        outcome1BadgeVersion,
        outcome1BadgeSetTwitchID,
        outcome2TwitchID,
        outcome2Title,
        outcome2Color,
        outcome2TotalPoints,
        outcome2TotalUsers,
        outcome2BadgeVersion,
        outcome2BadgeSetTwitchID,
        outcome3TwitchID,
        outcome3Title,
        outcome3Color,
        outcome3TotalPoints,
        outcome3TotalUsers,
        outcome3BadgeVersion,
        outcome3BadgeSetTwitchID,
        outcome4TwitchID,
        outcome4Title,
        outcome4Color,
        outcome4TotalPoints,
        outcome4TotalUsers,
        outcome4BadgeVersion,
        outcome4BadgeSetTwitchID,
        outcome5TwitchID,
        outcome5Title,
        outcome5Color,
        outcome5TotalPoints,
        outcome5TotalUsers,
        outcome5BadgeVersion,
        outcome5BadgeSetTwitchID,
        outcome6TwitchID,
        outcome6Title,
        outcome6Color,
        outcome6TotalPoints,
        outcome6TotalUsers,
        outcome6BadgeVersion,
        outcome6BadgeSetTwitchID,
        outcome7TwitchID,
        outcome7Title,
        outcome7Color,
        outcome7TotalPoints,
        outcome7TotalUsers,
        outcome7BadgeVersion,
        outcome7BadgeSetTwitchID,
        outcome8TwitchID,
        outcome8Title,
        outcome8Color,
        outcome8TotalPoints,
        outcome8TotalUsers,
        outcome8BadgeVersion,
        outcome8BadgeSetTwitchID,
        outcome9TwitchID,
        outcome9Title,
        outcome9Color,
        outcome9TotalPoints,
        outcome9TotalUsers,
        outcome9BadgeVersion,
        outcome9BadgeSetTwitchID
    ) values (
    """ + ",\n".join(f":{key}" for key, value in to_insert.items()) + ");"
    , to_insert)
    cursor.execute("commit;")

def insert_broadcast_settings_message(cursor, data):
    to_insert = {
        "channelTwitchID": data["channel_id"],
        "utcTimestamp": datetime.utcnow(),
        "channelName": data["channel"],
        "oldStatus": data["old_status"],
        "status": data["status"],
        "oldGame": data["old_game"],
        "game": data["game"],
        "oldGameTwitchID": str(data["old_game_id"]),
        "gameTwitchID": str(data["game_id"]),
    }

    cursor.execute("""
    insert into broadcast_settings_messages (
        channelTwitchID,
        utcTimestamp,
        channelName,
        oldStatus,
        status,
        oldGame,
        game,
        oldGameTwitchID,
        gameTwitchID
    ) values (
    """ + ",\n".join(f":{key}" for key, value in to_insert.items()) + ");"
    , to_insert)
    cursor.execute("commit;")


def create_tables(cursor):
    for create_table in TABLES:
        cursor.execute(create_table)
