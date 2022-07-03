import sqlite3
from datetime import datetime

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
        eventCreatedAt datetime not null,
        eventCreatedByType text not null,
        eventCreatedByUserTwitchID text not null,
        eventCreatedByUserDisplayName text not null,
        eventEndedAt datetime,
        eventEndedByType text,
        eventEndedByUserTwitchID text,
        eventEndedByUserDisplayName text,
        eventLockedAt datetime,
        eventLockedByType text,
        eventLockedByUserTwitchID text,
        eventLockedByUserDisplayName text,
        eventWinningOutcomeTwitchId text,

        outcome0TwitchID text not null,
        outcome0Color text not null,
        outcome0TotalPoints integer not null,
        outcome0TotalUsers integer not null,
        outcome0BadgeVersion text not null,
        outcome0BadgeSetTwitchID text not null,

        outcome1TwitchID text not null,
        outcome1Color text not null,
        outcome1TotalPoints integer not null,
        outcome1TotalUsers integer not null,
        outcome1BadgeVersion text not null,
        outcome1BadgeSetTwitchID text not null,

        outcome2TwitchID text not null,
        outcome2Color text not null,
        outcome2TotalPoints integer not null,
        outcome2TotalUsers integer not null,
        outcome2BadgeVersion text not null,
        outcome2BadgeSetTwitchID text not null,

        outcome3TwitchID text not null,
        outcome3Color text not null,
        outcome3TotalPoints integer not null,
        outcome3TotalUsers integer not null,
        outcome3BadgeVersion text not null,
        outcome3BadgeSetTwitchID text not null,

        outcome4TwitchID text not null,
        outcome4Color text not null,
        outcome4TotalPoints integer not null,
        outcome4TotalUsers integer not null,
        outcome4BadgeVersion text not null,
        outcome4BadgeSetTwitchID text not null,

        outcome5TwitchID text not null,
        outcome5Color text not null,
        outcome5TotalPoints integer not null,
        outcome5TotalUsers integer not null,
        outcome5BadgeVersion text not null,
        outcome5BadgeSetTwitchID text not null,

        outcome6TwitchID text not null,
        outcome6Color text not null,
        outcome6TotalPoints integer not null,
        outcome6TotalUsers integer not null,
        outcome6BadgeVersion text not null,
        outcome6BadgeSetTwitchID text not null,

        outcome7TwitchID text not null,
        outcome7Color text not null,
        outcome7TotalPoints integer not null,
        outcome7TotalUsers integer not null,
        outcome7BadgeVersion text not null,
        outcome7BadgeSetTwitchID text not null,

        outcome8TwitchID text not null,
        outcome8Color text not null,
        outcome8TotalPoints integer not null,
        outcome8TotalUsers integer not null,
        outcome8BadgeVersion text not null,
        outcome8BadgeSetTwitchID text not null,

        outcome9TwitchID text not null,
        outcome9Color text not null,
        outcome9TotalPoints integer not null,
        outcome9TotalUsers integer not null,
        outcome9BadgeVersion text not null,
        outcome9BadgeSetTwitchID text not null
    );
    """
]

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
        :channelTwitchID,
        :utcTimestamp,
        :channelName,
        :oldStatus,
        :status,
        :oldGame,
        :game,
        :oldGameTwitchID,
        :gameTwitchID
    )
    """, to_insert)
    cursor.execute("commit;")


def create_tables(cursor):
    for create_table in TABLES:
        cursor.execute(create_table)
