import plotly.express as px
import pandas as pd
import sqlite3
import datetime
import sys

from ws import NL_ID

DATE = sys.argv[1]

with sqlite3.connect("websockets.db") as connection:
    viewcounts = pd.read_sql_query(f"select channelTwitchID, utcTimestamp, viewers from viewercount_messages where channelTwitchID = '{NL_ID}' and date(utcTimestamp) = '{DATE}';", connection)
    games = pd.read_sql_query(f"select utcTimestamp, game from broadcast_settings_messages where channelTwitchID = '{NL_ID}' and date(utcTimestamp) = '{DATE}'", connection)
    predictions = pd.read_sql_query(f"select min(utcTimestamp) as utcTimestamp, eventTitle from prediction_messages where channelTwitchID = '{NL_ID}' and date(utcTimestamp) = '{DATE}' group by eventTwitchID", connection)

viewcounts["utcTimestamp"] = viewcounts["utcTimestamp"].astype("str")
fig = px.scatter(viewcounts, x="utcTimestamp", y="viewers")

for i, row in games.iterrows():
    # Parsing the date as a workaround to bug here https://github.com/plotly/plotly.py/issues/3065
    fig.add_vline(x=datetime.datetime.fromisoformat(row['utcTimestamp']).timestamp() * 1000, annotation_text=row['game'], line_color='purple')


for i, row in predictions.iterrows():
    fig.add_vline(x=datetime.datetime.fromisoformat(row['utcTimestamp']).timestamp() * 1000, annotation_text=row['eventTitle'], annotation_textangle=45, line_color='red', line_dash='dash')

fig.show()
