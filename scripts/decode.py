import os
import os.path
import sqlite3
import duckdb
import pandas as pd
from typedstream.stream import TypedStreamReader

# Paths
DB_PATH = "dbt/dbs/chat.db"
CHAT_DB_PATH = "chat.db"
DUCKDB_PATH = DB_PATH
OUTPUT_TABLE_NAME = "attributed_body_cleaned"

# Connect to DuckDB
con = duckdb.connect(DUCKDB_PATH)

# Code below is credited to:
# caleb531 -- https://apple.stackexchange.com/a/468461


# The textual contents of some messages are encoded in a special attributedBody
# column on the message row; this attributedBody value is in Apple's proprietary
# typedstream format, but can be parsed with the pytypedstream package
# (<https://pypi.org/project/pytypedstream/>)
def decode_message_attributedbody(data):
    if not data:
        return None

    for event in TypedStreamReader.from_data(data):
        # The first bytes object is the one we want
        if type(event) is bytes:
            return event.decode("utf-8")


with sqlite3.connect(DB_PATH) as connection:
    messages_df = pd.read_sql_query(
        sql="""SELECT ROWID, 
                      guid, 
                      text, 
                      attributedBody  AS attributed_body
                 FROM message 
             ORDER BY date ASC
        """,
        con=connection,
        parse_dates={"datetime": "ISO8601"},
    )

    # Decode any attributedBody values and merge them into the 'text' column
    messages_df["text"] = messages_df["text"].fillna(
        messages_df["attributed_body"].apply(decode_message_attributedbody)
    )

    con.register("attributed_body_cleaned", messages_df)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {OUTPUT_TABLE_NAME} AS
        SELECT * FROM attributed_body_cleaned
    """
    )

# Retrieve the cleaned data
df = con.execute(f"SELECT * FROM {OUTPUT_TABLE_NAME}").fetchdf()
print(df.head())
