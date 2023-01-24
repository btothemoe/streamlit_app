# streamlit_app.py

import streamlit as st
import snowflake.connector

# Everything is accessible via the st.secrets dict:

st.write("DB username:", st.secrets["user"])
st.write("DB account:", st.secrets["account"])


#Connect to snowflake
conn = snowflake.connector.connect(
                user=st.secrets["user"],
                password=st.secrets["password"],
                account=st.secrets["account"],
                warehouse=st.secrets["warehouse"],
                database=st.secrets["database"],
                schema=st.secrets["schema"]
                )


# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

rows = run_query("SELECT top 10 sl_cust, sl_store FROM sales;")

# Print results.
for row in rows:
    st.write(f"{row[0]} has a :{row[1]}:")

conn.close()
