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

col1, col2 = conn.cursor().execute("SELECT top 10 sl_cust, sl_store FROM sales;").fetchone()
print('{0}, {1}'.format(col1, col2))

conn.close()
