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

cs = conn.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    st.write(one_row[0])
finally:
    cs.close()
conn.close()
