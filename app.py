# streamlit_app.py

import streamlit as st
import snowflake.connector

cnn = snowflake.connector.connect(**)

tot_sql = “SELECT top 10 * FROM sales;”

cur = cnn.cursor()
cur.execute(tot_sql)

tot_df = cur.fetch_pandas_all()

cur.close()
cnn.close()
