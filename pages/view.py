# streamlit_app.py

import streamlit as st
import snowflake.connector

st.set_page_config(
    layout="wide",
    page_title="Presale Manager",
)


#Main Section
st.title('PRESALE MANAGER')
st.write('When you’re wondering why a SKU isn’t showing up online, this tool has your back!')


# Perform query function.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


#Connect to snowflake
conn = snowflake.connector.connect(
            user=st.secrets["user"],
            password=st.secrets["password"],
            account=st.secrets["account"],
            warehouse=st.secrets["warehouse"],
            database=st.secrets["database"]
)

rows = run_query(f"""
    SELECT 
            SKU as sku
        ,   QTY_INITIAL as qty_initial
        ,   QTY_REMOVED as qty_removed
        ,   CREATED_UTC as created
        ,   MODIFIED_UTC as modified 
    FROM ZUMZ_MI_US.ZUMZ_INVENTORY_PRESALE""")

# Print results.
st.dataframe(rows)

conn.close()