# streamlit_app.py

import streamlit as st
import snowflake.connector
import pyodbc

st.set_page_config(
    layout="wide",
    page_title="Presale Manager",
)


#Main Section
st.title('PRESALE MANAGER')
st.write('When you’re wondering why a SKU isn’t showing up online, this tool has your back!')


# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


# Sidebar
with st.sidebar:
    st.title('INPUTS')
    sku = st.text_input('6-DIGIT SKU')
    st.selectbox('COUNTRY', ['US', 'CA'])
    st.button('RUN IT!')

# APROPOS LOOKUP
if sku:

    st.header('ZMRSQL081 LOOKUP')
    
    #Connect to ZMRSQL081
    @st.experimental_singleton
    def init_connection():
        return pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + st.secrets["sql_server"]
            + ";DATABASE="
            + st.secrets["sql_database"]
            + ";UID="
            + st.secrets["sql_username"]
            + ";PWD="
            + st.secrets["sql_password"]
        )

    conn = init_connection()

    rows = run_query("SELECT TOP 10 * FROM ZUMZ_ItemMaster;")
    
    # Print results.
    st.dataframe(rows)

    conn.close()