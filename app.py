# streamlit_app.py

import streamlit as st
import snowflake.connector


st.set_page_config(layout="wide")

# Sidebar
with st.sidebar:
    st.title('INPUTS')
    st.text_input('6-DIGIT SKU')
    st.selectbox('COUNTRY', ['US', 'CA'])
    st.button('RUN IT!')

#Main Section
st.title('WEB INVENTORY LOOKUP TOOL')
st.write('When you’re wondering why a SKU isn’t showing up online, this tool has your back!')

col1, col2 = st.columns((1,1))

with col1:
    st.header('ITEM INFO')

with col2:
    st.header('TESTS')

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
st.dataframe(rows)

conn.close()
