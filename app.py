import streamlit as st
import pandas as pd
import snowflake.connector

st.set_page_config(
    layout="wide",
    page_title="Presale Manager",
)

#Main Section
# Sidebar
with st.sidebar:
    st.title('INPUTS')
    sku = st.text_input('6-Digit Item ID', '359378')
    st.selectbox('Country', ['US', 'CA'])
    st.text("")
    runReport = st.button('RUN IT!')
    st.divider()

#Main Section
st.title('VERTEX TAX TOOL')
st.write('Your one stop shop for all things tax!')


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

if (runReport):
    
    rows = run_query(f"""
        SELECT 
                SKU as sku
            ,   QTY_INITIAL as qty_initial
            ,   QTY_REMOVED as qty_removed
            ,   CREATED_UTC as created
            ,   MODIFIED_UTC as modified 
        FROM ZUMZ_MI_US.ZUMZ_INVENTORY_PRESALE""")

    df = pd.DataFrame(
        rows,
        columns=('sku', 'qty_intitial', 'qty_removed', 'created', 'modified')
    )


    # Print results.
    st.dataframe(df)


    conn.close()    