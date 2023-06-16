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
        'DISTRIBUTE_TAX'                AS "Message Type"
    ,   'sale'                          AS "Transaction Type"
    ,   infor.id_trn                    AS "Document Number"
    ,   atb.sl_tran                     AS "Line Item Number"
    ,   infor.ts_trn_bgn                AS "Document Date"
    FROM ZUMZ_ATB_US.sales              atb
    JOIN ZUMZ_CEN_US.tr_trn             infor
    ON atb.sl_date = infor.dc_dy_bsn
        AND atb.sl_inv_num = RIGHT(infor.id_trn, 5)
        AND atb.sl_store = RIGHT(infor.id_bsn_un, 3)
        AND atb.sl_register = RIGHT(infor.id_ws, 3)
    JOIN ZUMZ_MI_US.ZUMZ_ItemMaster     mi
        ON atb.sl_seq = mi.ItemId
    WHERE atb.sl_date > CURRENT_DATE - 2
        AND (atb.sl_2slsman != '600' OR atb.sl_register != 'OMS')
        """)

    df = pd.DataFrame(
        rows,
        columns=('Message Type', 'Transaction Type', 'Document Number', 'Line Item Number', 'Document Date')
    )


    # Print results.
    st.dataframe(df)


    conn.close()    