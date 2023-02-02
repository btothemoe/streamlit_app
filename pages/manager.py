# streamlit_app.py

import streamlit as st
import snowflake.connector

st.set_page_config(
    layout="wide",
    page_title="Presale Manager",
)



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

    st.header('APROPOS LOOKUP')
    
    #Connect to snowflake
    conn = snowflake.connector.connect(
                user=st.secrets["user"],
                password=st.secrets["password"],
                account=st.secrets["account"],
                warehouse=st.secrets["warehouse"],
                database=st.secrets["database"],
                schema=st.secrets["schema"]
                )

    rows = run_query(f"""SELECT 
        inv_id3				AS ItemId    
        , TRIM(inv_desc)		AS ItemDesc
        , TRIM(inv_id2)			AS CategoryId
        ,CASE
            WHEN LENGTH(TRIM(inv_subcategory)) = 0 THEN NULL::CHAR(4)
            ELSE TRIM(inv_subcategory)
        END					AS SubCatId
        , TRIM(inv_id1)			AS VendorId
        , TRIM(inv_ven_id)		AS VendorStyle
        , inv_first_purch		AS FirstRecvdDate
        , inv_last_pur			AS LastRecvdDate
        , inv_cost				AS Cost
        , inv_reg_price			AS OrigPrice
        , inv_price				AS CurrPrice
        , inv_wholesale2		AS SpecialPrice
        , inv_wholesale1		AS TicketPrice
        , TRIM(inv_del_flag)	AS atbStatus
        , inv_user2				AS SubLicense
    FROM inv
    WHERE inv_id3 = '{sku}'""")

    # Print results.
    st.dataframe(rows)

    conn.close()


# ZMRSQL081 LOOKUPS
if sku:
    
    st.header('MASTERINTERFACE LOOKUP')
    
    #Connect to snowflake
    conn = snowflake.connector.connect(
                user=st.secrets["user"],
                password=st.secrets["password"],
                account=st.secrets["account"],
                warehouse=st.secrets["warehouse"],
                database=st.secrets["database"],
                schema="ZUMZ_MI_US"
                )

    rows = run_query(f"""SELECT *
	FROM ZUMZ_ItemMaster 
	WHERE ItemId = '{sku}'""")

    # Print results.
    st.dataframe(rows)
    
    conn.close()



# SALESWARP LOOKUP
if sku:

    st.header('SALESWARP LOOKUP')
    
    #Connect to snowflake
    conn = snowflake.connector.connect(
                user=st.secrets["user"],
                password=st.secrets["password"],
                account=st.secrets["account"],
                warehouse=st.secrets["warehouse"],
                database=st.secrets["database"],
                schema="ZUMZ_SW4X_US"
                )

    rows = run_query(f"""SELECT *
	FROM base_products
	WHERE sku = '{sku}'
    OR LEFT(sku, 6) = '{sku}'""")

    # Print results.
    st.dataframe(rows)
    
    conn.close()