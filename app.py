# streamlit_app.py

import streamlit as st
import snowflake.connector


st.set_page_config(layout="wide")

# Sidebar
with st.sidebar:
    st.title('INPUTS')
    sku = st.text_input('6-DIGIT SKU')
    st.selectbox('COUNTRY', ['US', 'CA'])
    st.button('RUN IT!')

#Main Section
st.title('WEB INVENTORY LOOKUP TOOL')
st.write('When you’re wondering why a SKU isn’t showing up online, this tool has your back!')

col1, col2 = st.columns((1,1))

#with col1:
#    st.header('ITEM INFO')

#with col2:
#    st.header('TESTS')

# st.write("DB username:", st.secrets["user"])
# st.write("DB account:", st.secrets["account"])



# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()



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


# MASTERINTERFACE LOOKUP
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





