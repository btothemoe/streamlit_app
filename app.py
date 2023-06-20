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
    startDate = st.text_input('Start Date', '2023-06-12')
    endDate = st.text_input('End Date', '2023-06-13')
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
    ,   atb.sl_date                     AS "Posting Date"
    ,   'ZUMZ'                          AS "Company Code"
    ,   'US'                            AS "Division Code"
    ,   ''                              AS "Customer Class Code"            --Empty For Now
    ,   atb.sl_store                    AS "Admin Origin Location Code"
    ,   atb.sl_store                    AS "Physical Origin Location Code"
    ,   atb.sl_store                    AS "Destination Location Code"
    ,   ''                              AS "Destination Street Address"     --What are these for in Store?
    ,   ''                              AS "Destination City"               --What are these for in Store?
    ,   ''                              AS "Destination Main Division"      --What are these for in Store?
    ,   ''                              AS "Destination Postal Code"        --What are these for in Store?
    ,   ''                              AS "Destination Country"            --What are these for in Store?
    ,   CONCAT(LPAD(atb.sl_seq, 6, 0), 
            LPAD(atb.sl_color, 4, 0), 
            LPAD(atb.sl_size, 4, 0))    AS "Product Code"
    ,   mi.VertexTaxCodeId              AS "Product Class"
    ,   atb.sl_qty                      AS "Quantity"
    ,   atb.sl_price                    AS "Extended Price"
    ,   sl_taxamt1 
            + sl_taxamt2 
            + sl_taxamt3 
            + sl_taxamt4                AS "Input Tax Total"
    ,   infor.ts_trn_bgn                AS "Flex Date 1"
    FROM ZUMZ_ATB_US.sales              atb
    JOIN ZUMZ_CEN_US.tr_trn             infor
    ON atb.sl_date = infor.dc_dy_bsn
        AND atb.sl_inv_num = RIGHT(infor.id_trn, 5)
        AND atb.sl_store = RIGHT(infor.id_bsn_un, 3)
        AND atb.sl_register = RIGHT(infor.id_ws, 3)
    JOIN ZUMZ_MI_US.ZUMZ_ItemMaster     mi
        ON atb.sl_seq = mi.ItemId
    WHERE sl_date >= '{startDate}' -- CURRENT_DATE - 2
        AND sl_date <= '{endDate}'
        AND (atb.sl_2slsman != '600' OR atb.sl_register != 'OMS')
    
    UNION ALL 

    //Master Query - Web
    SELECT DISTINCT
        'DISTRIBUTE_TAX'                AS "Message Type"
    ,   'sale'                          AS "Transaction Type"
    ,   orders.customer_order_code      AS "Document Number"
    ,   atb.sl_tran                     AS "Line Item Number"
    ,   orders.order_date               AS "Document Date"
    ,   atb.sl_date                     AS "Posting Date"
    ,   'ZUMZ'                          AS "Company Code"
    ,   'US'                            AS "Division Code"
    ,   ''                              AS "Customer Class Code"            --Empty For Now
    ,   atb.sl_2slsman                  AS "Admin Origin Location Code"
    ,   atb.sl_store                    AS "Physical Origin Location Code"
    ,   ''                              AS "Destination Location Code"      --Empty For Now
    ,   shipaddress.address_1           AS "Destination Street Address" 
    ,   shipaddress.locality            AS "Destination City"
    ,   shipaddress.region              AS "Destination Main Division"
    ,   shipaddress.postal_code         AS "Destination Postal Code"
    ,   shipaddress.country_code        AS "Destination Country"
    ,   CONCAT(LPAD(atb.sl_seq, 6, 0), 
            LPAD(atb.sl_color, 4, 0), 
            LPAD(atb.sl_size, 4, 0))    AS "Product Code"
    ,   mi.VertexTaxCodeId              AS "Product Class"
    ,   atb.sl_qty                      AS "Quantity"
    ,   atb.sl_price                    AS "Extended Price"
    ,   sl_taxamt1 
            + sl_taxamt2 
            + sl_taxamt3 
            + sl_taxamt4                AS "Input Tax Total"
    ,   orders.order_date               AS "Flex Date 1"
    FROM raw.ZUMZ_ATB_US.sales                                      atb
    JOIN Apollo.ZUMZ_US.SW_SALES_IMPORT_FILE                        sw
        ON  atb.sl_cust      = sw.tl_cust
        AND atb.sl_register  = sw.tl_register
        AND atb.sl_trantype  = sw.tl_trantype
        AND atb.sl_store     = sw.tl_store
        AND atb.sl_date      = sw.tl_date
        AND atb.sl_slsman    = sw.tl_slsman
        AND atb.sl_inv_num   = sw.tl_inv_num
        AND TRIM(atb.sl_eitem) = TRIM(sw.tl_eitem)::varchar(15)
    JOIN raw.ZUMZ_MI_US.ZUMZ_ItemMaster                             mi
        ON atb.sl_seq = mi.ItemId
    JOIN raw.ZUMZ_SW4x_US.base_orders                               orders
        ON sw.baseorder_id = orders.id
    LEFT JOIN raw.ZUMZ_SW4x_US.customer_addresses                   shipaddress
        ON orders.shipping_address_id = shipaddress.id
    WHERE sl_date >= '{startDate}' -- CURRENT_DATE - 2
        AND sl_date <= '{endDate}'
    AND (sl_2slsman = '600' OR sl_register = 'OMS')  
    """)

    pd.set_option('display.min_rows', 100)
    
    df = pd.DataFrame(
        rows,
        columns=('Message Type', 'Transaction Type', 'Document Number', 'Line Item Number', 'Document Date',
                 'Posting Date', 'Company Code', 'Division Code', 'Customer Class Code', 'Admin Origin Location Code',
                 'Physical Origin Location Code', 'Destination Location Code', 'Destination Street Address', 'Destination City',
                 'Destination Main Division', 'Destination Postal Code', 'Destination Country', 'Product Code',
                 'Product Class', 'Quantity', 'Extended Price', 'Input Tax Total', 'Flex Date 1')
    )

    
    # Print results.
    st.dataframe(
        df,
        height=650,
        hide_index=True
    )


    conn.close()    