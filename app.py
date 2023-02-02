# streamlit_app.py

import streamlit as st
import snowflake.connector

st.set_page_config(
    layout="wide",
    page_title="Presale Tool",
)


#Main Section
st.title('PRESALE MANAGER')
st.write('When you’re wondering why a SKU isn’t showing up online, this tool has your back!')

col1, col2 = st.columns((1,1))






