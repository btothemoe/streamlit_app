# streamlit_app.py

import streamlit as st

# Everything is accessible via the st.secrets dict:

st.write("DB username:", st.secrets["user"])
st.write("DB account:", st.secrets["account"])
