import streamlit as st

st.write("# Newsletter Email")

with st.container(border=True):

    email = st.text_input('Enter email address: ')
