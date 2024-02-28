# Import python packages
import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title("Snowpark Forecasting Framework")
st.write(
    """Pick a store and see your forecast!
    """
)

# Get the current credentials
session = get_active_session()

df = session.table('actual_vs_forecast').to_pandas()
df = df.sort_values(by=['DATE'])
start_date = st.sidebar.date_input('Start date', df['DATE'].min())
end_date = st.sidebar.date_input('End date', df['DATE'].max())

df['DATE']= pd.to_datetime(df['DATE']).dt.date
df = df[df.DATE.between(start_date, end_date)]

store_id = st.sidebar.multiselect("Pick your store",df['STORE_ID'].unique().tolist(),1)

df = df[df['STORE_ID'].isin(store_id)]
df = df[["STORE_ID", "DATE","ACTUAL","FORECAST"]]

st.title('Daily Forecast by Store ID')

actuals = alt.Chart(df).mark_line(color='blue').encode(
    x = alt.X('DATE', title = 'Date'),
    y = alt.Y('ACTUAL'))

forecast = alt.Chart(df).mark_line(color='#A0E3F6').encode(
    x = alt.X('DATE', title = 'Date'),
    y = alt.Y('FORECAST'))

st.altair_chart(actuals+forecast,theme= None, use_container_width=True)

st.dataframe(df)