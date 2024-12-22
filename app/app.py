import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression
import aiohttp
import asyncio
from functions import get_temperature_data, async_get_current_temperature



async def main(cities_list: list, api_key: str):
    'Выполняет асинхронные запросы к API'
    
    tasks = []
    for city in cities_list:
        tasks.append(asyncio.create_task(async_get_current_temperature(api_key, city)))
    try:
        return await asyncio.gather(*tasks)
    except ValueError as e:
        st.error(e)


# Код приложения
st.set_page_config(layout="wide")
st.title('Temperature analysis')

data = st.file_uploader('Please upload .csv file with your temperature data')
if data:
    # Загрузка датасета
    df = pd.read_csv(data, parse_dates=['timestamp'])
    
    # Описательные статистики загруженного датасета
    st.markdown('#### Describe statistics for uploaded data')
    if st.checkbox('Show describe statistics'):
        st.dataframe(df.describe(include='all'))
    
    # список городов
    cities = df['city'].unique()

    # Выбор города
    st.markdown('#### Filters')
    city = st.selectbox('Choose city', cities)
    
    if city:
        # Выбор временного периода и фильтрация датасета
        date_range = st.slider('Select time period', value=(df['timestamp'].agg('min').date(), df['timestamp'].agg('max').date()))
        df = df[(df['timestamp'] >= pd.to_datetime(date_range[0])) & (df['timestamp'] <= pd.to_datetime(date_range[1]))]
        
        # Обработка датасета
        data, trend_line_coef, seasons_profile, city_profile = get_temperature_data(city, df)
        
        # График температуры со скользящим средним, линией тренда и аномалиями
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=data['timestamp'], y=data['temperature'], 
                                name='temperature', line=dict(color='CornflowerBlue', width=1)))
        fig.add_trace(go.Scatter(x=data['timestamp'], y=data['rolling_mean'], 
                                name='rolling mean temperature', line=dict(color='Navy', width=4)))
        fig.add_trace(go.Scatter(x=data['timestamp'], y=data['trend_line_points'], 
                                name='trend line', line=dict(color='Crimson', width=4)))
        fig.add_trace(go.Scatter(x=data[data['anomaly']==True]['timestamp'],
                         y=data[data['anomaly']==True]['temperature'],
                         mode='markers', name='anomaly', line=dict(color='Purple', width=4)))
        fig.update_layout(title=dict(text=f'Temperature, rolling mean temperature, trend line and anomaly in {city}', 
                                     font=dict(size=22)), 
                        xaxis=dict(title=dict(text='Date')),  yaxis=dict(title=dict(text='Temperature (degrees C)')))
        st.plotly_chart(fig, use_container_width=True)
        
        # Направление тренда
        if trend_line_coef > 0:
            st.markdown(f'The trend is **upward** with coefficient **{trend_line_coef}**')
        elif trend_line_coef < 0:
            st.markdown(f'The trend is  **dawnward** with coefficient **{trend_line_coef}**')
        else:
            st.markdown('The trend is **no change** (horizontal line)')
        
        # Температурный профиль города
        st.write('')
        st.markdown(f'#### The temperature profile of city {city}')
        st.dataframe(city_profile)
        
        # Температурный профиль сезонов в выбранном городе
        st.write('')
        st.markdown(f'#### The seasons temperature profile of city {city}')
        st.dataframe(seasons_profile[['season', 'temp_mean', 'temp_std', 'temp_min', 'temp_max']])
        
        st.write('')
        
        # Получение текущей температуры
        st.markdown('#### Current temperature')
        with st.form('api_key_form'):
            header = st.markdown('##### Enter your API KEY for OpenWeatherMap API')
            api_key = st.text_input('API KEY')
            submit = st.form_submit_button('Apply')
        
        if submit:
            temperatures_list = asyncio.run(main(cities, api_key))
            if temperatures_list:
                current_temperatures = {k: v for k, v in zip(cities, temperatures_list)}
                current_temp = current_temperatures.get(city)
                
                st.markdown(f'Current temmperature in {city} is **{current_temp} degrees Celsius**')
                
                mean_temp = seasons_profile.loc[seasons_profile['season']=='winter', 'temp_mean'].to_list()[0]
                temp_std = seasons_profile.loc[seasons_profile['season']=='winter', 'temp_std'].to_list()[0]
                
                if abs(mean_temp - current_temp) / temp_std > 2:
                    st.markdown('The current temperature is **abnormal**.')
                else:
                    st.markdown('Current temperature is **within historical norm** for the season.')