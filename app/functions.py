import pandas as pd
from sklearn.linear_model import LinearRegression
import aiohttp
import asyncio



def get_temperature_data(city: str, data: pd.DataFrame) -> tuple[pd.DataFrame, float, pd.DataFrame, pd.DataFrame]:
    '''
        Функция принимает название города и датафрейм с данными температуры.
        Возвращает кортеж из отфильтрованного по городу датафрейма с добавленными столбцами скользящего среднего,
            стандартного отклонения, среднего, точек для линии тренда и индикатор аномальности значения температуры;
            число с плавающей точкой - коэффициент линии тренда, датафрейм с температурным профилем сезонов в выбранном
            городе и датафрейм с температурным профилем города.
    '''
    data = data[data['city'] == city]
    
    # Расчет скользящего среднего и скользящего std
    grouped_df = data.groupby(['city'])
    for city, group in grouped_df:
        data.loc[group.index, 'rolling_mean'] = group['temperature'].rolling(window=30).mean()
        data.loc[group.index, 'rolling_std'] = group['temperature'].rolling(window=30).std()
    
    data = data.dropna()
    
    # Получение линии тренда для одного города при помощи линейной регрессии
    X = data.dropna()[['timestamp']].astype('int')
    y = data.dropna()['rolling_mean']
    reg = LinearRegression()
    reg.fit(X, y)
    data['trend_line_points'] = reg.predict(X)
    
    # Коэффициент линии тренда
    trend_line_coef = reg.coef_[0]
    
    # Температурные профили сезонов по городам
    seasons_profile = data[['city', 'season', 'temperature']].groupby(['city', 'season'], as_index=False)\
                        .agg(temp_mean=('temperature', 'mean'), temp_std=('temperature', 'std'), 
                            temp_min=('temperature', 'min'), temp_max=('temperature', 'max'))

    # Поиск аномальных значений температуры относительно средней температуры для сезона по городам
    data = pd.merge(data, seasons_profile, how='inner', on=['city', 'season'])
    data['anomaly'] = abs(data['temperature']) > abs(data['temp_mean']) + data['temp_std'] * 2
    
    # Температурные профили городов
    city_profile = data[['city', 'temperature']].groupby(['city'], as_index=False)\
                        .agg(temp_mean=('temperature', 'mean'), temp_std=('temperature', 'std'), 
                            temp_min=('temperature', 'min'), temp_max=('temperature', 'max'))
    
    return (data, trend_line_coef, seasons_profile, city_profile)



async def async_get_current_temperature(api_key: str, city: str) -> float | None:
    '''
        Функция принимает ключ для OpenWeatherMap API и название города.
        Возвращает текущую температуру в выбранном городе.
    '''

    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                current_temp = await response.json() 
                return round(current_temp.get('main').get('temp'), 2)
            elif response.status == 401:
                raise ValueError('Invalid API key. Please see https://openweathermap.org/faq#error401 for more info.')
            else:
                print(f'Ошибка, сервер вернул ответ с кодом {response.status}')
            return None

