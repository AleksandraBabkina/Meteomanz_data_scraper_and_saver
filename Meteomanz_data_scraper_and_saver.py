import requests as req
import re
import math
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, select, or_, and_, Table, MetaData, inspect, text, types
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects import oracle
import sys
import oracledb
import time
from IPython.display import display, clear_output

oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb

# Header with connection, DO NOT MODIFY
username = 'username'
password = 'password'
dsn = 'dsn'

conection_string = f'oracle+cx_oracle://{username}:{password}@{dsn}' # Opening SQL connection
engine = create_engine(conection_string) # Engine

# Creating session
Session = sessionmaker(bind=engine)
session = Session()

headers = {
'Accept' : 'Accept',
'Accept-Encoding' : 'Accept-Encoding',
'Accept-Language' : 'Accept-Language',
'Cache-Control' : 'Cache-Control',
'Cookie' : 'Cookie',
'Priority' : 'Priority',
'Sec-Ch-Ua' : 'Sec-Ch-Ua',
'Sec-Ch-Ua-Mobile' : 'Sec-Ch-Ua-Mobile',
'Sec-Ch-Ua-Platform' : 'Sec-Ch-Ua-Platform',
'Sec-Fetch-Dest' : 'Sec-Fetch-Dest',
'Sec-Fetch-Mode' : 'Sec-Fetch-Mode',
'Sec-Fetch-Site' : 'Sec-Fetch-Site',
'Sec-Fetch-User' : 'Sec-Fetch-User',
'Upgrade-Insecure-Requests' : 'Upgrade-Insecure-Requests',
'User-Agent' : 'User-Agent'
}

# def parse_meteomanz_data(url, headers):
#     r = requests.get(url, headers=headers)
#     text = r.text
#     temp = []
#     for x in text.splitlines():
#         x = re.sub('[\n]','',x)
#         x = re.sub(': ','\' : \'',x)
#         x = '\'' + x + '\',' 
#         temp.append(x)
#     temp[-1] = temp[-1][:-1]
#     return temp
# temp = parse_meteomanz_data(url, headers)

def count_pages(temp):
    max_page = [x for x in temp.splitlines() if '<b><i>Showing results</b></i>' in x]
    max_page = max_page[0]
    match = re.search(r'of (\d+)', max_page)
    num = int(match.group(1))
    num_pages = math.ceil(num / 300)
    return num_pages

def parse_and_save_data(text):
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(text, 'html.parser')

    # Find the table with the data
    table = soup.find('table', class_='data')

    # Extract the column names from the table headers
    column_names = [th.text.strip() for th in table.find('tr').find_all('th')]

    # Extract the data from the table rows
    data = []
    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        data.append([col.text.strip() for col in cols])

    # Create a Pandas DataFrame from the data
    df = pd.DataFrame(data, columns=column_names)

    df = df.rename(columns={'Station': 'STATION',
                            'Date ∇': 'DATE_', 
                            'Ave. T.(ÂºC)': 'AVERAGE_T_IN_C',
                            'Max. T.(ÂºC)': 'MAX_T_IN_C',
                            'Min. T.(ÂºC)': 'MIN_T_IN_C',
                            'Prec.(mm)': 'PRECIPITATIONS_IN_MM',
                            'S.L.Press./Gheopot.': 'PRESSURE_IN_HPA',
                            'Wind dir': 'WIND_DIRECTION_IN_A',
                            'Wind sp.(Km/h)': 'WIND_SPEED_IN_KM_H',
                            'Cloud c.': 'CLOUD_COVER',
                            'Snow depth(cm)': 'SNOW_DEPTH',
                            'Insolat.(hours)': 'INSOLATION_IN_HOURS'})

    df['DATE_'] = pd.to_datetime(df['DATE_'], dayfirst=True)
    df['STATION'] = df['STATION'].astype(str)
    if 'SNOW_DEPTH' not in df.columns:
        df['SNOW_DEPTH'] = pd.to_numeric(0)
    df['SNOW_DEPTH'] = df['SNOW_DEPTH'].astype(str)
    df['CLOUD_COVER'] = df['CLOUD_COVER'].astype(str).replace('-', 0)
    df['WIND_DIRECTION_IN_A'] = df['WIND_DIRECTION_IN_A'].apply(lambda x: str(x).replace('Âº', ' ')).replace('-', '0').astype(str)
    df['PRESSURE_IN_HPA'] = df['PRESSURE_IN_HPA'].astype(str).apply(lambda x: re.search(r'\((.*?)\)', x).group(1) if '(' in x and ')' in x else x)
    df['SNOW_DEPTH'] = df['SNOW_DEPTH'].astype(str).apply(lambda x: re.search(r'(\d+(?:\.\d+)?)', x).group(0) if 'less than' in x else x)

    cols = ['AVERAGE_T_IN_C', 'MAX_T_IN_C', 'MIN_T_IN_C', 'PRECIPITATIONS_IN_MM',
            'PRESSURE_IN_HPA', 'WIND_SPEED_IN_KM_H', 'SNOW_DEPTH', 'INSOLATION_IN_HOURS']

    for col in cols:
        df[col] = df[col].replace('Tr', 0.0).replace('-', 0.0)
        df[col] = df[col].apply(lambda x: str(x).replace(' Hpa', ''))
        if col == 'SNOW_DEPTH':
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.dropna(subset=[col])
        df[col] = df[col].astype(float)

    df = df.replace(0.0, None)
    df = df.replace('', None)
    df = df.replace(0, None)

    df = pd.DataFrame({
        'STATION': df['STATION'].astype(str),
        'DATE_': pd.to_datetime(df['DATE_']).dt.date,
        'AVERAGE_T_IN_C': df['AVERAGE_T_IN_C'].astype(float),
        'MAX_T_IN_C': df['MAX_T_IN_C'].astype(float),
        'MIN_T_IN_C': df['MIN_T_IN_C'].astype(float),
        'PRECIPITATIONS_IN_MM': df['PRECIPITATIONS_IN_MM'].astype(float),
        'PRESSURE_IN_HPA': df['PRESSURE_IN_HPA'].astype(float),
        'WIND_DIRECTION_IN_A': df['WIND_DIRECTION_IN_A'].astype(str),
        'WIND_SPEED_IN_KM_H': df['WIND_SPEED_IN_KM_H'].astype(float),
        'CLOUD_COVER': df['CLOUD_COVER'].astype(str),
        'SNOW_DEPTH': df['SNOW_DEPTH'].astype(float),
        'INSOLATION_IN_HOURS': df['INSOLATION_IN_HOURS'].astype(float)
    })

    with session.begin():
        df.to_sql('al_babkina_meteostanse_new', engine, if_exists='append', index=False)
        session.commit()
    
    session.close()
    
# DATA LOADED
# ASIA 2010 - 2024, 2023
# EUROPE 6310 - 2024, 2023

for c in [6310, 2010]:
    for year in [2024, 2023]:
        if c == 6310 and year == 2024:
            months = ['05', '06', '07', '08', '09']
        elif c == 2010 and year == 2024:
            months = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
        else:
            months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        for m in months:
            if m in ['01', '03', '05', '07', '08', '10', '12']:
                last_day = 31
            elif m in ['04', '06', '09', '11']:
                last_day = 30
            elif m == '02':
                if year in [2023, 2022, 2021]:
                    last_day = 28
                elif year in [2024, 2020]:
                    last_day = 29
            # time.sleep(1)
            url = f'http://www.meteomanz.com/sy2?l=1&cou={c}&ind=00000&d1=01&m1={m}&y1={year}&d2={last_day}&m2={m}&y2={year}'
            r = req.get(url, headers=headers)
            text = r.text
            num_pages = count_pages(text)
            try:
                parse_and_save_data(text)
            except AttributeError:
                print("Table not found")
                continue
            clear_output()
            print(f'Page: 1 of {num_pages}, Month: {m} of 12, Year: {year}, Continent: {c}') 
            for n in range(2, num_pages+1):
                url = f'http://www.meteomanz.com/sy2?cou={c}&l=1&ty=hp&ind=00000&d1=01&m1={m}&y1={year}&d2={last_day}&m2={m}&y2={year}&so=102&np={n}'
                # time.sleep(1)
                r = req.get(url, headers=headers)
                text = r.text
                try:
                    parse_and_save_data(text)
                except AttributeError:
                    print("Table not found")
                    continue
                clear_output()
                print(f'Page: {n} of {num_pages}, Month: {m} of 12, Year: {year}, Continent: {c}')