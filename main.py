import time
import sqlite3
import hashlib
import requests
import pandas as pd


def millisecond_time(seconds=None):
    return float(f'{(seconds * 1000):0.2f}')


def get_countries():
    response = requests.get('https://restcountries.com/v3.1/all')
    if response.status_code >= 500:
        raise 'Error: Problemas con el servicio, intenta mas tarde.'

    countries = []
    for country in response.json():
        capital = country.get('capital', None)
        if capital:
            if len(capital) > 1:
                capitals = ''
                for i in capital:
                    capitals += f'{i}, '
                capital = capitals
            else:
                capital = capital[0]
        countries.append(
            {
                'Capital': capital,
                'Region': country.get('region', None),
                'Language': country.get('languages', None)
            }
        )
    return countries


def get_dataframe(countries):
    df_empty = pd.DataFrame()
    for country in countries:
        start = time.perf_counter()
        language = country.get('Language', None)
        hash_language = ''
        if language:
            for key, value in language.items():
                hash_language += f'{value}, '
            hash_language = hashlib.sha1(
                str.encode(hash_language, 'utf-8')).hexdigest()

        df_empty = df_empty.append({
            'Region': country['Region'],
            'Capital': country['Capital'],
            'Language': hash_language,
            'Time': millisecond_time(seconds=time.perf_counter()-start)
        }, ignore_index=True)
    return df_empty


def put_time_spent(**kwargs):
    con = sqlite3.connect('time_spent.db')
    cur = con.cursor()
    try:
        cur.execute('''CREATE TABLE time_line
            (minimum_time float, max_time float, mean_time float, total_time float)''')
    except sqlite3.OperationalError:
        pass

    minimum = kwargs['minimum_time']
    max = kwargs['max_time']
    average = kwargs['mean_time']
    total = kwargs['total_time']

    cur.execute(f'''INSERT INTO time_line
        VALUES ({minimum}, {max}, {average}, {total})''')
    con.commit()
    query_sql = 'SELECT * FROM time_line'
    df = pd.read_sql(query_sql, con)
    con.close()
    return df


if '__main__' == '__main__':
    countries = get_countries()

    data_frame = get_dataframe(countries)
    print('Data Frame')
    print(data_frame)

    describe = data_frame['Time'].describe()
    time_spent = put_time_spent(
        minimum_time=describe['min'], max_time=describe['max'],
        mean_time=describe['mean'], total_time=describe['count'])
    print('Generate database')
    print(time_spent)

    print('Generate data.json')
    data_frame.to_json('data.json')
