import pandas as pd
import requests
import io
import socket
import time

from sqlalchemy import create_engine

def init_sql_conn():
    user = "root"
    host = socket.gethostname()
    db = "covid"
    engine = create_engine(f'mysql://{user}@{host}/{db}')
    conn = engine.connect()
    return conn

if __name__ == "__main__":

    conn = init_sql_conn()

    start_date = "7/10/20"

    url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series"

    datasets = {
            "deaths": "time_series_covid19_deaths_global.csv",
            "cases": "time_series_covid19_confirmed_global.csv",
            }

    columns = ["day", "region"]

    while True:
        df_all = pd.DataFrame(columns=columns)

        for dtype, fname in datasets.items():
            raw = requests.get(url+"/"+fname).content
            df = pd.read_csv(io.StringIO(raw.decode('utf-8')))
            df = df.drop(["Province/State","Lat","Long"], axis=1)
            df = df.groupby(["Country/Region"]).sum().T
            df.index = pd.to_datetime(df.index)
            df['day'] = df.index
            df.reset_index()
            df = df.loc[df.index >= start_date]
            df = pd.melt(df,
                    id_vars='day',
                    value_vars=df.columns.tolist()[:-1],
                    var_name='region',
                    value_name=dtype,
                    )
            df.sort_values(by=["region","day"], inplace=True)
            df_grouped = df.groupby(["region"])
            df["daily_"+dtype] =  df_grouped[dtype].diff()
            df_all = pd.merge(df_all, df, how='outer', on=['day','region'])

        try:
            df_all.to_sql(con=conn, name='covid_region', if_exists='append', index=False)
        except ValueError as err:
            print(f'{err} for {dtype}')
        except Exception as err:
            print(f'Unknown exception {err} for {dtype}')
        time.sleep(60*60)
