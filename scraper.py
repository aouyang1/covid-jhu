from datetime import datetime
from datetime import timedelta
import io
import os

from sqlalchemy import create_engine
import pandas as pd
import requests

def init_sql_conn():
    user = "root"
    if "MYSQL_PASSWORD" not in os.environ:
        raise Exception("Must provide MYSQL_PASSWORD environment variable to connect")
    pwd = os.environ["MYSQL_PASSWORD"]
    host = "localhost"
    db = "covid"
    engine = create_engine(f'mysql://{user}:{pwd}@{host}/{db}')
    conn = engine.connect()
    return conn

def scrape_from(conn, start_date, end_date=datetime.now().date()):
    columns_to_drop = ["FIPS", "Lat", "Long_", "Latitude", "Longitude", "Combined_Key"]
    column_rename = {
        "Last Update": "report_date",
        "Last_Update": "report_date",
        "City": "city",
        "Admin2": "city",
        "Province/State": "province_state",
        "Province_State": "province_state",
        "Country/Region": "country",
        "Country_Region": "country",
        "Confirmed": "confirmed",
        "Deaths": "deaths",
        "Recovered": "recovered",
        "Active": "active",
    }
    url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"

    table = "covid"

    columns = ["report_date", "city", "province_state", "country", "confirmed", "deaths", "recovered", "active"]
    df_all = pd.DataFrame(columns=columns)

    curr_date = start_date
    while curr_date <= end_date:
        # pull in the csv file from url and convert to dataframe
        date = datetime.strftime(curr_date, "%m-%d-%Y")
        full_url = url+date+".csv"
        resp = requests.get(full_url)
        if resp.status_code != 200:
            print(f'Got status code {resp.status_code} for {date}')
            curr_date += timedelta(days=1)
            continue
        df = pd.read_csv(io.StringIO(resp.content.decode('utf-8')))

        # drop columns that we're not interested in
        for c in columns_to_drop:
            if c in df.columns:
                df = df.drop(c, axis=1)

        # standardize the column names
        df = df.rename(columns=column_rename)

        # adds columns that may not be present due to file schema changes
        for c in columns:
            if c not in df.columns:
                df[c] = 0

        # try to convert report_date to a datetime
        try:
            df["report_date"] = pd.to_datetime(df["report_date"])
        except KeyError as err:
            print(f'{err} for {date}')
            continue

        # make sure the primary is unique, filter out records with 0 counts, and keep the max of identical records
        def dffilter(df):
            cond = (df["country"] != "US")
            cond = cond | (df["city"] == 0)
            cond = cond | (df["confirmed"].fillna(0)+\
                    df["deaths"].fillna(0)+\
                    df["recovered"].fillna(0)+\
                    df["active"].fillna(0) == 0)
            cond = cond | (df["province_state"].astype(str).str.contains("County|,", na=False))
            return ~(cond)

        df = df[dffilter(df)]
        df["confirmed"].astype(float)
        df["deaths"].astype(float)
        df["recovered"].astype(float)
        df["active"].astype(float)
        df = df.groupby(["report_date", "city", "province_state", "country"]) \
                .agg({
                    "confirmed": ["max"],
                    "deaths": ["max"],
                    "recovered":["max"],
                    "active":["max"],
                    }) \
                .stack() \
                .reset_index() \
                .drop(["level_4"], axis=1)

        df_all = df_all.append(df)
        curr_date += timedelta(days=1)

    df_all.sort_values(by=["city","province_state","country","report_date"], inplace=True)
    df_grouped = df_all.groupby(["city","province_state","country"])
    df_all["daily_confirmed"] =  df_grouped["confirmed"].diff()
    df_all["daily_deaths"] =  df_grouped["deaths"].diff()
    df_all["daily_recovered"] =  df_grouped["recovered"].diff()
    df_all["daily_active"] =  df_grouped["active"].diff()

    try:
        df_all.to_sql(con=conn, name=table, if_exists="replace", index=False)
    except ValueError as err:
        print(f'{err} for {date}')
    except Exception as err:
        print(f'Unknown exception{err} for {date}')

if __name__ == "__main__":
    start_date = datetime(2020, 3, 22).date()
    conn = init_sql_conn()
    scrape_from(conn, start_date)
    conn.close()
