import pandas as pd

def load_monkeypox_data(path):
    return pd.read_csv(path)

def load_covid_daily_data(path):
    return pd.read_csv(path)

def load_covid_latest_excel(path):
    return pd.read_excel(path)
