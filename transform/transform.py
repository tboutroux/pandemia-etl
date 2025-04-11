def transform_continents(df):
    return df[['Continent']].dropna().drop_duplicates().rename(columns={'Continent': 'name'})

def transform_countries(df, continent_df):
    countries = df[['Country/Region', 'ISO3', 'Population', 'Continent']].copy()
    countries.columns = ['name', 'code3', 'population', 'continent_name']
    countries = countries.merge(continent_df, left_on='continent_name', right_on='name', suffixes=('', '_continent'))
    countries = countries[['name', 'code3', 'population', 'id']]
    countries.columns = ['name', 'code3', 'population', 'continent_id']
    countries['iso2'] = None
    countries['who_region'] = None
    return countries

def get_disease_df():
    import pandas as pd
    return pd.DataFrame({'name': ['COVID-19', 'Monkeypox']})
