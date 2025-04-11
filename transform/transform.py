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

def transform_regions(df, country_df):
    regions = df[['Country/Region', 'Region']].dropna().drop_duplicates()
    regions.columns = ['country_name', 'name']
    regions = regions.merge(country_df, left_on='country_name', right_on='name')
    regions = regions[['name_y', 'name']]
    regions.columns = ['country_id', 'name']
    return regions

def transform_global_data(df, disease_df):
    global_data = df[['Date', 'Cases - cumulative total', 'Deaths - cumulative total', 'Disease']].copy()
    global_data.columns = ['date', 'cases', 'deaths', 'disease_name']
    global_data = global_data.merge(disease_df, left_on='disease_name', right_on='name')
    global_data = global_data[['date', 'cases', 'deaths', 'id']]
    global_data.columns = ['date', 'cases', 'deaths', 'disease_id']
    return global_data

def get_disease_df():
    import pandas as pd
    return pd.DataFrame({'name': ['COVID-19', 'Monkeypox']})
