from extract.extra import load_monkeypox_data, load_covid_daily_data, load_covid_latest_excel
from transform.transform import transform_continents, transform_countries, get_disease_df
from load.load import insert_dataframe, get_engine

def main():
    # Chargement
    monkeypox = load_monkeypox_data("data/owid-monkeypox-data.csv")
    covid_daily = load_covid_daily_data("data/worldometer_coronavirus_daily_data.csv")
    covid_latest = load_covid_latest_excel("data/country_wise_latest.csv.xls")

    # Transformation
    continents = transform_continents(covid_latest)
    insert_dataframe(continents, "Continent")

    continent_df = get_engine().execute("SELECT id, name FROM Continent").fetchall()
    continent_df = pd.DataFrame(continent_df, columns=["id", "name"])

    countries = transform_countries(covid_latest, continent_df)
    insert_dataframe(countries, "Country")

    diseases = get_disease_df()
    insert_dataframe(diseases, "Disease")

    print("Import terminé.")

if __name__ == "__main__":
    main()
