from extract.extract import extract_data
from transform.transform import transform_continents, transform_countries, get_disease_df
from load.load import insert_dataframe, get_engine
import pandas as pd

def main():
    # Chargement
    monkeypox = extract_data("data/owid-monkeypox-data.csv")
    covid_daily = extract_data("data/worldometer_coronavirus_daily_data.csv")
    covid_latest = extract_data("data/country_wise_latest.csv")

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
