from sqlalchemy import create_engine
import pandas as pd

def get_engine():
    return create_engine("mysql+pymysql://root:@localhost:3306/MSPR")

def insert_dataframe(df, table_name):
    engine = get_engine()
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

def get_table(engine, table_name):
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)

