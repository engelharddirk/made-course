import sqlalchemy as sal
from sqlalchemy import BIGINT, TEXT, FLOAT, INTEGER, CHAR
import pandas as pd
import sys

FILEPATH = sys.argv[1]
SATISFACTION_SOURCE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ilc_pw01/?format=TSV&compressed=false"
MOVIES_SOURCE = "https://datasets.imdbws.com/title.basics.tsv.gz"

MOVIES_DTYPE = {
    'tconst': TEXT,
    'titleType': TEXT,
    'primaryTitle': TEXT,
    'originalTitle': TEXT,
    'isAdult': TEXT,
    'startYear': INTEGER,
    'endYear': INTEGER,
    'runtimeMinutes': INTEGER,
    'genres': TEXT
}

SATISFACTION_DTYPE = {
    'indic_wb': TEXT,
    'sex': CHAR,
    'age': TEXT,
    'country': TEXT,
    '2013': TEXT,
    '2018': TEXT,
    '2021': TEXT,
    '2022': TEXT
}

def prepare_pipeline_engine():
    engine = sal.create_engine(f"sqlite://{FILEPATH}")
    return engine

def get_dataframes_from_sources():
    df_list = [
        pd.read_csv(SATISFACTION_SOURCE, "\t|,"),
        pd.read_csv(MOVIES_SOURCE, "\t")
    ]
    return df_list

def process_dataframes(dataframes):
    df_satisfaction_only_lifesat = dataframes[0][dataframes[0]["indic_wb"].str.contains("LIFESAT")]
    df_satisfaction_only_lifesat = df_satisfaction_only_lifesat.drop(columns=['freq', 'unit', 'isced11'])
    df_satisfaction_only_lifesat = df_satisfaction_only_lifesat.rename(columns={"geo\\TIME_PERIOD": "country"})
    return [df_satisfaction_only_lifesat, dataframes[1]]

def save_to_sql(engine, dataframes):
    dataframes[0].to_sql("satisfaction", engine, dtype=SATISFACTION_DTYPE, index=False, if_exists="replace")
    dataframes[1].to_sql("movies", engine, dtype=MOVIES_DTYPE, index=False, if_exists="replace")


def main():
    engine = prepare_pipeline_engine()
    dataframes = get_dataframes_from_sources()
    dataframes = process_dataframes(dataframes)
    save_to_sql(engine, dataframes)


if __name__ == "__main__":
    main()