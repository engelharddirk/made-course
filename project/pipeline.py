import sqlalchemy as sal
from sqlalchemy import BIGINT, TEXT, FLOAT, INTEGER, CHAR, BOOLEAN, null, DATE
import pandas as pd
import sys

FILEPATH = sys.argv[1]
SATISFACTION_SOURCE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ilc_pw01/?format=TSV&compressed=false"
LIFE_LADDER_SOURCE = 'https://happiness-report.s3.amazonaws.com/2023/DataForTable2.1WHR2023.xls'
MOVIES_SOURCE = "http://www.omdb.org/data/all_movies.csv.bz2"
MOVIES_GENRE_SOURCE = "http://www.omdb.org/data/movie_categories.csv.bz2"
MOVIES_GENRE_NAME_SOURCE = "http://www.omdb.org/data/category_names.csv.bz2"
MOVIES_COUNTRY_SOURCE = "http://www.omdb.org/data/movie_countries.csv.bz2"

MOVIES_DTYPE = {
    'id': INTEGER,
    'name': TEXT,
    'date': DATE,
}

MOVIES_GENRE_DTYPE = {
    'movie_id': INTEGER,
    'category_id': INTEGER,
}

MOVIE_COUNTRY_DTYPE = {
    'movie_id': INTEGER,
    'country_code': TEXT
}

MOVIES_GENRE_NAMES_DTYPE = {
    'category_id': INTEGER,
    'name': TEXT,
    'language_iso_639_1': TEXT
}

SATISFACTION_DTYPE = {
    'indic_wb': TEXT,
    'sex': CHAR,
    'age': TEXT,
    'country': TEXT,
    '2013': FLOAT,
    '2018': FLOAT,
    '2021': FLOAT,
    '2022': FLOAT
}

LIFE_LADDER_DTYPE = {
    'Country name': TEXT,
    'year': INTEGER,
    'Life Ladder': FLOAT
}

def prepare_pipeline_engine():
    engine = sal.create_engine(f"sqlite://{FILEPATH}")
    return engine

def get_dataframes_from_sources():
    df_movies = pd.read_csv(MOVIES_SOURCE, sep=",", escapechar="\\", engine='python')
    df_movies_categories = pd.read_csv(MOVIES_GENRE_SOURCE, sep=",", engine='python')
    df_movies_category_names = pd.read_csv(MOVIES_GENRE_NAME_SOURCE, sep=",")
    df_movies_category_names = df_movies_category_names.loc[df_movies_category_names['language_iso_639_1'] == 'en']
    df_movies_countries = pd.read_csv(MOVIES_COUNTRY_SOURCE, sep=",")
    df_life_ladder = pd.read_excel('https://happiness-report.s3.amazonaws.com/2023/DataForTable2.1WHR2023.xls', decimal=',')
    df_list = [
        pd.read_csv(SATISFACTION_SOURCE, sep="\t|,", engine='python'),
        df_movies,
        df_movies_categories,
        df_movies_category_names,
        df_movies_countries,
        df_life_ladder,
    ]
    return df_list

def process_dataframes(dataframes):
    df_movies = dataframes[1]
    df_life_ladder = dataframes[5]
    df_life_ladder = df_life_ladder[['Country name', 'year', 'Life Ladder']]
    df_satisfaction = dataframes[0][dataframes[0]["indic_wb"].str.contains("LIFESAT")]
    df_satisfaction = df_satisfaction[df_satisfaction["age"] == "Y_GE16"]
    df_satisfaction = df_satisfaction[df_satisfaction["sex"] == "T"]
    df_satisfaction = df_satisfaction.drop(columns=['freq', 'unit', 'isced11', 'age', 'sex', 'indic_wb'])
    df_satisfaction = df_satisfaction.rename(
    columns={
        "geo\\TIME_PERIOD": "country", 
        "2013 ": "2013", 
        "2018 ": "2018", 
        "2021 ": "2021", 
        "2022 ": "2022"
        }
    )
    df_satisfaction['2013'] = df_satisfaction['2013'].replace(r'^(?![0-9]*\.[0-9] $).*', None, regex=True).astype(float) # regex: everything that
    df_satisfaction['2018'] = df_satisfaction['2018'].replace(r'^(?![0-9]*\.[0-9] $).*', None, regex=True).astype(float) # is not a float 0.0<10.0
    df_satisfaction['2021'] = df_satisfaction['2021'].replace(r'^(?![0-9]*\.[0-9] $).*', None, regex=True).astype(float) # in str representation
    df_satisfaction['2022'] = df_satisfaction['2022'].replace(r'^(?![0-9]*\.[0-9]$).*', None, regex=True).astype(float)
    df_movies = df_movies.drop('parent_id', axis=1)
    df_movies['id'] = df_movies['id'].astype(int)
    df_movies['date'] = df_movies['date'].replace('N', None)
    df_movies['date'] = pd.to_datetime(df_movies['date'])
    
    return [df_satisfaction, df_movies, dataframes[2], dataframes[3], dataframes[4], df_life_ladder]

def save_to_sql(engine, dataframes):
    dataframes[0].to_sql("satisfaction", engine, dtype=SATISFACTION_DTYPE, index=False, if_exists="replace")
    dataframes[1].to_sql("movies", engine, dtype=MOVIES_DTYPE, index=False, if_exists="replace")
    dataframes[2].to_sql("movie_categories", engine, dtype=MOVIES_GENRE_DTYPE, index=False, if_exists="replace")
    dataframes[3].to_sql("movie_category_names", engine, dtype=MOVIES_GENRE_NAMES_DTYPE, index=False, if_exists="replace")
    dataframes[4].to_sql("movie_countries", engine, dtype=MOVIE_COUNTRY_DTYPE, index=False, if_exists="replace")
    dataframes[5].to_sql("life_ladder", engine, dtype=LIFE_LADDER_DTYPE, index=False, if_exists="replace")


def main():
    engine = prepare_pipeline_engine()
    dataframes = get_dataframes_from_sources()
    dataframes = process_dataframes(dataframes)
    save_to_sql(engine, dataframes)


if __name__ == "__main__":
    main()