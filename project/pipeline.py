import sqlalchemy as sal
from sqlalchemy import BIGINT, TEXT, FLOAT
import pandas as pd

dataframe_satisfaction = pd.read_csv("https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ilc_pw01/?format=TSV&compressed=false","\t")
engine = sal.create_engine("sqlite:///data/projectdata.sqlite")
dataframe_satisfaction.to_sql("satisfaction", engine, index=False, if_exists="replace")
dataframe_movies = pd.read_csv("https://datasets.imdbws.com/title.basics.tsv.gz","\t")
dataframe_movies.to_sql("movies", engine, index=False, if_exists="replace")