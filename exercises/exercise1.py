import sqlalchemy as sal
from sqlalchemy import BIGINT, TEXT, FLOAT
import pandas as pd

datatypes = {
    'column_1' : BIGINT, 'column_2' : TEXT, 'column_3' : TEXT, 'column_4' : TEXT, 'column_5' : TEXT, 'column_6' : TEXT, 'column_7' : FLOAT, 'column_8' : FLOAT, 'column_9' : BIGINT, 'column_10' : FLOAT, 'column_11' : TEXT, 'column_12' : TEXT, 'geo_punkt' : TEXT,
}
dataframe = pd.read_csv("https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv",";")
engine = sal.create_engine("sqlite:///airports.sqlite")
dataframe.to_sql("airports", engine, dtype=datatypes, index=False, if_exists="replace")
