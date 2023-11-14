import sqlalchemy as sal
import pandas as pd

dataframe = pd.read_csv("https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv",";")
engine = sal.create_engine("sqlite:///airports.sqlite")
dataframe.to_sql("airports", engine, index=False)
