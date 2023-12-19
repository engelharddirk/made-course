import pipeline
import os
import sys
import pandas as pd
import datetime as dt
from sqlalchemy import BIGINT, TEXT, FLOAT, INTEGER, CHAR, BOOLEAN, null, DATE

dummy_movie_data = pd.DataFrame(
    [["59171","The Girl","\\N","2012-10-06"],
     ["59172","One Week","\\N","1920-09-01"],
     ["59173","Neighbors","\\N","1920-12-22"]],
    columns=["id","name","parent_id","date"]
)
dummy_satisfaction_data = pd.DataFrame(
    [["A","RTG","ED0-2","LIFESAT","T","Y_GE16","BE",7.3,7.2,7.2,7.2],
     ["A","RTG","ED0-2","LIFESAT","T","Y_GE16","BG","3.8 ",4.3,5.0,4.7],
     ["A","RTG","ED0-2","LIFESAT","T","Y_GE16","CH",7.9,7.6,7.7,":"]],
    columns=["freq","unit","isced11","indic_wb","sex","age","geo\\TIME_PERIOD","2013","2018","2021","2022"]
)
dummy_category_data = pd.DataFrame(
    [["20143","50"],
     ["20144","80"],
     ["20146","10281"]],
     columns=["movie_id","category_id"]
)
dummy_category_name_data = pd.DataFrame(
    [["35","Comedy","en"],
     ["37","Western","de"],
     ["71","Familiendrama","de"]],
     columns=["category_id","name","language_iso_639_1"]
)

expexted_movie_df = pd.DataFrame(
    [[59171,"The Girl",dt.datetime(2012, 10, 6)],
     [59172,"One Week",dt.datetime(1920, 9, 1)],
     [59173,"Neighbors",dt.datetime(1920, 12, 22)]],
    columns=["id","name","date"],
)
expected_satisfaction_df = pd.DataFrame(
    [["BE",7.3,7.2,7.2,7.2],
     ["BG",3.8,4.3,5.0,4.7],
     ["CH",7.9,7.6,7.7,None]],
     columns=["country","2013","2018","2021","2022"]
)

def test_data():
    dataframes = [dummy_satisfaction_data, dummy_movie_data, dummy_category_data, dummy_category_name_data]
    dataframes = pipeline.process_dataframes(dataframes)
    test_data_shape(dataframes)
    test_data_content(dataframes)
    return dataframes

def test_data_shape(dataframes):
    assert len(dataframes[0].columns) == 5
    assert len(dataframes[1].columns) == 3
    pd.testing.assert_frame_equal(dataframes[1], expexted_movie_df, check_dtype=False)
    assert len(dataframes[2].columns) == 2
    assert len(dataframes[3].columns) == 3

def test_data_content(dataframes):
    assert len(dataframes[0]) > 0
    pd.testing.assert_frame_equal(dataframes[0], expected_satisfaction_df, check_dtype=False)
    assert len(dataframes[1]) > 0
    pd.testing.assert_frame_equal(dataframes[1], expexted_movie_df, check_dtype=False)
    assert len(dataframes[2]) > 0
    assert len(dataframes[3]) > 0

def test_system(dataframes):
    engine = pipeline.prepare_pipeline_engine()
    pipeline.save_to_sql(engine, dataframes)
    assert os.path.isfile(sys.argv[0])

def test_all():
    dataframes = test_data()
    test_system(dataframes)

if __name__ == '__main__':
    test_all()