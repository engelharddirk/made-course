import pipeline
import os
import sys

def test_data():
    dataframes = pipeline.get_dataframes_from_sources()
    [df_satisfaction, df_movies] = pipeline.process_dataframes(dataframes)
    test_data_shape(df_satisfaction, df_movies)
    test_data_content(df_satisfaction, df_movies)
    return [df_satisfaction, df_movies]

def test_data_shape(satisfaction, movies):
    assert len(satisfaction.columns) == 8
    assert len(movies.columns) == 9

def test_data_content(satisfaction, movies):
    assert len(satisfaction) > 0
    assert len(movies) > 0

def test_system(dataframes):
    engine = pipeline.prepare_pipeline_engine()
    pipeline.save_to_sql(engine, dataframes)
    assert os.path.isfile(sys.argv[0])

def test_all():
    dataframes = test_data()
    test_system(dataframes)

if __name__ == '__main__':
    test_all()