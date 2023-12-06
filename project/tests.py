import pipeline
import os
import sys

def test_data():
    dataframes = pipeline.get_dataframes_from_sources()
    [df_satisfaction, df_movies] = pipeline.process_dataframes(dataframes)
    test_data_shape(df_satisfaction, df_movies)
    test_data_content(df_satisfaction, df_movies)

def test_data_shape(satisfaction, movies):
    assert len(satisfaction.columns) == 8
    assert len(movies.columns) == 9

def test_data_content(satisfaction, movies):
    assert len(satisfaction) > 0
    assert len(movies) > 0

def test_system():
    dataframes = pipeline.get_dataframes_from_sources()
    dataframes = pipeline.process_dataframes(dataframes)
    engine = pipeline.prepare_pipeline_engine()
    dataframes[0].to_sql("satisfaction", engine, dtype=pipeline.SATISFACTION_DTYPE, index=False, if_exists="replace")
    dataframes[1].to_sql("movies", engine, dtype=pipeline.MOVIES_DTYPE, index=False, if_exists="replace")
    assert os.path.isfile(sys.argv[0])

def test_all():
    test_data()
    test_system()

if __name__ == '__main__':
    test_all()