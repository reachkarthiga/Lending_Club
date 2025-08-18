import pytest
from lib.Utils import getSpark
from lib.Transformations import assign_member_id
from lib.personal.personal_transformations import personal_points_calculation
from lib.personal.personal_transformations import clean_personal_df
from lib.DataReader import read_data

env = 'LOCAL'

@pytest.fixture(scope='session')
def spark():
    spark_session =  getSpark(env)
    yield spark_session
    spark_session.stop()

@pytest.fixture(scope='session')
def get_raw_data(spark):
    df = read_data(spark, env)
    return assign_member_id(df)

@pytest.fixture(scope='session')        
def get_expected_home_ownership(spark):
    return spark.read.format('csv').load('data/test_results/valid_home_ownership.csv')

@pytest.fixture(scope='session')        
def get_expected_loan_status(spark):
    return spark.read.format('csv').option('header', True).load('data/test_results/valid_loan_status.csv')

@pytest.fixture(scope='session')        
def get_expected_purpose(spark):
    return spark.read.format('csv').load('data/test_results/valid_purpose.csv')

@pytest.fixture(scope='session')        
def get_expected_grades(spark):
    return spark.read.format('csv').load('data/test_results/valid_grades.csv')  

@pytest.fixture(scope='session')        
def get_expected_sub_grades(spark):
    return spark.read.format('csv').load('data/test_results/valid_sub_grades.csv')

@pytest.fixture(scope='session')
def get_expected_payment_plan(spark):
    return spark.read.format('csv').load('data/test_results/valid_payment_plan.csv')

@pytest.fixture(scope='session')
def get_per_df(spark):
    return clean_personal_df(get_raw_data, spark)