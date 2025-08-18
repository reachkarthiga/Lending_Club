import pytest
from lib.DataReader import read_data
from test_lc.conftest import get_raw_data as get_raw_data
from pyspark.sql.functions import col, round
from data_manipulation import get_home_ownership, get_state_length, get_max_inc, get_grade, get_sub_grade
from lib.personal.personal_transformations import clean_personal_df, personal_points_calculation
from pyspark.sql.types import BooleanType

env = 'LOCAL'


def test_read_function(spark):
    row_count = read_data(spark, env).count()
    assert row_count == 2260701


def test_personal_df_cleaning(get_raw_data, spark, get_expected_home_ownership, get_expected_grades, get_expected_sub_grades):
    per_df = clean_personal_df(get_raw_data, spark)
    row_count = per_df.count()
    assert row_count == 2257383

    assert get_expected_home_ownership.collect() == get_home_ownership(per_df).collect()

    pt_per_df = personal_points_calculation(per_df, spark)
    assert pt_per_df.count() == 2257383

    assert per_df.schema['verification_status'].dataType  == BooleanType() 
    assert get_state_length(per_df) == 0
    assert get_max_inc(per_df) == 1000000.0

    assert get_grade(per_df).collect() == get_expected_grades.collect()
    assert get_sub_grade(per_df).collect() == get_expected_sub_grades.collect()


@pytest.mark.parametrize("grade, points", [
    ('A1', 200.0),
    ('A3', 175.0),
    ('B2', 155.0),
    ('D4', 95.0),
    ('E1', 85.0),
    ('F5', 40.0),
    ('G3', 25.0)
])
def test_sub_grade_points(spark, get_raw_data, grade, points):
    get_pt_per_df = personal_points_calculation(clean_personal_df(get_raw_data, spark), spark)
    expected_count = get_pt_per_df.filter((col('sub_grade') == grade) & (col('grade_points') != points)).count()
    assert expected_count == 0


@pytest.mark.parametrize("home_ownership, points", [
    ('OWN', 350.0),
    ('RENT', 250.0),
    ('MORTAGE', 100.0)])
def test_home_ownership_points(spark, get_raw_data, home_ownership, points):
    get_pt_per_df = personal_points_calculation(clean_personal_df(get_raw_data, spark), spark)
    expected_count = get_pt_per_df.filter((col('home_ownership') == home_ownership) & (col('home_points') != points)).count()
    assert expected_count == 0


@pytest.mark.parametrize("emp_length, points", [
    (0, -500.0),
    (1, 0.0),
    (3, 100.0),
    (5, 250.0),
    (7, 350.0),
    (9, 500.0)])
def test_emp_length_points(spark, get_raw_data, emp_length, points):
    get_pt_per_df = personal_points_calculation(clean_personal_df(get_raw_data, spark), spark)
    expected_count = get_pt_per_df.filter((col('emp_length') == emp_length) & (col('exp_points') != points)).count()
    assert expected_count == 0


@pytest.mark.parametrize("verification_status, points", [
    (True, 350.0),
    (False, 100.0)])
def test_verification_status_points(spark, get_raw_data, verification_status, points):
    get_pt_per_df = personal_points_calculation(clean_personal_df(get_raw_data, spark), spark)
    expected_count = get_pt_per_df.filter((col('verification_status') == verification_status) & (col('verification_points') != points)).count()
    assert expected_count == 0


def test_personal_points_calculation(spark, get_raw_data):

    get_pt_per_df = personal_points_calculation(clean_personal_df(get_raw_data, spark), spark)
    assert get_pt_per_df.count() == 2257383
    assert get_pt_per_df.select('total_personal_points').collect() == get_pt_per_df.select(round((col('home_points') + col('exp_points') + col('verification_points') + col('grade_points')), 2)).collect()

