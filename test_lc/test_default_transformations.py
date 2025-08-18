import pytest
from data_manipulation import *
from conftest import *
from lib.defaults.defaults_transformations import defaults_points_calculation, clean_defaults_df
from pyspark.sql.types import IntegerType

def test_def_cleaning(get_raw_data, spark):
    def_df = clean_defaults_df(get_raw_data, spark)
    row_count = def_df.count()
    assert row_count == 2257383

    assert def_df.schema['mort_acc'].dataType == IntegerType()
    assert def_df.schema['delinq_2yrs'].dataType == IntegerType()
    assert def_df.schema['acc_now_delinq'].dataType == IntegerType()
    assert def_df.schema['pub_rec_bankruptcies'].dataType == IntegerType()

    assert get_max_mort_acc(def_df) == 15
    assert get_max_acc_now_delinq(def_df) == 5
    assert get_max_pub_rec_bankruptcies(def_df) == 10
    assert get_max_delinq_2yrs(def_df) == 24

   
@pytest.mark.parametrize("mort_acc, points", [
    (0, 500.0),
    (1, 250.0),
    (2, 100.0),
    (5, -500.0),
    (3, -500.0)])
def test_mort_acc_points(spark, get_raw_data, mort_acc, points):
    get_pt_def_df = defaults_points_calculation(clean_defaults_df(get_raw_data, spark), spark)
    expected_count = get_pt_def_df.filter((col('mort_acc') == mort_acc) & (col('mort_points') != points)).count()
    assert expected_count == 0  

@pytest.mark.parametrize("delinq_2yrs,acc_now_delinq, points", [
    (0, 0, 500.0),
    (0, 1, 350.0),
    (1, 0, 350.0),
    (1, 1, 250.0),
    (2, 1, 100.0)])
def test_delinq_2yrs_points(spark, get_raw_data, delinq_2yrs, acc_now_delinq, points):
    get_pt_def_df = defaults_points_calculation(clean_defaults_df(get_raw_data, spark), spark)
    expected_count = get_pt_def_df.filter((col('delinq_2yrs') == delinq_2yrs) & (col('acc_now_delinq') == acc_now_delinq) & (col('deliq_points') != points)).count()
    assert expected_count == 0

@pytest.mark.parametrize("pub_rec_bankruptcies, points", [
    (0, 500.0),
    (1, 250.0),
    (3, -500.0),
    (5, -500.0)])   
def test_pub_rec_bankruptcies_points(spark, get_raw_data, pub_rec_bankruptcies, points):    
    get_pt_def_df = defaults_points_calculation(clean_defaults_df(get_raw_data, spark), spark)
    expected_count = get_pt_def_df.filter((col('pub_rec_bankruptcies') == pub_rec_bankruptcies) & (col('public_rec_points') != points)).count()
    assert expected_count == 0

def test_point_calculation(spark, get_raw_data):
    def_df = clean_defaults_df(get_raw_data, spark)
    pt_def_df = defaults_points_calculation(def_df, spark)

    assert pt_def_df.select('total_defaults_points').collect() == pt_def_df.select(
        col('mort_points') + col('deliq_points') + col('public_rec_points')).collect()
    assert pt_def_df.count() == 2257383