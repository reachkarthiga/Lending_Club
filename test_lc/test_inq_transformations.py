import pytest
from data_manipulation import *
from conftest import * 
from lib.inquires.inquires_transformations import inquires_points_calculation, clean_inquires_df
from pyspark.sql.types import IntegerType, LongType

def test_inq_cleaning(get_raw_data, spark):
    inq_df = clean_inquires_df(get_raw_data, spark)
    row_count = inq_df.count()
    assert row_count == 2257383

    assert inq_df.schema['mnths_since_inq'].dataType == IntegerType()
    assert inq_df.schema['inq_last_12m'].dataType == IntegerType()
    assert inq_df.schema['inq_fi'].dataType == IntegerType()
    assert inq_df.schema['chargeoff_within_12_mths'].dataType == LongType()

    assert get_max_chargeoff_within_12_mths(inq_df) == 10
    assert get_max_inq_last_12m(inq_df) == 100
    assert get_max_inq_fi(inq_df) == 50

@pytest.mark.parametrize("inq_last_12m, points", [
    (0, 500.0),
    (1, 350.0),
    (2, 250.0),
    (3, 100.0),
    (4, 0.0),
    (7, 0.0)])
def test_inq_points(spark, get_raw_data, inq_last_12m, points):
    get_pt_inq_df = inquires_points_calculation(clean_inquires_df(get_raw_data, spark), spark)
    expected_count = get_pt_inq_df.filter((col('inq_last_12m') == inq_last_12m) & (col('inq_12m_points') != points)).count()
    assert expected_count == 0

@pytest.mark.parametrize("inq_fi, points", [
    (0, 500.0),
    (1, 350.0),
    (2, 250.0),
    (3, 100.0),
    (4, 0.0),
    (7, 0.0)])
def test_inq_fi_points(spark, get_raw_data, inq_fi, points):
    get_pt_inq_df = inquires_points_calculation(clean_inquires_df(get_raw_data, spark), spark)
    expected_count = get_pt_inq_df.filter((col('inq_fi') == inq_fi) & (col('inq_points') != points)).count()
    assert expected_count == 0

@pytest.mark.parametrize("mnths_since_inq, points", [
    (0, 500.0),
    (1, 350.0),     
    (3, 250.0),
    (4, 100.0),
    (7, 0.0)])
def test_mnths_since_inq_points(spark, get_raw_data, mnths_since_inq, points):
    get_pt_inq_df = inquires_points_calculation(clean_inquires_df(get_raw_data, spark), spark)
    expected_count = get_pt_inq_df.filter((col('mnths_since_inq') == mnths_since_inq) & (col('mnth_inq_points') != points)).count()
    assert expected_count == 0

@pytest.mark.parametrize("chargeoff_within_12_mths, points", [
    (0, 500.0),
    (1, -500.0),
    (3, -500.0),
    (4, -500.0),
    (7, -500.0)])
def test_chargeoff_within_12_mths_points(spark, get_raw_data, chargeoff_within_12_mths, points):
    get_pt_inq_df = inquires_points_calculation(clean_inquires_df(get_raw_data, spark), spark)
    expected_count = get_pt_inq_df.filter((col('chargeoff_within_12_mths') == chargeoff_within_12_mths) & (col('charge_off_points') != points)).count()
    assert expected_count == 0

def test_point_calculation(spark, get_raw_data):
    inq_df = clean_inquires_df(get_raw_data, spark)
    pt_inq_df = inquires_points_calculation(inq_df, spark)

    assert pt_inq_df.select('total_inquires_points').collect() == pt_inq_df.select(
        col('inq_12m_points') + col('inq_points') + col('mnth_inq_points') + col('charge_off_points')).collect()
    assert pt_inq_df.count() == 2257383
