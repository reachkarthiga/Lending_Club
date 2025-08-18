import pytest
from lib.payment.payments_transformations import clean_payment_df, payments_points_calculation
from lib.personal.personal_transformations import clean_personal_df
from data_manipulation import *
from pyspark.sql.types import IntegerType, DoubleType, DateType

def test_pymnt_cleaning(get_raw_data, spark, get_expected_payment_plan):
    payment_df = clean_payment_df(get_raw_data)
    row_count = payment_df.count()
    assert row_count == 2260668

    assert payment_df.schema['total_rec_prncp'].dataType == DoubleType()
    assert payment_df.schema['total_rec_int'].dataType == DoubleType()
    assert payment_df.schema['out_prncp'].dataType == DoubleType()
    assert payment_df.schema['last_pymnt_amnt'].dataType == DoubleType()
    assert payment_df.schema['id'].dataType == IntegerType()
    assert payment_df.schema['issue_d'].dataType == DateType()
    assert payment_df.schema['total_rec_late_fee'].dataType == DoubleType()
    
    assert get_pymnt_plan(payment_df).collect() == get_expected_payment_plan.collect()

@pytest.mark.parametrize("total_rec_late_fee, late_fees_points", [
    (50000, -500.0),
    (30000, 0),
    (5000, 250),
    (10000, 100),
    (1000, 350),
    (500, 500)])
def test_installment_points(spark, get_raw_data, total_rec_late_fee, late_fees_points):
    payment_df = clean_payment_df(get_raw_data)
    pt_payment_df = payments_points_calculation(payment_df, clean_personal_df(get_raw_data, spark), spark)
    expected_count = pt_payment_df.filter((col('total_rec_late_fee') == total_rec_late_fee) & (col('late_fees_points') != late_fees_points)).count()
    assert expected_count == 0

def test_point_calculation(spark, get_raw_data):
    payment_df = clean_payment_df(get_raw_data)
    per_df = clean_personal_df(get_raw_data, spark)
    pt_payment_df = payments_points_calculation(payment_df, per_df, spark)

    assert pt_payment_df.select('total_payments_points').collect() == pt_payment_df.select(col('late_fees_points') + col('installment_points')).collect()
    assert pt_payment_df.count() == 2257383
