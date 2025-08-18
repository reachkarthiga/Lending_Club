import pytest
from lib.loan.loan_transformations import loan_points_calculation, clean_loan_df
from lib.personal.personal_transformations import clean_personal_df
from data_manipulation import *
from pyspark.sql.types import IntegerType, DateType

from test_lc.conftest import get_expected_loan_status


def test_loan_cleaning(get_raw_data, spark, get_expected_loan_status, get_expected_purpose):
    loan_df = clean_loan_df(get_raw_data)
    row_count = loan_df.count()
    assert row_count == 2260668

    assert loan_df.schema['id'].dataType == IntegerType()
    assert loan_df.schema['term'].dataType == IntegerType()
    assert loan_df.schema['issue_d'].dataType == DateType()

    assert get_expected_purpose.collect() == get_purpose(loan_df).collect()
    assert get_expected_loan_status.collect() == get_loan_status(loan_df).collect()
    
@pytest.mark.parametrize("loan_status, points", [
    ('Fully Paid', 500.0),
    ('Current', 350.0),
    ('Late', 250.0),
    ('Charged Off', -500.0),
    ('In Grace Period', 100.0),
    ('Default', 0.0)])
def test_loan_status_points(spark, get_raw_data, loan_status, points):
    get_pt_loan_df = loan_points_calculation(clean_loan_df(get_raw_data), clean_personal_df(get_raw_data, spark), spark)
    expected_count = get_pt_loan_df.filter((col('loan_status') == loan_status) & (col('loan_status_points') != points)).count()
    assert expected_count == 0          

@pytest.mark.parametrize("purpose, points", [
    ('debt_consolidation', -500.0),
    ('credit_card', -500.0),
    ('home_improvement', 250.0),
    ('major_purchase', 250.0),          
    ('small_business', 250.0),
    ('education', 500.0),
    ('house', 500.0)])
def test_purpose_points(spark, get_raw_data, purpose, points):
    get_pt_loan_df = loan_points_calculation(clean_loan_df(get_raw_data), clean_personal_df(get_raw_data, spark), spark)
    expected_count = get_pt_loan_df.filter((col('purpose') == purpose) & (col('purpose_points') != points)).count()
    assert expected_count == 0

def test_point_calculation(spark, get_raw_data):
    loan_df = clean_loan_df(get_raw_data)
    per_df = clean_personal_df(get_raw_data, spark)
    pt_loan_df = loan_points_calculation(loan_df, per_df, spark)

    assert pt_loan_df.select('total_loan_points').collect() == pt_loan_df.select(
        col('loan_status_points') + col('purpose_points')).collect()
    assert pt_loan_df.count() == 2257383 


