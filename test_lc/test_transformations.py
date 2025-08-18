import pytest  
from lib.loan.loan_transformations  import clean_loan_df, loan_points_calculation
from lib.personal.personal_transformations import clean_personal_df, personal_points_calculation
from lib.defaults.defaults_transformations import clean_defaults_df, defaults_points_calculation
from lib.inquires.inquires_transformations import clean_inquires_df, inquires_points_calculation
from lib.payment.payments_transformations import  payments_points_calculation, clean_payment_df
from lib.Transformations import calculate_final_points
from pyspark.sql.functions import col, round

def test_calculate_final_points(spark, get_raw_data):
    per_df = clean_personal_df(get_raw_data, spark)
    pt_loan_df = loan_points_calculation( clean_loan_df(get_raw_data),per_df, spark)
    pt_per_df = personal_points_calculation(clean_personal_df(get_raw_data, spark), spark)
    pt_defaults_df = defaults_points_calculation(clean_defaults_df(get_raw_data, spark), spark)
    pt_inquires_df = inquires_points_calculation(clean_inquires_df(get_raw_data, spark), spark)
    pt_payments_df = payments_points_calculation( clean_payment_df(get_raw_data), per_df,spark)

    final_df = calculate_final_points(pt_payments_df, pt_loan_df, pt_inquires_df, pt_defaults_df, pt_per_df)

    assert final_df.count() == 2257383
    assert final_df.sort('personal_member_id').select('total_points').collect() == final_df.sort('personal_member_id').select(
        round((col('total_loan_points')*0.15 + col('total_personal_points')*0.15 + col('total_defaults_points')*0.25 
               + col('total_inquires_points')*0.25 + col('total_payments_points')*0.2 ), 2)).collect()