from pyspark.sql.functions import col, sha2, concat_ws
from lib.Utils import create_temp_view


def assign_member_id(lending_club_raw):
    return lending_club_raw.withColumn("member_id",
                                       sha2(concat_ws('-', col("grade"), col("sub_grade"), col("emp_title"),
                                                      col("emp_length"), col(
                                                          "verification_status"), col("home_ownership"),
                                                      col("annual_inc"), col("zip_code"), col("addr_state")), 512))

def make_df_sql_tables(loan_df, personal_df, payment_df, defaults_df, inquires_df):
    create_temp_view(loan_df, name='loan_tbl')
    create_temp_view(personal_df, name='pers_tbl')
    create_temp_view(payment_df, name='pymt_tbl')
    create_temp_view(defaults_df, name='dfts_tbl')
    create_temp_view(inquires_df, name='inqs_tbl')


def calculate_final_points(payments_points_df, loan_points_df, inquires_points_df, defaults_points_df, personal_points_df):

    payments_points_df = payments_points_df.select('payment_member_id', 'total_payments_points')
    loan_points_df = loan_points_df.select('loan_member_id', 'total_loan_points')
    inquires_points_df = inquires_points_df.select('inquires_member_id', 'total_inquires_points')
    defaults_points_df = defaults_points_df.select('defaults_member_id', 'total_defaults_points')
    personal_points_df = personal_points_df.select('personal_member_id', 'total_personal_points')

    points_df = personal_points_df.join(inquires_points_df, inquires_points_df.inquires_member_id == personal_points_df.personal_member_id, 'inner')\
        .join(defaults_points_df, defaults_points_df.defaults_member_id == personal_points_df.personal_member_id, 'inner')\
        .join(loan_points_df, loan_points_df.loan_member_id == personal_points_df.personal_member_id, 'inner')\
        .join(payments_points_df, payments_points_df.payment_member_id == personal_points_df.personal_member_id, 'inner')

    points_df = points_df.withColumn('total_points', ((col('total_payments_points') * 0.2) + (col('total_defaults_points') * 0.25) +
                                                      (col('total_inquires_points') * 0.25) + (col('total_loan_points') * 0.15) +
                                                      (col('total_personal_points') * 0.15)))
    
    return points_df.select('personal_member_id', 'total_points','total_payments_points', \
                            'total_inquires_points', 'total_personal_points', 'total_defaults_points', 'total_loan_points' )
