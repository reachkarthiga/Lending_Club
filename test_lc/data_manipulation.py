from pyspark.sql.functions import *

def get_home_ownership(per_df):
    return per_df.select('home_ownership').distinct().sort('home_ownership')

def get_loan_status(loan_Df):
    return loan_Df.select('loan_status').filter(col('loan_status').isNotNull()).distinct().sort('loan_status')

def get_purpose(loan_df):
    return loan_df.select('purpose').distinct().sort('purpose')

def get_max_inc(per_df):
    return per_df.agg(max('annual_inc')).first()[0]

def get_state_length(per_df):
    return per_df.filter((col('addr_state').isNotNull()) & (length(col("addr_state")) != 2)).count()

def get_grade(per_df):
    return per_df.select('grade').distinct().sort('grade')

def get_sub_grade(per_df):
    return per_df.select('sub_grade').distinct().sort('sub_grade')

def get_max_chargeoff_within_12_mths(inq_df):
    return inq_df.agg(max('chargeoff_within_12_mths')).first()[0]

def get_max_inq_last_12m(inq_df):
    return inq_df.agg(max('inq_last_12m')).first()[0]

def get_max_inq_fi(inq_df):
    return inq_df.agg(max('inq_fi')).first()[0]

def get_max_mort_acc(def_df):
    return def_df.agg(max('mort_acc')).first()[0]

def get_max_pub_rec_bankruptcies(def_df):
    return def_df.agg(max('pub_rec_bankruptcies')).first()[0]

def get_max_delinq_2yrs(def_df):
    return def_df.agg(max('delinq_2yrs')).first()[0]

def get_max_acc_now_delinq(def_df):
    return def_df.agg(max('acc_now_delinq')).first()[0]

def get_pymnt_plan(payment_df):
    return payment_df.select('pymnt_plan').filter(col('pymnt_plan').isNotNull()).distinct().sort('pymnt_plan')

def check_grade_sub_grade_points(per_pt_df, grade, points):
    return 