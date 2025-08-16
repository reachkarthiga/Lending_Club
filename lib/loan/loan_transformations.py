
from pyspark.sql.functions import expr, col, current_timestamp, to_date, when, substring, lit
from pyspark.sql.types import IntegerType


def get_loan_details(lending_club_raw):
    return lending_club_raw.select('id', 'member_id', 'loan_amnt', 'term', 'int_rate',
                                   'installment', 'issue_d', 'loan_status', 'purpose')


def clean_loan_df(loan_df):
    loan_clean_df = rename_col(loan_df)
    loan_clean_df = remove_bad_data(loan_clean_df)
    loan_clean_df = change_col_datatypes(loan_clean_df)
    loan_clean_df = remove_nulls(loan_clean_df)
    loan_clean_df = change_col_outliers(loan_clean_df)
    loan_clean_df = ingest_timestamp(loan_clean_df)
    return loan_clean_df

def rename_col(loan_df):
    return loan_df.withColumnRenamed('member_id', 'loan_member_id')

def remove_duplicates(loan_df, spark):
    loan_df.createOrReplaceTempView('loan_tbl')
    return spark.sql(""" With cte_1 as (
                    select *, row_number() over(partition by loan_member_id order by issue_d desc) as rn 
                    from loan_tbl )
                    select id as id, loan_member_id as loan_member_id, loan_amnt as loan_amnt, term as term, int_rate as int_rate, 
                    installment as installment, issue_d as issue_d, loan_status as loan_status, purpose as purpose from cte_1 where rn = 1 """)



def change_col_outliers(loan_df):
    return loan_df.withColumn("loan_status", when(col("loan_status").isin(["Does not meet the credit policy. Status:Fully Paid", "Fully Paid"]), "Full Paid").
                              when(col("loan_status").isin(["Does not meet the credit policy. Status:Charged Off", "Charged Off"]), "Charged Off").
                              when(col("loan_status").isin(["Late (31-120 days)", "Late (16-30 days)"]), "Late").
                              when(col("loan_status").isin(["Current", "In Grace Period", "Default"]), col("loan_status")).otherwise(lit(None)))\
        .withColumn("purpose", when(col('purpose')
                                    .isin(['debt_consolidation', 'credit_card', 'home_improvement', 'major_purchase', 'medical', 'small_business', 'car', 'vacation', 'moving', 'house', 'wedding', 'renewable_energy', 'educational']),
                                    col('purpose')).otherwise("other"))\



def change_col_datatypes(loan_df):
    return loan_df.withColumn("issue_d", to_date(col("issue_d"), "MMM-yyyy"))\
        .withColumn("term", substring(col("term"), 0, 3).cast(IntegerType()))\
        .withColumn("id", col("id").cast(IntegerType()))


def remove_nulls(loan_df):
    return loan_df.fillna(0, subset=['loan_amnt', "int_rate", 'installment'])


def remove_bad_data(loan_df):
    return loan_df.where(expr(""" loan_member_id not like 'cf83e1357eefb8bdf%' """))


def ingest_timestamp(loan_df):
    return loan_df.withColumn("ingestion_date", current_timestamp())


def loan_points_calculation(loan_df, personal_df, spark):

    points_good = spark.conf.get('spark.sql.good')
    points_ok = spark.conf.get('spark.sql.ok')
    points_very_good = spark.conf.get('spark.sql.very_good')
    points_very_bad = spark.conf.get('spark.sql.very_bad')
    points_bad = spark.conf.get('spark.sql.bad')
    points_unacceptable = spark.conf.get('spark.sql.unacceptable')
    points_no_points = spark.conf.get('spark.sql.no_points')

    personal_df = personal_df.select(
        'personal_member_id', 'annual_inc', 'tot_hi_cred_lim')
    
    loan_df = remove_duplicates(loan_df, spark)

    loan_df = loan_df.join(personal_df, loan_df.loan_member_id ==
                           personal_df.personal_member_id, 'inner')

    loan_points_df = loan_df.withColumn('loan_status_points', when(col('loan_status') == "Fully Paid", points_very_good)
                                        .when(col('loan_status') == "Current", points_good)
                                        .when(col('loan_status') == "Late", points_ok)
                                        .when(col('loan_status') == "Charged Off", points_unacceptable)
                                        .when(col('loan_status') == "In Grace Period", points_bad)
                                        .when(col('loan_status') == "Default", points_very_bad).otherwise(points_no_points))\
        .withColumn('purpose_points', when(col('purpose').isin(['debt_consolidation', 'credit_card']), points_unacceptable)
                    .when(col('purpose').isin(['educational', 'house']), points_very_good).otherwise(points_ok))\
        .withColumn('income_points', when((col('annual_inc')) < (col('loan_amnt') * 0.5), points_unacceptable)
                    .when((col('annual_inc') <= col('loan_amnt')) & (col('annual_inc') < (col('loan_amnt') * 0.5)), points_very_bad)
                    .when((col('annual_inc') > col('loan_amnt')) & (col('annual_inc') < (col('loan_amnt') * 1.5)), points_bad)
                    .when((col('annual_inc') > (col('loan_amnt') * 1.5)) & (col('annual_inc') < (col('loan_amnt') * 2.0)), points_ok)
                    .when((col('annual_inc') > col('loan_amnt')) & (col('annual_inc') < (col('loan_amnt') * 2.5)), points_good)
                    .when((col('annual_inc') > (col('loan_amnt') * 2.5)), points_very_good).otherwise(points_no_points))\
        .withColumn('credi_line_points', when(col('loan_amnt') >= (col('tot_hi_cred_lim')*2), points_unacceptable)
                    .when(col('loan_amnt') >= (col('tot_hi_cred_lim')*2), points_very_bad)
                    .when(col('loan_amnt') >= (col('tot_hi_cred_lim')*1.5), points_bad)
                    .when(col('loan_amnt') >= col('tot_hi_cred_lim'), points_ok)
                    .when((col('loan_amnt') * 1.5) >= col('tot_hi_cred_lim'), points_good)
                    .when((col('loan_amnt') * 2) >= col('tot_hi_cred_lim'), points_very_good).otherwise(points_no_points))


    return loan_points_df.withColumn('total_loan_points', (col('loan_status_points') + col('purpose_points')))\
        