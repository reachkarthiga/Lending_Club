
from pyspark.sql.functions import expr, col, when, lit, round, current_timestamp, to_date
from pyspark.sql.types import IntegerType, DoubleType


def get_payment_details(lending_club_raw):
    return lending_club_raw.select('id', 'member_id', 'installment', 'loan_amnt', 'loan_status', 'pymnt_plan', 'total_pymnt', 'total_rec_prncp',
                                   'total_rec_int', 'total_rec_late_fee', 'out_prncp',
                                   'last_pymnt_d', 'last_pymnt_amnt', 'issue_d')


def clean_payment_df(payment_df):
    payment_clean_df = rename_col(payment_df)
    payment_clean_df = remove_bad_data(payment_clean_df)
    payment_clean_df = change_col_datatypes(payment_clean_df)
    payment_clean_df = remove_nulls(payment_clean_df)
    payment_clean_df = change_col_outliers(payment_clean_df)
    payment_clean_df = ingest_timestamp(payment_clean_df)
    return payment_clean_df

def rename_col(payment_df):
    return payment_df.withColumnRenamed('member_id', 'payment_member_id')

def remove_duplicates(payment_df, spark):
    payment_df.createOrReplaceTempView('pymnt_tbl')
    return spark.sql(""" With cte_1 as (
                    select *, row_number() over(partition by payment_member_id order by issue_d desc) as rn 
                    from pymnt_tbl )
                    select id, payment_member_id, installment, loan_amnt, loan_status, pymnt_plan, total_pymnt, 
                    total_rec_prncp,total_rec_int, total_rec_late_fee, out_prncp,last_pymnt_d, last_pymnt_amnt 
                    from cte_1 where rn = 1 """)



def remove_bad_data(payment_df):
    return payment_df.where(expr(""" payment_member_id not like 'cf83e1357eefb8bdf%' """))


def change_col_datatypes(payment_df):
    return payment_df.withColumn("issue_d", to_date(col("issue_d"), "MMM-yyyy"))\
        .withColumn("total_rec_prncp", round(col("total_rec_prncp").cast(DoubleType()), 2))\
        .withColumn("total_rec_int", round(col("total_rec_int").cast(DoubleType()), 2))\
        .withColumn("total_rec_late_fee", round(col("total_rec_late_fee").cast(DoubleType()), 2))\
        .withColumn("out_prncp", round(col("out_prncp").cast(DoubleType()), 2))\
        .withColumn("id", col("id").cast(IntegerType()))\
        .withColumn("last_pymnt_amnt", round(col("last_pymnt_amnt").cast(DoubleType()), 2))\



def ingest_timestamp(payment_df):
    return payment_df.withColumn("ingestion_date", current_timestamp())


def change_col_outliers(payment_df):
    return payment_df.withColumn("loan_status", when(col("loan_status").isin(["Does not meet the credit policy. Status:Fully Paid", "Fully Paid"]), "Full Paid").
                                 when(col("loan_status").isin(["Does not meet the credit policy. Status:Charged Off", "Charged Off"]), "Charged Off").
                                 when(col("loan_status").isin(["Late (31-120 days)", "Late (16-30 days)"]), "Late").
                                 when(col("loan_status").isin(["Current", "In Grace Period", "Default"]), col("loan_status")).otherwise(lit(None)))\
        .withColumn("pymnt_plan", when(col("pymnt_plan") == "Charged Off", lit(None)).otherwise(col("pymnt_plan")))\
        .withColumn("total_pymnt", round((col("total_rec_prncp") + col("total_rec_int") + col("total_rec_late_fee")), 2))\
        .withColumn("out_prncp", round((col('loan_amnt') - col('total_rec_prncp')), 2))


def remove_nulls(payment_df):
    return payment_df.fillna(0, subset=['loan_amnt', "total_pymnt", "total_rec_prncp", "total_rec_int", "total_rec_late_fee", "out_prncp", 'last_pymnt_amnt'])


def payments_points_calculation(payments_df, personal_df, spark):

    points_good = spark.conf.get('spark.sql.good')
    points_ok = spark.conf.get('spark.sql.ok')
    points_very_good = spark.conf.get('spark.sql.very_good')
    points_very_bad = spark.conf.get('spark.sql.very_bad')
    points_bad = spark.conf.get('spark.sql.bad')
    points_unacceptable = spark.conf.get('spark.sql.unacceptable')
    points_no_points = spark.conf.get('spark.sql.no_points')

    personal_df = personal_df.select('personal_member_id', 'annual_inc')

    payments_df = remove_duplicates(payments_df, spark)
    payments_df = payments_df.join(
        personal_df, personal_df.personal_member_id == payments_df.payment_member_id, 'inner')
              

    payments_points_df = payments_df.withColumn('late_fees_points', when(col('total_rec_late_fee') >= 40000, points_unacceptable)
                                                .when((col('total_rec_late_fee') >= 25000 ) & ( col('total_rec_late_fee') < 40000), points_very_bad)
                                                .when((col('total_rec_late_fee') >= 10000 ) & ( col('total_rec_late_fee') < 25000), points_bad)
                                                .when((col('total_rec_late_fee') >= 5000 ) & ( col('total_rec_late_fee') < 10000), points_ok)
                                                .when((col('total_rec_late_fee') >= 1000 ) & ( col('total_rec_late_fee') < 5000), points_good)
                                                .when(col('total_rec_late_fee') < 1000, points_very_good).otherwise(points_no_points))\
        .withColumn('installment_points', when(col('installment')*2 > col('annual_inc')/12, points_very_bad)
                   .when(col('installment') > col('annual_inc')/12, points_bad)
                   .when(col('installment') == col('annual_inc')/12, points_ok)
                   .when(col('installment') < col('annual_inc')/12, points_good)
                   .when(col('installment')*2 < col('annual_inc')/12, points_very_good)
                   .otherwise(points_no_points))
    
    return payments_points_df.withColumn('total_payments_points', (col('late_fees_points') + col('installment_points')))\
      
