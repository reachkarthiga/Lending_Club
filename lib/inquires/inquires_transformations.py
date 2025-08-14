from pyspark.sql.functions import expr, to_date, col, current_timestamp, when
from pyspark.sql.types import IntegerType


def get_inquires_details(lending_club_raw):
    # mths_since_recent_inq - max, chargeoff_within_12_mths - make as total charge offs - sum , inq_fi - most recent one,
    # inq_last_12m - most recent one based on iss_Dt
    return lending_club_raw.select('member_id', 'mths_since_recent_inq','chargeoff_within_12_mths', 'inq_last_12m','inq_fi', 'issue_d')


def clean_inquires_df(inquires_df, spark):
    inquires_clean_df = rename_col(inquires_df)
    inquires_clean_df = remove_bad_data(inquires_clean_df)
    inquires_clean_df = change_col_datatypes(inquires_clean_df)
    inquires_clean_df = remove_nulls(inquires_clean_df)
    inquires_clean_df = remove_duplicates(inquires_clean_df, spark)
    inquires_clean_df = change_col_outliers(inquires_clean_df)
    inquires_clean_df = ingest_timestamp(inquires_clean_df)
    return inquires_clean_df

def rename_col(inquires_df):
    return inquires_df.withColumnRenamed('member_id', 'inquires_member_id')


def remove_bad_data(inquires_df):
    return inquires_df.where(expr(""" inquires_member_id not like 'cf83e1357eefb8bdf%' """))


def ingest_timestamp(inquires_df):
    return inquires_df.withColumn("ingestion_date", current_timestamp())\



def change_col_datatypes(inquires_df):
    return inquires_df.withColumn("issue_d", to_date(col("issue_d"), "MMM-yyyy"))\
        .withColumn("mths_since_recent_inq", col("mths_since_recent_inq").cast(IntegerType()))\
        .withColumn("chargeoff_within_12_mths", col("chargeoff_within_12_mths").cast(IntegerType()))\
        .withColumn("inq_last_12m", col("inq_last_12m").cast(IntegerType()))\
        .withColumn("inq_fi", col("inq_fi").cast(IntegerType()))


def remove_nulls(inquires_df):
    return inquires_df.fillna(0, subset=['mths_since_recent_inq', "chargeoff_within_12_mths", "inq_last_12m", 'inq_fi'])


def remove_duplicates(inquires_df, spark):
    inquires_df.createOrReplaceTempView("inq_tbl")
    return spark.sql(""" With cte_1 as (
        select inquires_member_id , max(mths_since_recent_inq) as mnths_since_inq , 
        sum(chargeoff_within_12_mths) as chargeoff_within_12_mths 
        from inq_tbl group by inquires_member_id ), 
        cte_2 as (
        select inquires_member_id , inq_last_12m, inq_fi,  
        row_number() over(partition by inquires_member_id order by issue_d desc) as rn 
        from inq_tbl )
                     
        select a.inquires_member_id as inquires_member_id  , a.mnths_since_inq  as mnths_since_inq, 
        a.chargeoff_within_12_mths as chargeoff_within_12_mths , 
        b.inq_last_12m as inq_last_12m, b.inq_fi as inq_fi 
        from cte_1 a join cte_2 b on a.inquires_member_id = b.inquires_member_id 
        where b.rn = 1 """)


def change_col_outliers(inquires_df):
    return inquires_df.withColumn("inq_last_12m", when(col('inq_last_12m') > 100, 100).otherwise(col('inq_last_12m')))\
        .withColumn("inq_fi", when(col('inq_fi') > 50, 50).otherwise(col('inq_fi')))\
        .withColumn("chargeoff_within_12_mths", when(col('chargeoff_within_12_mths') > 10, 10).otherwise(col('chargeoff_within_12_mths')))


def inquires_points_calculation(inquires_df, spark):

    points_good = spark.conf.get('spark.sql.good')
    points_ok = spark.conf.get('spark.sql.ok')
    points_very_good = spark.conf.get('spark.sql.very_good')
    points_very_bad = spark.conf.get('spark.sql.very_bad')
    points_bad = spark.conf.get('spark.sql.bad')
    points_unacceptable = spark.conf.get('spark.sql.unacceptable')
    points_no_points = spark.conf.get('spark.sql.no_points')

    points_inquires_df = inquires_df.withColumn('mnth_inq_points', when(col('mnths_since_inq') == 0, points_very_good)
                                                .when((col('mnths_since_inq') > 0) & (col('mnths_since_inq') <= 2), points_good)
                                                .when((col('mnths_since_inq') >= 3) & (col('mnths_since_inq') < 4), points_ok)
                                                .when((col('mnths_since_inq') >= 4) & (col('mnths_since_inq') < 6), points_bad)
                                                .when((col('mnths_since_inq') >= 6) & (col('mnths_since_inq') < 9), points_very_bad)
                                                .otherwise(points_no_points)) \
        .withColumn('charge_off_points', when(col('chargeoff_within_12_mths') == 0, points_very_good).otherwise(points_unacceptable))\
        .withColumn('inq_points', when(col('inq_fi') == 0, points_very_good)
                    .when(col('inq_fi') == 1, points_good)
                    .when(col('inq_fi') == 2, points_ok)
                    .when(col('inq_fi') == 3, points_bad)
                    .when(col('inq_fi') == 4, points_very_bad)
                    .when(col('inq_fi') > 4, points_no_points))\
        .withColumn('inq_12m_points', when(col('inq_last_12m') == 0, points_very_good)
                    .when(col('inq_last_12m') == 1, points_good)
                    .when(col('inq_last_12m') == 2, points_ok)
                    .when(col('inq_last_12m') == 3, points_bad)
                    .when(col('inq_last_12m') == 4, points_very_bad)
                    .when(col('inq_last_12m') > 4, points_no_points))

    return points_inquires_df.withColumn('total_inquires_points', (col('mnth_inq_points') + col('inq_12m_points') +
                                                                   col('inq_points') + col('charge_off_points')))\
   