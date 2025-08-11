from pyspark.sql.functions import expr, col, lit, when, length, substring, current_timestamp, regexp_replace, to_date, round
from pyspark.sql.types import IntegerType, DoubleType


def get_personal_details(lending_club_raw):
    return lending_club_raw.select('member_id', 'grade', 'sub_grade', 'emp_title',
                                   'emp_length', 'verification_status', 'home_ownership',
                                   'annual_inc', 'zip_code', 'addr_state', 'avg_cur_bal',
                                   'tot_cur_bal', 'tot_hi_cred_lim', 'issue_d')


def clean_personal_df(personal_df, spark):
    personal_clean_df = rename_col(personal_df)
    personal_clean_df = remove_bad_data(personal_clean_df)
    personal_clean_df = change_col_datatypes(personal_clean_df)
    personal_clean_df = remove_nulls(personal_clean_df)
    personal_clean_df = remove_duplicates(personal_clean_df, spark)
    personal_clean_df = change_col_outliers(personal_clean_df)
    personal_clean_df = ingest_timestamp(personal_clean_df)
    return personal_clean_df


def rename_col(personal_df):
    return personal_df.withColumnRenamed('member_id', 'personal_member_id')


def remove_bad_data(personal_df):
    return personal_df.where(expr(""" personal_member_id not like 'cf83e1357eefb8bdf%' """))


def change_col_datatypes(personal_df):
    return personal_df.withColumn("issue_d", to_date(col("issue_d"), "MMM-yyyy"))\
        .withColumn("annual_inc", col("annual_inc").cast(DoubleType()))\
        .withColumn("avg_cur_bal", col("avg_cur_bal").cast(DoubleType()))\
        .withColumn("tot_cur_bal", col("tot_cur_bal").cast(DoubleType()))\
        .withColumn("tot_hi_cred_lim", col("tot_hi_cred_lim").cast(DoubleType()))\
        .withColumn("emp_length", col("emp_length").cast(IntegerType()))


def ingest_timestamp(personal_df):
    return personal_df.withColumn("ingestion_date", current_timestamp())


def change_col_outliers(personal_df):
    return personal_df.withColumn("addr_state", when(length(col("addr_state")) == 2, col("addr_state")).otherwise(None))\
        .withColumn("verification_status", when(col("verification_status").isin(["Source Verified", "Verified"]), True)
                    .otherwise(False))\
        .withColumn("home_ownership", when(col("home_ownership").isin(["OWN", "RENT", "MORTGAGE"]),
                                           col("home_ownership")).otherwise("RENT"))\
        .withColumn("zip_code", when(length(col("zip_code")) == 5, col("zip_code"))
                    .otherwise(lit(None)))\
        .withColumn("zip_code", when((substring(col("zip_code"), 3, 2) != 'xx') &
                                     (substring(col("zip_code"), 0, 3).cast(
                                         IntegerType()).isNull()),
                                     lit(None)).otherwise(col("zip_code")))\
        .withColumn("emp_length", regexp_replace(col("emp_length"), "(\D)", ""))\
        .withColumn("annual_inc", when(col("annual_inc") > 1000000, 1000000)
                    .otherwise(col('annual_inc')))


def remove_nulls(personal_df):
    return personal_df.fillna(0, subset=["annual_inc", "avg_cur_bal", "tot_cur_bal", "tot_hi_cred_lim", "emp_length"])


def remove_duplicates(personal_df, spark):
    personal_df.createOrReplaceTempView("pers_tbl")
    return spark.sql(""" With cte_1 as (
                    select *,  
                    row_number() over(partition by personal_member_id order by issue_d desc) as rn 
                    from pers_tbl )
                    select personal_member_id , grade, sub_grade, emp_title, emp_length, verification_status,
                    home_ownership, annual_inc, zip_code, addr_state, avg_cur_bal, tot_cur_bal , tot_hi_cred_lim from cte_1 where rn = 1 """)


def personal_points_calculation(personal_df, spark):

    points_good = spark.conf.get('spark.sql.good')
    points_ok = spark.conf.get('spark.sql.ok')
    points_very_good = spark.conf.get('spark.sql.very_good')
    points_very_bad = spark.conf.get('spark.sql.very_bad')
    points_bad = spark.conf.get('spark.sql.bad')
    points_unacceptable = spark.conf.get('spark.sql.unacceptable')
    points_no_points = spark.conf.get('spark.sql.no_points')
    grade_points = 200.0

    personal_points_df = personal_df.withColumn('verification_points', when(col('verification_status'), points_good).otherwise(points_bad))\
        .withColumn('grade_points', when((col('grade') == 'A') & (col('sub_grade') == 'A1'), grade_points * 1)
                    .when((col('grade') == 'A') & (col('sub_grade') == 'A2'), grade_points * 0.9)
                    .when((col('grade') == 'A') & (col('sub_grade') == 'A3'), grade_points * 0.875)
                    .when((col('grade') == 'A') & (col('sub_grade') == 'A4'), grade_points * 0.85)
                    .when((col('grade') == 'A') & (col('sub_grade') == 'A5'), grade_points * 0.825)
                    .when((col('grade') == 'B') & (col('sub_grade') == 'B1'), grade_points * 0.8)
                    .when((col('grade') == 'B') & (col('sub_grade') == 'B2'), grade_points * 0.775)
                    .when((col('grade') == 'B') & (col('sub_grade') == 'B3'), grade_points * 0.75)
                    .when((col('grade') == 'B') & (col('sub_grade') == 'B4'), grade_points * 0.725)
                    .when((col('grade') == 'B') & (col('sub_grade') == 'B5'), grade_points * 0.7)
                    .when((col('grade') == 'C') & (col('sub_grade') == 'C1'), grade_points * 0.675)
                    .when((col('grade') == 'C') & (col('sub_grade') == 'C2'), grade_points * 0.65)
                    .when((col('grade') == 'C') & (col('sub_grade') == 'C3'), grade_points * 0.625)
                    .when((col('grade') == 'C') & (col('sub_grade') == 'C4'), grade_points * 0.6)
                    .when((col('grade') == 'C') & (col('sub_grade') == 'C5'), grade_points * 0.575)
                    .when((col('grade') == 'D') & (col('sub_grade') == 'D1'), grade_points * 0.55)
                    .when((col('grade') == 'D') & (col('sub_grade') == 'D2'), grade_points * 0.525)
                    .when((col('grade') == 'D') & (col('sub_grade') == 'D3'), grade_points * 0.5)
                    .when((col('grade') == 'D') & (col('sub_grade') == 'D4'), grade_points * 0.475)
                    .when((col('grade') == 'D') & (col('sub_grade') == 'D5'), grade_points * 0.45)
                    .when((col('grade') == 'E') & (col('sub_grade') == 'E1'), grade_points * 0.425)
                    .when((col('grade') == 'E') & (col('sub_grade') == 'E2'), grade_points * 0.4)
                    .when((col('grade') == 'E') & (col('sub_grade') == 'E3'), grade_points * 0.375)
                    .when((col('grade') == 'E') & (col('sub_grade') == 'E4'), grade_points * 0.35)
                    .when((col('grade') == 'E') & (col('sub_grade') == 'E5'), grade_points * 0.325)
                    .when((col('grade') == 'F') & (col('sub_grade') == 'F1'), grade_points * 0.3)
                    .when((col('grade') == 'F') & (col('sub_grade') == 'F2'), grade_points * 0.275)
                    .when((col('grade') == 'F') & (col('sub_grade') == 'F3'), grade_points * 0.25)
                    .when((col('grade') == 'F') & (col('sub_grade') == 'F4'), grade_points * 0.225)
                    .when((col('grade') == 'F') & (col('sub_grade') == 'F5'), grade_points * 0.2)
                    .when((col('grade') == 'G') & (col('sub_grade') == 'G1'), grade_points * 0.175)
                    .when((col('grade') == 'G') & (col('sub_grade') == 'G2'), grade_points * 0.15)
                    .when((col('grade') == 'G') & (col('sub_grade') == 'G3'), grade_points * 0.125)
                    .when((col('grade') == 'G') & (col('sub_grade') == 'G4'), grade_points * 0.1)
                    .when((col('grade') == 'G') & (col('sub_grade') == 'G5'), grade_points * 0.05)
                    .otherwise(points_no_points))\
        .withColumn('exp_points', when(col('emp_length') == 0, points_unacceptable)
                    .when((col('emp_length').isin([1, 2])), points_very_bad)
                    .when((col('emp_length').isin([3, 4])), points_bad)
                    .when((col('emp_length').isin([5, 6])), points_ok)
                    .when((col('emp_length').isin([7, 8])), points_good)
                    .when((col('emp_length').isin([9, 10])), points_very_good))\
        .withColumn('home_points', when(col('home_ownership') == 'OWN', points_good)
                    .when(col('home_ownership') == 'RENT', points_ok)
                    .when(col('home_ownership') == 'MORTAGE', points_bad)
                    .otherwise(points_no_points))


    return personal_points_df.withColumn('total_personal_points', (col('verification_points') + col('home_points') + col('exp_points') + col('grade_points')))\
        .withColumn('total_personal_points', round(col('total_personal_points'), 2))\

