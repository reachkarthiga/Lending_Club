from pyspark.sql.functions import expr, col, to_date, current_timestamp, when
from pyspark.sql.types import IntegerType


def get_defaults_details(lending_club_raw):
    # All recent record should be selected
    return lending_club_raw.select('member_id', 'delinq_2yrs', 'pub_rec_bankruptcies','acc_now_delinq', 'mort_acc', 'issue_d')


def clean_defaults_df(defaults_df, spark):
    defaults_clean_df = rename_col(defaults_df)
    defaults_clean_df = remove_bad_data(defaults_clean_df)
    defaults_clean_df = change_col_datatypes(defaults_clean_df)
    defaults_clean_df = remove_nulls(defaults_clean_df)
    defaults_clean_df = remove_duplicates(defaults_clean_df, spark)
    defaults_clean_df = change_col_outliers(defaults_clean_df)
    defaults_clean_df = ingest_timestamp(defaults_clean_df)
    return defaults_clean_df

def rename_col(defaults_df):
    return defaults_df.withColumnRenamed('member_id', 'defaults_member_id')


def remove_nulls(defaults_df):
    return defaults_df.fillna(0, subset=['delinq_2yrs', "pub_rec_bankruptcies", "acc_now_delinq", 'mort_acc'])


def remove_bad_data(defaults_df):
    return defaults_df.where(expr(""" defaults_member_id not like 'cf83e1357eefb8bdf%' """))


def change_col_outliers(defaults_df):
    return defaults_df.withColumn('pub_rec_bankruptcies', when(col('pub_rec_bankruptcies') > 10, 10).otherwise(col('pub_rec_bankruptcies')))\
        .withColumn('delinq_2yrs', when(col('delinq_2yrs') > 24, 24).otherwise(col('delinq_2yrs')))\
        .withColumn('acc_now_delinq', when(col('acc_now_delinq') > 5, 5).otherwise(col('acc_now_delinq')))\
        .withColumn('mort_acc', when(col('mort_acc') > 15, 15).otherwise(col('mort_acc')))


def change_col_datatypes(defaults_df):
    return defaults_df.withColumn("issue_d", to_date(col("issue_d"), "MMM-yyyy"))\
        .withColumn("delinq_2yrs", col("delinq_2yrs").cast(IntegerType()))\
        .withColumn("acc_now_delinq", col("acc_now_delinq").cast(IntegerType()))\
        .withColumn("pub_rec_bankruptcies", col("pub_rec_bankruptcies").cast(IntegerType()))\
        .withColumn("mort_acc", col("mort_acc").cast(IntegerType()))


def ingest_timestamp(defaults_df):
    return defaults_df.withColumn("ingestion_date", current_timestamp())\



def remove_duplicates(defaults_df, spark):
    defaults_df.createOrReplaceTempView("def_tbl")
    return spark.sql(""" With cte_1 as (
                    select defaults_member_id , delinq_2yrs, pub_rec_bankruptcies,  acc_now_delinq, mort_acc, 
                    row_number() over(partition by defaults_member_id order by issue_d desc) as rn 
                    from def_tbl )
                    select defaults_member_id , delinq_2yrs, pub_rec_bankruptcies,  acc_now_delinq, mort_acc 
                    from cte_1 where rn = 1 """)


def defaults_points_calculation(defaults_df, spark):


    points_good = spark.conf.get('spark.sql.good')
    points_ok = spark.conf.get('spark.sql.ok')
    points_very_good = spark.conf.get('spark.sql.very_good')
    points_very_bad = spark.conf.get('spark.sql.very_bad')
    points_bad = spark.conf.get('spark.sql.bad')
    points_unacceptable = spark.conf.get('spark.sql.unacceptable')
    points_no_points = spark.conf.get('spark.sql.no_points')

    defaults_points_df = defaults_df.withColumn('deliq_points', when((col('delinq_2yrs') == 0) & (col('acc_now_delinq') == 0), points_very_good)
                                                .when((col('delinq_2yrs') == 1) & (col('acc_now_delinq') == 0), points_good)
                                                .when((col('delinq_2yrs') == 0) & (col('acc_now_delinq') == 1), points_good)
                                                .when((col('delinq_2yrs') == 1) & (col('acc_now_delinq') == 1), points_ok)
                                                .when((col('delinq_2yrs') == 2) & (col('acc_now_delinq') >= 1), points_bad)
                                                .when((col('delinq_2yrs') == 3) & (col('acc_now_delinq') >= 1), points_very_bad)
                                                .otherwise(points_no_points))\
        .withColumn('public_rec_points', when(col('pub_rec_bankruptcies') == 0, points_very_good)
                    .when(col('pub_rec_bankruptcies') == 1, points_ok)
                    .when(col('pub_rec_bankruptcies') > 1, points_unacceptable))\
        .withColumn('mort_points', when(col('mort_acc') == 0, points_very_good)
                    .when(col('mort_acc') == 1, points_ok)
                    .when(col('mort_acc') == 2, points_bad)
                    .when(col('mort_acc') > 2, points_unacceptable))

    return defaults_points_df.withColumn("total_defaults_points", (col('mort_points') + col('public_rec_points') + col('deliq_points')))\
 