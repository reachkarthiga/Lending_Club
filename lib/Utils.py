from pyspark.sql import SparkSession
from .ConfigReader import get_app_config, get_pyspark_config


def getSpark(env):
    if env == 'LOCAL':
        return SparkSession.builder\
            .config(conf=get_pyspark_config(env))\
            .config('spark.driver.extraJavaOptions','-Dlog4j.configuration=file:lib/log4j.properties')\
            .master("local[*]") \
            .getOrCreate()
    else:
        return SparkSession.builder\
            .config(conf=get_pyspark_config(env)) \
            .enableHiveSupport() \
            .getOrCreate()


def save_cleaned_data(dataframe, env, folder_name):
    env_conf = get_app_config(env)
    dataframe = dataframe.repartition(1)
    file_path = env_conf['lending_club.output.file.path']
    file_path = file_path + '/' + folder_name
    dataframe.write.format('csv').option(
        'header', True).mode('overwrite').save(file_path)
    
def create_temp_view(dataframe, name):
    return dataframe.createOrReplaceTempView(name)


def create_mdm_view(spark):

    spark.sql(""" create or replace temp view lending_club_mdm as 
                select p.member_id,p.grade,p.sub_grade,p.emp_title,p.emp_length,
                p.verification_status,p.home_ownership,p.annual_inc,p.zip_code,
                p.addr_state,p.tot_cur_bal ,l.id,l.loan_amnt,l.term,l.int_rate,
                l.issue_d,l.loan_status,l.purpose,r.installment,r.pymnt_plan,
                r.total_pymnt,r.total_rec_prncp,r.total_rec_int,r.total_rec_late_fee,
                r.out_prncp,r.last_pymnt_d,r.last_pymnt_amnt, d.delinq_2yrs, 
                d.pub_rec_bankruptcies, d.acc_now_delinq, d.mort_acc,i.mnths_since_inq, 
                i.chargeoff_within_12_mths , i.inq_last_12m, i.inq_fi    
                from loan_tbl l left join pymt_tbl r on r.id = l.id
                left join pers_tbl p on l.member_id = p.member_id
                left join dfts_tbl d on l.member_id = d.member_id
                left join inqs_tbl i on l.member_id = i.member_id
                """)


def writeToDisk(points_dataframe, env):
    env_conf = get_app_config(env)
    points_dataframe = points_dataframe.repartition(1)
    file_path = env_conf['lending_club.output.points.path']
    points_dataframe.write.format('csv').option(
        'header', True).mode('overwrite').save(file_path)


    
def stopSpark(spark):
    spark.stop()
