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
    
def stopSpark(spark):
    spark.stop()
