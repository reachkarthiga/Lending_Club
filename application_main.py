import sys
from lib.logger import Log4j
from lib import Transformations, DataReader, Utils
from lib.personal import personal_transformations
from lib.payment import payments_transformations
from lib.defaults import defaults_transformations
from lib.inquires import inquires_transformations

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)
    job_run_env = sys.argv[1]

    spark = Utils.getSpark(job_run_env)

    logger = Log4j(spark)

    logger.info(f"Starting the application in {job_run_env} environment")
    
    lending_club_raw = DataReader.read_data(spark, job_run_env)

    lending_club_mbr = Transformations.assign_member_id(lending_club_raw)

    personal_df = personal_transformations.get_personal_details(lending_club_mbr)
    personal_df_cleaned = personal_transformations.clean_personal_df(personal_df, spark)
    Utils.save_cleaned_data(personal_df_cleaned, job_run_env, "Personal")   
    personal_points_df = personal_transformations.personal_points_calculation(personal_df_cleaned,spark)
    logger.info("Personal data processed and saved successfully")

    payment_df = payments_transformations.get_payment_details(lending_club_mbr)
    payment_df_cleaned = payments_transformations.clean_payment_df(payment_df)
    Utils.save_cleaned_data(payment_df_cleaned,job_run_env, "Payment")
    payments_points_df = payments_transformations.payments_points_calculation(payment_df_cleaned, personal_df_cleaned,spark)
    logger.info("Payment data processed and saved successfully")    

    defaults_df = defaults_transformations.get_defaults_details(lending_club_mbr)
    defaults_df_cleaned = defaults_transformations.clean_defaults_df(defaults_df, spark)
    Utils.save_cleaned_data(defaults_df_cleaned, job_run_env, "Defaults")
    defaults_points_df = defaults_transformations.defaults_points_calculation(defaults_df_cleaned, spark)
    logger.info("Defaults data processed and saved successfully")

    inquires_df = inquires_transformations.get_inquires_details(lending_club_mbr)
    inquires_df_cleaned = inquires_transformations.clean_inquires_df(inquires_df, spark)
    Utils.save_cleaned_data(inquires_df_cleaned, job_run_env, "Inquires")   
    inquires_points_df = inquires_transformations.inquires_points_calculation(inquires_df_cleaned, spark)
    logger.info("Inquires data processed and saved successfully")

    Utils.stopSpark(spark)






