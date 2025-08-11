from pyspark.sql.functions import col, sha2, concat_ws


def assign_member_id(lending_club_raw):
    return lending_club_raw.withColumn("member_id",
                                       sha2(concat_ws('-', col("grade"), col("sub_grade"), col("emp_title"),
                                                      col("emp_length"), col(
                                                          "verification_status"), col("home_ownership"),
                                                      col("annual_inc"), col("zip_code"), col("addr_state")), 512))
