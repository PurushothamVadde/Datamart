#Read data from mysql- transactionsync and create a dataframe
#add a column 'ins_dt' Insertion data
#Write the dataframe in s3 partitined by ind_dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os
from com.pg.utils import aws_utils as ut

if __name__ == '__main__':

    spark = SparkSession\
        .builder\
        .appName("ingesting data from other sources")\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secret_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    conf = open(app_secret_path)
    secret_conf = yaml.load(conf, Loader=yaml.FullLoader)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", secret_conf["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", secret_conf["s3_conf"]["secret_access_key"])

    src_list = app_conf["source_list"]

    for src in src_list:
        src_conf = app_conf[src]
        if src == 'SB':
            txn_df = ut.read_from_sql(spark, src_conf, secret_conf)\
                .withColumn("ins_dt", current_date())

            txn_df.show(5, False)

            txn_df.write\
                .mode("append") \
                .partitionBy("ins_dt")\
                .parquet("s3a://"+app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] +"/" + src)

        elif src == 'OL':
            ol_txn_df = ut.read_from_sftp(spark, src_conf, secret_conf,
                                           os.path.abspath(current_dir + "/../../" + secret_conf["sftp_conf"]["pem"]))\
                .withColumn("ins_dt", current_date())

            ol_txn_df.show(5, False)

            ol_txn_df.write \
                .mode("append") \
                .partitionBy("ins_dt")\
                .parquet("s3a://"+app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] +"/" + src)

        elif src == 'CP':
            cp_df = ut.read_from_s3(spark, src_conf)\
                .withColumn("ins_dt", current_date())

            cp_df.show(5, False)

            cp_df.write \
                .mode("append") \
                .partitionBy("ins_dt")\
                .parquet("s3a://"+app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] +"/" + src)

        elif src == 'ADDR':
            cust_addr_df = ut.read_from_mongoDB(spark, src_conf)\
                .withColumn("ins_dt", current_date())

            cust_addr_df.show(5, False)

            cust_addr_df.write \
                .mode("append") \
                .partitionBy("ins_dt")\
                .parquet("s3a://"+app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] +"/" + src)



#spark-submit --packages "mysql:mysql-connector-java:8.0.15, com.springml:spark-sftp_2.11:1.1.1, org.mongodb.spark:mongo-spark-connector_2.11:2.4.1, org.apache.hadoop:hadoop-aws:2.7.4" com/pg/source_data_loading.py