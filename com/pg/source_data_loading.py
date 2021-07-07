# Read data from mysql - transactionsync, create data frama out of it
# add a column 'inst_dt' - current_date()
# write the dataframe in s3 partition by int_dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
from com.pg.utils import aws_utils as ut

if __name__ == '__main__':

    # create the spark object
    spark = SparkSession\
        .builder\
        .appName("Data Ingestion from Project Sources")\
        .getOrCreate()

    # to log only the error logs in the console
    spark.sparkContext.setLogLevel('ERROR')

    # Reading the Configuration files
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Reading Data from S3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])


    src_list = app_conf["source_list"]


    for src in src_list:

        if src == 'SB':
            jdbc_params = {
                "url": ut.get_mysql_jdbc_url(app_secret),
                "lowerBound": '1',
                "upperBound": '100',
                "dbtable": app_conf["mysql_conf"]["dbtable"],
                "numPartitions": "2",
                "partitionColumn": app_conf["mysql_conf"]["partition_column"],
                "user": app_secret["mysql_conf"]["username"],
                "password": app_secret["mysql_conf"]["password"]
            }

            # use the ** operator/un-packer to treat a python dictionary as **kwargs
            # Read Transactions Data from Mysql
            tnxDF = spark\
                .read\
                .format("jdbc")\
                .option("driver", "com.mysql.cj.jdbc.Driver")\
                .option(**jdbc_params)\
                .load()
            tnxDF = tnxDF.withColumn("ins_dt", current_date())
            tnxDF.show(5, False)

            # write data to S3 in parquet format
            tnxDF\
                .write\
                .partitionBy("ins_dt")\
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + src)
        elif src == 'OL':
            # Reading Data from SFTP server
            ol_txn_df = spark.read\
                .format("com.springml.spark.sftp")\
                .option("host", app_secret["sftp_conf"]["hostname"])\
                .option("port", app_secret["sftp_conf"]["port"])\
                .option("username", app_secret["sftp_conf"]["username"])\
                .option("pem", os.path.abspath(current_dir + "/../../../../" + app_secret["sftp_conf"]["pem"]))\
                .option("fileType", "csv")\
                .option("delimiter", "|")\
                .load(app_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv")

            ol_txn_df = ol_txn_df.withColumn("ins_dt", current_date())
            ol_txn_df.show(5, False)
            # write data to S3 in parquet format
            ol_txn_df\
                .write\
                .partitionBy("ins_dt")\
                .parquet("s3a://" + app_conf["sftp_conf"]["staging_dir"]+ "/" + src)


        elif src == 'CP':
            cust_df = spark.read \
                .option("header", "false") \
                .option("delimiter", ",") \
                .option("inferSchema", "true") \
                .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "data/KC_Extract_1_20171009.csv") \

            cust_df = cust_df.withColumn("ins_dt", current_date())
            cust_df.show(5)
            # write data to S3
            cust_df \
                .write\
                .partitionBy("ins_dt")\
                .parquet("s3a://" + app_conf["sftp_conf"]["staging_dir_s3"] + "/" + src)

        elif src == 'ADDR':
            # Reading from mongodb
            cust_addr_df = spark\
                .read\
                .format("com.mongodb.spark.sql.DefaultSource")\
                .option("database", app_conf["mongodb_config"]["database"])\
                .option("collection", app_conf["mongodb_config"]["collection"])\
                .load()

            cust_addr_df = cust_df.withColumn("ins_dt", current_date())
            cust_addr_df.show(5)
            # write data to S3
            cust_addr_df \
                .write\
                .partitionBy("ins_dt")\
                .parquet("s3a://" + app_conf["sftp_conf"]["staging_dir"] + "/" + "Customer_Address")



# spark-submit --packages "mysql:mysql-connector-java:8.0.15" com/pg/source_data_loading.py
# spark-submit --packages "com.springml:spark-sftp_2.11:1.1.1" com/pg/source_data_loading.py
# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" com/pg/source_data_loading.py