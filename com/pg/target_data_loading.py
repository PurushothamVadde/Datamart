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

    output_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + 'SB'

    jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                   "lowerBound": "1",
                   "upperBound": "100",
                   "dbtable": app_conf["mysql_conf"]["dbtable"],
                   "numPartitions": "2",
                   "partitionColumn": app_conf["mysql_conf"]["partition_column"],
                   "user": app_secret["mysql_conf"]["username"],
                   "password": app_secret["mysql_conf"]["password"]
                   }

    print("\nReading data from MySQL DB using SparkSession.read.format(),")
    txn_df = spark \
        .read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbc_params) \
        .load()
    txn_df = txn_df.withColumn("ins_dt", current_date())
    txn_df.show()
    ut.write_to_s3(txn_df, output_path)


# spark-submit --packages "mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1" com/pg/target_data_loading.py