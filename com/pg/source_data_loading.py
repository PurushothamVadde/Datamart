# Read data from mysql - transactionsync, create data frama out of it
# add a column 'inst_dt' - current_date()
# write the dataframe in s3 partition by int_dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
import utils.aws_utils as ut


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
    app_config_path = os.path.abspath(current_dir + "/../../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

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
    tnxDF = spark\
        .read\
        .format("jdbc")\
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .option(**jdbc_params)\
        .load()

    tnxDF = tnxDF.withColumn("ins_dt", current_date())
    tnxDF.show()
# spark-submit --packages "mysql:mysql-connector-java:8.0.15" com/pg/source_data_loading.py
