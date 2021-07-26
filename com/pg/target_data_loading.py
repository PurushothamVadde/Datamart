from pyspark.sql import SparkSession
import yaml
import os.path
import sys
from pyspark.sql.functions import current_date

import utils.aws_utils as ut

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar"\
         --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    tgt_list = app_conf['target_list']
    for tgt in tgt_list:
        print('Preparing', tgt, 'data,')
        tgt_conf = app_conf[tgt]
        if tgt == 'REGIS_DIM':
            print('Loading the source data,')
            for src in tgt_conf['source_data']:
                file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src

                txn_df = ut.read_parquet_from_s3(spark,file_path)
                txn_df.createOrReplaceTempView(src)
                txn_df.printSchema()
                txn_df.show(5, False)

            print('Preparing the', tgt, 'data,')
            txn_df = spark.sql(app_conf['REGIS_DIM']['loadingQuery'])
            txn_df.show()
            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)

            txn_df = ut.write_data_to_Redshift(txn_df,
                                               jdbc_url,
                                               "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                               app_conf['REGIS_DIM']['tableName'])
            print("Completed   <<<<<<<<<")

        elif tgt == 'CHILD_DIM':
            src = tgt_conf['source_data']

            file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src

            txn_df = ut.read_parquet_from_s3(spark, file_path)
            txn_df.createOrReplaceTempView(src)
            txn_df.printSchema()
            txn_df.show(5, False)

            print('Preparing the', tgt, 'data,')
            txn_df = spark.sql(app_conf['CHILD_DIM']['loadingQuery'])
            txn_df.show()
            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            txn_df = ut.write_data_to_Redshift(txn_df,
                                               jdbc_url,
                                               "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                               app_conf['CHILD_DIM']['tableName'])
            print("Completed   <<<<<<<<<")

        elif tgt == 'RTL_TXN_FCT':
            # Reading Data From OL
            file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/OL"
            print(file_path)
            txn_df = spark.read\
                .option("header", "true")\
                .option("delimiter", "|")\
                .parquet(file_path)

            txn_df.show(5, False)
            txn_df.createOrReplaceTempView("OL")
            txn_df.printSchema()
            # Reading Data From SB
            file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/SB"
            print(file_path)
            txn_df = spark.read\
                .option("header", "true")\
                .option("delimiter", "|")\
                .parquet(file_path)

            txn_df.show(5, False)
            txn_df.createOrReplaceTempView("SB")
            txn_df.printSchema()

            # Reading Data from Redshift
            REGIS_DIM = spark.read\
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbc_url) \
                .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                .option("forward_spark_s3_credentials", "true") \
                .option("dbtable", "DATAMART.REGIS_DIM") \
                .load()

            REGIS_DIM.show(5, False)
            REGIS_DIM.createOrReplaceTempView("REGIS_DIM")

            # Fact Table
            txn_df = spark.sql(app_conf['RTL_TXN_FCT']['loadingQuery'])
            txn_df.show(5, False)



# spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/pg/target_data_loading.py
#  spark-submit  --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/pg/target_data_loading.py