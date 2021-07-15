def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)


# we have to write a function that takes all the necessary info, read data from mysql and return a dataframe

def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)


def read_from_sql(spark, app_conf, secret_conf):
    print("\nReading data from MYSQL DB,")
    jdbc_params = {"url": get_mysql_jdbc_url(secret_conf),
                   "lowerBound": "1",
                   "upperBound": "100",
                   "dbtable": app_conf["mysql_conf"]["dbtable"],
                   "numPartitions": "2",
                   "partitionColumn": app_conf["mysql_conf"]["partition_column"],
                   "user": secret_conf["mysql_conf"]["username"],
                   "password": secret_conf["mysql_conf"]["password"]
                   }
    df = spark \
        .read \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbc_params) \
        .load()

    return df


def read_from_sftp(spark, app_conf, secret_conf, pem_file_path):
    print("\nReading data from SFTP,")
    df = spark.read \
        .format("com.springml.spark.sftp")\
        .option("host", secret_conf["sftp_conf"]["hostname"])\
        .option("port", secret_conf["sftp_conf"]["port"])\
        .option("username", secret_conf["sftp_conf"]["username"])\
        .option("pem", pem_file_path)\
        .option("fileType", "csv")\
        .option("delimiter", "|")\
        .load(app_conf["sftp_conf"]["directory"] + "/" + app_conf["sftp_conf"]["filename"])

    return df


def read_from_s3(spark, app_conf):
    print("\nReading data from S3,")
    df = spark.read \
        .option("header", "true") \
        .format("csv") \
        .option("delimeter", "|") \
        .load("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf['filename'])

    return df


def read_from_mongoDB(spark, app_conf):
    print("\nReading data from MongoDB,")
    df = spark.read \
        .format("com.mongoDB.spark.sql.DefaultSource") \
        .option("database", app_conf["mongodb_config"]["database"]) \
        .option("collection", app_conf["mongodb_config"]["collection"]) \
        .load()

    return df





