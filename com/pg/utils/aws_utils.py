
def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)


def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)


def read_data_from_mysql(spark,jdbc_params):
    dataframe =spark\
        .read\
        .format("jdbc")\
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .option(**jdbc_params)\
        .load()
    return dataframe

def write_to_s3(df, path):
    print('Writing data to', path)
    df.write \
        .mode("overwrite") \
        .partitionBy("ins_dt") \
        .parquet(path)