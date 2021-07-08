
import yaml
import os.path
from com.pg.utils import aws_utils as ut

# Reading the Configuration files
current_dir = os.path.abspath(os.path.dirname(__file__))
app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

conf = open(app_config_path)
app_conf = yaml.load(conf, Loader=yaml.FullLoader)
secret = open(app_secrets_path)
app_secret = yaml.load(secret, Loader=yaml.FullLoader)

print("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"])

