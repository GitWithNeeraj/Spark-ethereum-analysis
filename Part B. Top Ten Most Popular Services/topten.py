import os
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")  

    transactions = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv", header=True, inferSchema=True)
    contracts = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv", header=True, inferSchema=True)

    tf = transactions.filter("length(_c0) > 0 and length(_c1) > 0 and length(_c3) > 0 and length(_c6) > 0 and length(_c7) > 0")\
                      .select("_c6", "_c7").withColumnRenamed("_c6", "address").withColumnRenamed("_c7", "value")

    cf = contracts.filter("length(_c0) > 0 and length(_c1) > 0 and length(_c2) > 0 and length(_c3) > 0 and length(_c4) > 0 and length(_c5) > 0")\
                  .select("_c0").withColumnRenamed("_c0", "address")

    joined = tf.join(cf, "address", "left_outer").groupBy("address").agg({"value": "sum", "contractAddress": "count"}).withColumnRenamed("sum(value)", "total_value").withColumnRenamed("count(contractAddress)", "contract_count")

    top10_sc = joined.filter("total_value is not null and contract_count is not null").sort(joined.total_value.desc()).limit(10).rdd.map(lambda x: (x[0], x[1])).collect()

    s3 = boto3.client('s3', endpoint_url='http://' + s3_endpoint_url,
                      aws_access_key_id=s3_access_key_id,
                      aws_secret_access_key=s3_secret_access_key)

    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    s3.put_object(Body=json.dumps(top10_sc), Bucket=s3_bucket, Key='ethereum_partb_' + date_time + '/top10_smart_contracts2.txt')

    spark.stop()
