import os
import sys
import time
import json
import boto3
from pyspark.sql import SparkSession
from datetime import datetime

def check_transaction(line):
    fields = line.split(",")
    if len(fields) != 15:
        return False
    try:
        float(fields[7])
        int(fields[11])
        return True
    except:
        return False

def map_transaction(line):
    fields = line.split(",")
    value = float(fields[7])
    timestamp = int(fields[11])
    month = time.strftime("%m/%Y", time.gmtime(timestamp))
    return (month, (value, 1))

def reduce_transaction(t1, t2):
    total_value = t1[0] + t2[0]
    total_count = t1[1] + t2[1]
    return (total_value, total_count)

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("Ethereum").getOrCreate()

    # Read S3 bucket details from environment variables
    s3_endpoint_url = os.environ["S3_ENDPOINT_URL"] + ":" + os.environ["BUCKET_PORT"]
    s3_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    s3_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    s3_data_repository_bucket = os.environ["DATA_REPOSITORY_BUCKET"]
    s3_bucket_name = os.environ["BUCKET_NAME"]

    # Configure Hadoop to access S3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoop_conf.set("fs.s3a.access.key", s3_access_key_id)
    hadoop_conf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

    # Load transaction data from S3
    transactions = spark.sparkContext.textFile(
        "s3a://{}/ECS765/ethereum-parvulus/transactions.csv".format(
            s3_data_repository_bucket
        )
    )

    # Filter invalid transactions and map valid transactions to (month, (value, 1)) tuples
    valid_transactions = transactions.filter(check_transaction).map(map_transaction)

    # Reduce tuples by month and calculate the average transaction value per month
    avg_transactions = valid_transactions.reduceByKey(reduce_transaction).mapValues(
        lambda v: v[0] / v[1]
    )

    # Convert the RDD to a list of strings and save the result to S3
    now = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    result_list = avg_transactions.map(lambda t: "{},{}".format(t[0], t[1])).collect()
    s3_client = boto3.client(
        "s3",
        endpoint_url="http://{}".format(s3_endpoint_url),
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )
    s3_client.put_object(
        Bucket=s3_bucket_name,
        Key="ethereum_avg_{}/transactions_avg.txt".format(now),
        Body=json.dumps(result_list),
    )

    # Stop the Spark session
    spark.stop()
