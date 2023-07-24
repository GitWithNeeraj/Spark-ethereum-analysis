import sys
import string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime


if __name__ == "__main__":
    # Create a SparkSession
    spark_session = SparkSession.builder.appName("Ethereum").getOrCreate()

    # Define a function to check if a line is a valid transaction
    def is_valid_transaction(line):
        try:
            fields = line.split(',')
            if len(fields) != 15:
                return False
            int(fields[11])
            return True
        except:
            return False

    # Read the environmental variables
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    # Configure the Hadoop configuration object for S3 access
    hadoop_conf = spark_session.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoop_conf.set("fs.s3a.access.key", s3_access_key_id)
    hadoop_conf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")  

    # Read the transaction data from S3
    transactions_rdd = spark_session.sparkContext.textFile(f"s3a://{s3_data_repository_bucket}/ECS765/ethereum-parvulus/transactions.csv")
    # Filter out invalid transactions
    clean_transactions_rdd = transactions_rdd.filter(is_valid_transaction)
    # Map the transactions to (month/year, 1) pairs and reduce by key to get the total number of transactions per month
    monthly_transactions_rdd = clean_transactions_rdd.map(lambda t: (time.strftime("%m/%Y", time.gmtime(int(t.split(',')[11]))), 1))
    total_transactions_rdd = monthly_transactions_rdd.reduceByKey(operator.add)

    # Initialize the S3 resource object
    s3_resource = boto3.resource('s3', endpoint_url=f'http://{s3_endpoint_url}', aws_access_key_id=s3_access_key_id, aws_secret_access_key=s3_secret_access_key)

    # Write the total number of transactions per month to S3
    now = datetime.now()
    timestamp = now.strftime("%d-%m-%Y_%H:%M:%S")
    s3_object = s3_resource.Object(s3_bucket, f'ethereum_{timestamp}/total_transactions.txt')
    s3_object.put(Body=json.dumps(total_transactions_rdd.take(100)))
    
    # Print the result to the console
    print(total_transactions_rdd.take(100))

    # Stop the SparkSession
    spark_session.stop()
