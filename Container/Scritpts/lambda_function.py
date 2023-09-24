import json
import uuid

import boto3
import matplotlib.pyplot as plt
import pandas as pd

s3client = boto3.client("s3")

def handler(event, context):
    records = event["Records"]
    outBucketName = "s3event-experiment"

    for record in records:
        
        csvfilname = "/tmp/" + str(uuid.uuid4()) + ".csv"

        tmp_key = record["s3"]["object"]["key"]
        tmp_bucket = record["s3"]["bucket"]["name"]
        
        s3client.download_file(tmp_bucket, tmp_key, csvfilname)

        df = pd.read_csv(csvfilname,dtype=float)
        colList = list(df.columns)

        plt.figure(figsize=(12,3))
        plt.plot(df[colList[0]],df[colList[1]])
        plt.plot(df[colList[0]],df[colList[2]])

        keyname =  str(uuid.uuid4()) + ".jpg"
        jpgfilename = "/tmp/" + keyname
        plt.savefig(jpgfilename)
        with open(jpgfilename, "rb") as f:
            s3client.put_object(Body=f,Bucket=outBucketName,Key=keyname,ContentType="image/jpeg")
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }