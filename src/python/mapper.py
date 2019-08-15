'''
Python mapper function

Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import boto3
import json
import random
import resource
from io import StringIO
import time

# create an S3 session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# constants
TASK_MAPPER_PREFIX = "task/mapper/";

def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)

def lambda_handler(event, context):
    
    start_time = time.time()

    job_bucket = event['jobBucket']
    src_bucket = event['bucket']
    src_keys = event['keys']
    job_id = event['jobId']
    mapper_id = event['mapperId']
   
    # aggr 
    output = {}
    line_count = 0
    err = ''

    # INPUT CSV => OUTPUT JSON

    # Download and process all keys
    for key in src_keys:
        response = s3_client.get_object(Bucket=src_bucket,Key=key)
        contents = response['Body'].read()
        
        for line in contents.split('\n')[:-1]:
            line_count +=1
            try:
                data = line.split(',')
                srcIp = data[0][:8]
                if srcIp not in output:
                    output[srcIp] = 0
                output[srcIp] += float(data[3])
            except Exception, e:
                print(e)
                #err += '%s' % e

    time_in_secs = (time.time() - start_time)
    #timeTaken = time_in_secs * 1000000000 # in 10^9 
    #s3DownloadTime = 0
    #totalProcessingTime = 0 
    pret = [len(src_keys), line_count, time_in_secs, err]
    mapper_fname = "%s/%s%s" % (job_id, TASK_MAPPER_PREFIX, mapper_id) 
    metadata = {
                    "linecount":  '%s' % line_count,
                    "processingtime": '%s' % time_in_secs,
                    "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
               }

    print("metadata", metadata)
    write_to_s3(job_bucket, mapper_fname, json.dumps(output), metadata)
    return pret

'''
ev = {
   "bucket": "-useast-1", 
   "keys": ["key.sample"],
   "jobId": "pyjob",
   "mapperId": 1,
   "jobBucket": "-useast-1"
   }
lambda_handler(ev, {});
'''
