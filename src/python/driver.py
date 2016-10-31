'''
 Driver to start BigLambda Job
 
 
 * Copyright 2016, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License. 
'''

import boto3
import json
import math
import random
import re
import StringIO
import sys
import time

import lambdautils

import glob
import subprocess 
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial

# create an S3 session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

JOB_INFO = 'jobinfo.json'

### UTILS ####
def zipLambda(fname, zipname):
    # faster to zip with shell exec
    subprocess.call(['zip', zipname] + glob.glob(fname) + glob.glob(JOB_INFO) +
                        glob.glob("lambdautils.py"))

def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)

def write_job_config(job_id, job_bucket, n_mappers, r_func, r_handler):
    fname = "jobinfo.json"; 
    with open(fname, 'w') as f:
        data = json.dumps({
            "jobId": job_id,
            "jobBucket" : job_bucket,
            "mapCount": n_mappers,
            "reducerFunction": r_func,
            "reducerHandler": r_handler
            }, indent=4);
        f.write(data)


######### MAIN ############# 

## JOB ID 
job_id =  "bl-release"

# Config
config = json.loads(open('driverconfig.json', 'r').read())

# 1. Get all keys to be processed  
# init 
bucket = config["bucket"]
job_bucket = config["jobBucket"]
region = config["region"]
lambda_memory = config["lambdaMemory"]
concurrent_lambdas = config["concurrentLambdas"]

#all_keys = s3_client.list_objects(Bucket=bucket, Prefix=config["prefix"])["Contents"]

# Fetch all the keys that match the prefix
all_keys = []
for obj in s3.Bucket(bucket).objects.filter(Prefix=config["prefix"]).all():
    all_keys.append(obj)

bsize = lambdautils.compute_batch_size(all_keys, lambda_memory)
batches = lambdautils.batch_creator(all_keys, bsize)
n_mappers = len(batches)

# 2. Create the lambda functions

L_PREFIX = "BL"

# Lambda functions
mapper_lambda_name = L_PREFIX + "-mapper-" +  job_id;
reducer_lambda_name = L_PREFIX + "-reducer-" +  job_id; 
rc_lambda_name = L_PREFIX + "-rc-" +  job_id;

# write job config
write_job_config(job_id, job_bucket, n_mappers, reducer_lambda_name, config["reducer"]["handler"]);

zipLambda(config["mapper"]["name"], config["mapper"]["zip"])
zipLambda(config["reducer"]["name"], config["reducer"]["zip"])
zipLambda(config["reducerCoordinator"]["name"], config["reducerCoordinator"]["zip"])

# mapper
l_mapper = lambdautils.LambdaManager(lambda_client, s3_client, region, config["mapper"]["zip"], job_id,
        mapper_lambda_name, config["mapper"]["handler"])
l_mapper.create_lambda_function()

# Reducer func
l_reducer = lambdautils.LambdaManager(lambda_client, s3_client, region, config["reducer"]["zip"], job_id,
        reducer_lambda_name, config["reducer"]["handler"])
l_reducer.create_lambda_function()

# Coordinator
l_rc = lambdautils.LambdaManager(lambda_client, s3_client, region, config["reducerCoordinator"]["zip"], job_id,
        rc_lambda_name, config["reducerCoordinator"]["handler"])
l_rc.create_lambda_function()

# Add permission to the coordinator
l_rc.add_lambda_permission(random.randint(1,1000), job_bucket)

# create event source for coordinator
l_rc.create_s3_eventsource_notification(job_bucket)

# Write Jobdata to S3
j_key = job_id + "/jobdata";
data = json.dumps({
                "mapCount": n_mappers, 
                "totalS3Files": len(all_keys),
                "startTime": time.time()
                })
write_to_s3(job_bucket, j_key, data, {})
    

### Execute ###

mapper_outputs = []

#2. Invoke Mappers
def invoke_lambda(batches, m_id):
    '''
    lambda invoke function
    '''
    # TODO: Increase timeout

    #batch = [k['Key'] for k in batches[m_id-1]]
    batch = [k.key for k in batches[m_id-1]]
    #print "invoking", m_id, len(batch)
    resp = lambda_client.invoke( 
            FunctionName = mapper_lambda_name,
            InvocationType = 'RequestResponse',
            Payload =  json.dumps({
                "bucket": bucket,
                "keys": batch,
                "jobBucket": job_bucket,
                "jobId": job_id,
                "mapperId": m_id
            })
        )
    out = eval(resp['Payload'].read())
    mapper_outputs.append(out)
    print "mapper output", out

# Exec Parallel
print "# of Mappers ", n_mappers 
pool = ThreadPool(n_mappers)
Ids = [i+1 for i in range(n_mappers)]
invoke_lambda_partial = partial(invoke_lambda, batches)

# Burst request handling
mappers_executed = 0
while mappers_executed < n_mappers:
    nm = min(concurrent_lambdas, n_mappers)
    results = pool.map(invoke_lambda_partial, Ids[mappers_executed: mappers_executed + nm])
    mappers_executed += nm

pool.close()
pool.join()

print "all the mappers finished"

# Delete Mapper function
l_mapper.delete_function()

######## COST ######

# Calculate costs - Approx (since we are using exec time reported by our func and not billed ms)
total_lambda_secs = 0
total_s3_get_ops = 0
total_s3_put_ops = 0
s3_storage_hours = 0
total_lines = 0

for output in mapper_outputs:
    total_s3_get_ops += int(output[0])
    total_lines += int(output[1])
    total_lambda_secs += float(output[2])


#Note: Wait for the job to complete so that we can compute total cost ; create a poll every 10 secs

# Get all reducer keys
reducer_keys = []

# Total execution time for reducers
reducer_lambda_time = 0

while True:
    job_keys = s3_client.list_objects(Bucket=job_bucket, Prefix=job_id)["Contents"]
    keys = [jk["Key"] for jk in job_keys]
    total_s3_size = sum([jk["Size"] for jk in job_keys])
    
    print "check to see if the job is done"

    # check job done
    if job_id + "/result" in keys:
        print "job done"
        reducer_lambda_time += float(s3.Object(job_bucket, job_id + "/result").metadata['processingtime'])
        for key in keys:
            if "task/reducer" in key:
                reducer_lambda_time += float(s3.Object(job_bucket, key).metadata['processingtime'])
                reducer_keys.append(key)
        break
    time.sleep(5)

# S3 Storage cost - Account for mappers only; This cost is neglibile anyways since S3 
# costs 3 cents/GB/month
s3_storage_hour_cost = 1 * 0.0000521574022522109 * (total_s3_size/1024.0/1024.0/1024.0) # cost per GB/hr 
s3_put_cost = len(job_keys) *  0.005/1000

# S3 GET # $0.004/10000 
total_s3_get_ops += len(job_keys) 
s3_get_cost = total_s3_get_ops * 0.004/10000 

# Total Lambda costs
total_lambda_secs += reducer_lambda_time
lambda_cost = total_lambda_secs * 0.00001667 * lambda_memory/ 1024.0
s3_cost =  (s3_get_cost + s3_put_cost + s3_storage_hour_cost)

# Print costs
print "Reducer L", reducer_lambda_time * 0.00001667 * lambda_memory/ 1024.0
print "Lambda Cost", lambda_cost
print "S3 Storage Cost", s3_storage_hour_cost
print "S3 Request Cost", s3_get_cost + s3_put_cost 
print "S3 Cost", s3_cost 
print "Total Cost: ", lambda_cost + s3_cost
print "Total Lines:", total_lines 


# Delete Reducer function
l_reducer.delete_function()
l_rc.delete_function()
