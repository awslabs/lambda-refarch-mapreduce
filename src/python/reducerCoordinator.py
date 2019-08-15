'''
REDUCER Coordinator 

Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import boto3
import json
import lambdautils
import random
import re
from io import StringIO
import time
import urllib

DEFAULT_REGION = "us-east-1";

### STATES
MAPPERS_DONE = 0;
REDUCER_STEP = 1;

### Helpers ###

# create an S3 session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

# Write to S3 Bucket
def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)

def write_reducer_state(n_reducers, n_s3, bucket, fname):
    ts = time.time()
    data = json.dumps({
                "reducerCount": '%s' % n_reducers, 
                "totalS3Files": '%s' % n_s3,
                "start_time": '%s' % ts 
               })
    write_to_s3(bucket, fname, data, {})

# Count mapper files
def get_mapper_files(files):
    ret = []
    for mf in files:
        if "task/mapper" in mf["Key"]:
            ret.append(mf)
    return ret

def get_reducer_batch_size(keys):
    #TODO: Paramertize memory size
    batch_size = lambdautils.compute_batch_size(keys, 1536, 1000)
    return max(batch_size, 2) # At least 2 in a batch - Condition for termination

def check_job_done(files):
    # TODO: USE re
    for f in files:
        if "result" in f["Key"]:
            return True
    return False

def get_reducer_state_info(files, job_id, job_bucket):
        
    reducers = [];
    max_index = 0;
    reducer_step = False;
    r_index = 0;
   
    # Check if step is complete
        
    # Check for the Reducer state
    # Determine the latest reducer step#
    for f in files:
        #parts = f['Key'].split('/');
        if "reducerstate." in f['Key']:
            idx = int(f['Key'].split('.')[1])
            if idx > r_index:
                r_index = idx
            reducer_step = True

    # Find with reducer state is complete 
    if reducer_step == False:
        # return mapper files
        return [MAPPERS_DONE, get_mapper_files(files)]
    else:
        # Check if the current step is done
        key = "%s/reducerstate.%s" % (job_id, r_index) 
        response = s3_client.get_object(Bucket=job_bucket, Key=key)
        contents = json.loads(response['Body'].read())
        
        # get reducer outputs
        for f in files:
            fname = f['Key']
            parts = fname.split('/')
            if len(parts) < 3:
                continue
            rFname = 'reducer/' + str(r_index)
            if rFname in fname:
                reducers.append(f)
        
        if int(contents["reducerCount"]) == len(reducers):
            return (r_index, reducers)
        else:
            return (r_index, [])

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    start_time = time.time();

    # Job Bucket. We just got a notification from this bucket
    bucket = event['Records'][0]['s3']['bucket']['name']

    #key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
   
    config = json.loads(open('./jobinfo.json', "r").read())
    
    job_id =  config["jobId"]
    map_count = config["mapCount"] 
    r_function_name = config["reducerFunction"] 
    r_handler = config["reducerHandler"] 

    ### Get Mapper Finished Count ###
    
    # Get job files
    files = s3_client.list_objects(Bucket=bucket, Prefix=job_id)["Contents"]

    if check_job_done(files) == True:
        print("Job done!!! Check the result file")
        # TODO:  Delete reducer and coordinator lambdas
        return
    else:
        ### Stateless Coordinator logic
        mapper_keys = get_mapper_files(files)
        print("Mappers Done so far ", len(mapper_keys))

        if map_count == len(mapper_keys):
            
            # All the mappers have finished, time to schedule the reducers
            stepInfo = get_reducer_state_info(files, job_id, bucket)

            print("stepInfo", stepInfo)

            step_number = stepInfo[0];
            reducer_keys = stepInfo[1];
               
            if len(reducer_keys) == 0:
                print("Still waiting to finish Reducer step ", step_number)
                return
                 
            # Compute this based on metadata of files
            r_batch_size = get_reducer_batch_size(reducer_keys); 
                
            print("Starting the the reducer step", step_number)
            print("Batch Size", r_batch_size)
                
            # Create Batch params for the Lambda function
            r_batch_params = lambdautils.batch_creator(reducer_keys, r_batch_size);
                
            # Build the lambda parameters
            n_reducers = len(r_batch_params)
            n_s3 = n_reducers * len(r_batch_params[0])
            step_id = step_number +1;

            for i in range(len(r_batch_params)):
                batch = [b['Key'] for b in r_batch_params[i]]

                # invoke the reducers asynchronously
                resp = lambda_client.invoke( 
                        FunctionName = r_function_name,
                        InvocationType = 'Event',
                        Payload =  json.dumps({
                            "bucket": bucket,
                            "keys": batch,
                            "jobBucket": bucket,
                            "jobId": job_id,
                            "nReducers": n_reducers, 
                            "stepId": step_id, 
                            "reducerId": i 
                        })
                    )
                print(resp)

            # Now write the reducer state
            fname = "%s/reducerstate.%s"  % (job_id, step_id)
            write_reducer_state(n_reducers, n_s3, bucket, fname)
        else:
            print("Still waiting for all the mappers to finish ..")

'''
ev = {
    "Records": [{'s3': {'bucket': {'name': "smallya-useast-1"}}}],
    "bucket": "smallya-useast-1",
    "jobId": "jobid134",
    "mapCount": 1,
    "reducerFunctionName": "shell-exec",
    "reducerHandler": "index.handler"
}
lambda_handler(ev, {})
'''
