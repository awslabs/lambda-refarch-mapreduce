'''
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
import botocore
import os

SSM_PATH = '/biglambda/'

class LambdaManager(object):
    def __init__ (self, l, s3, region, codepath, job_id, fname, handler, role, lmem=1536):
        self.awslambda = l;
        self.region = "us-east-1" if region is None else region
        self.s3 = s3
        self.codefile = codepath
        self.job_id = job_id
        self.function_name = fname
        self.handler = handler
        self.role = role
        self.memory = lmem 
        self.timeout = 300
        self.function_arn = None # set after creation

    # TracingConfig parameter switches X-Ray tracing on/off.
    # Change value to 'Mode':'PassThrough' to switch it off
    def create_lambda_function(self):
        runtime = 'python2.7';
        response = self.awslambda.create_function(
                      FunctionName = self.function_name, 
                      Code = { 
                        "ZipFile": open(self.codefile, 'rb').read()
                      },
                      Handler =  self.handler,
                      Role =  self.role, 
                      Runtime = runtime,
                      Description = self.function_name,
                      MemorySize = self.memory,
                      Timeout =  self.timeout,
                      TracingConfig={'Mode':'PassThrough'}
                    )
        self.function_arn = response['FunctionArn']
        print response

    def update_function(self):
        '''
        Update lambda function
        '''
        response = self.awslambda.update_function_code(
                FunctionName = self.function_name, 
                ZipFile=open(self.codefile, 'rb').read(),
                Publish=True
                )
        updated_arn = response['FunctionArn']
        # parse arn and remove the release number (:n) 
        arn = ":".join(updated_arn.split(':')[:-1])
        self.function_arn = arn 
        print response

    def update_code_or_create_on_noexist(self):
        '''
        Update if the function exists, else create function
        '''
        try:
            self.create_lambda_function()
        except botocore.exceptions.ClientError as e:
            # parse (Function already exist) 
            self.update_function()

    def add_lambda_permission(self, sId, bucket):
        resp = self.awslambda.add_permission(
          Action = 'lambda:InvokeFunction', 
          FunctionName = self.function_name, 
          Principal = 's3.amazonaws.com', 
          StatementId = '%s' % sId,
          SourceArn = 'arn:aws:s3:::' + bucket
        )
        print resp

    def create_s3_eventsource_notification(self, bucket, prefix=None):
        if not prefix:
            prefix = self.job_id +"/task";

        self.s3.put_bucket_notification_configuration(
          Bucket =  bucket, 
          NotificationConfiguration = { 
            'LambdaFunctionConfigurations': [
              {
                  'Events': [ 's3:ObjectCreated:*'],
                  'LambdaFunctionArn': self.function_arn,
                   'Filter' : {
                    'Key':    {
                        'FilterRules' : [
                      {
                          'Name' : 'prefix',
                          'Value' : prefix
                      },
                    ]
                  }
                }
              }
            ],
            #'TopicConfigurations' : [],
            #'QueueConfigurations' : []
          }
        )

    def delete_function(self):
        self.awslambda.delete_function(FunctionName=self.function_name)

    @classmethod
    def cleanup_logs(cls, func_name):
        '''
        Delete all Lambda log group and log streams for a given function

        '''
        log_client = boto3.client('logs')
        #response = log_client.describe_log_streams(logGroupName='/aws/lambda/' + func_name)
        response = log_client.delete_log_group(logGroupName='/aws/lambda/' + func_name)
        return response

def compute_batch_size(keys, lambda_memory, concurrent_lambdas):
    max_mem_for_data = 0.6 * lambda_memory * 1000 * 1000; 
    size = 0.0
    for key in keys:
        if isinstance(key, dict):
            size += key['Size']
        else:
            size += key.size
    avg_object_size = size/len(keys)
    print "Dataset size: %s, nKeys: %s, avg: %s" %(size, len(keys), avg_object_size)
    if avg_object_size < max_mem_for_data and len(keys) < concurrent_lambdas:
        b_size = 1
    else:
        b_size = int(round(max_mem_for_data/avg_object_size))
    return b_size

def batch_creator(all_keys, batch_size):
    '''
    '''
    # TODO: Create optimal batch sizes based on key size & number of keys

    batches = []
    batch = []
    for i in range(len(all_keys)):
        batch.append(all_keys[i]);
        if (len(batch) >= batch_size):
            batches.append(batch)
            batch = []

    if len(batch):
        batches.append(batch)
    return batches

def load_config():
    ssm_client = boto3.client('ssm')
    config_dict={}

    # based on https://gist.github.com/sonodar/b3c80c8b9e60f4e6dcda9108c46a6089
    def read_params(NextToken = None):
        params = {
        'Path': SSM_PATH,
        'Recursive': False,
        'WithDecryption': False
        }
        if NextToken is not None:
            params['NextToken'] = NextToken
        return ssm_client.get_parameters_by_path(**params)
    def parameters():
        NextToken = None
        while True:
            response = read_params(NextToken)
            parameters = response['Parameters']
            if len(parameters) == 0:
                break
            for parameter in parameters:
                yield parameter
            if 'NextToken' not in response:
                break
            NextToken = response['NextToken']

    config_dict['ssmPath']=SSM_PATH
    for parameter in parameters():
        config_dict[parameter.get('Name').replace(SSM_PATH,'')]= parameter.get('Value')
    return config_dict
