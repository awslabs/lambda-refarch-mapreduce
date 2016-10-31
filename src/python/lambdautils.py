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

class LambdaManager(object):
    def __init__ (self, l, s3, region, codepath, job_id, fname, handler, lmem=1536):
        self.awslambda = l;
        self.region = "us-east-1" if region is None else region
        self.s3 = s3
        self.codefile = codepath
        self.job_id = job_id
        self.function_name = fname
        self.handler = handler
        self.role = 'MY_ARN'
        self.memory = lmem 
        self.timeout = 300
        self.function_arn = None # set after creation

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
                      Timeout =  self.timeout
                    )
        self.function_arn = response['FunctionArn']
        print response

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
            'TopicConfigurations' : [],
            'QueueConfigurations' : []
          }
        )

    def delete_function(self):
        self.awslambda.delete_function(FunctionName=self.function_name)

def compute_batch_size(keys, lambda_memory, gzip=False):
    max_mem_for_data = 0.6 * lambda_memory * 1000 * 1000; 
    size = 0.0
    for key in keys:
        if isinstance(key, dict):
            size += key['Size']
        else:
            size += key.size
    avg_object_size = size/len(keys)
    print "Dataset size: %s, nKeys: %s, avg: %s" %(size, len(keys), avg_object_size)
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
