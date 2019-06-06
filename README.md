# Serverless Reference Architecture: MapReduce

This serverless MapReduce reference architecture demonstrates how to use [AWS Lambda](https://aws.amazon.com/lambda) in conjunction with [Amazon S3](https://aws.amazon.com/s3) to build a MapReduce framework that can process data stored in S3. 

By leveraging this framework, you can build a cost-effective pipeline to run ad hoc MapReduce jobs. The price-per-query model and ease of use make it very suitable for data scientists and developers alike. 

## Features

* Close to "zero" setup time
* Pay per execution model for every job
* Cheaper than other data processing solutions
* Enables data processing within a VPC

## Architecture

![Serverless MapReduce architecture](https://s3.amazonaws.com/smallya-test/bl-git.png "Serverless MapReduce architecture")

### IAM policies 

* Lambda execution role with 
    * [S3 read/write access](http://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-create-iam-role.html)
    * Cloudwatch log access (logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents)
    * X-Ray write access (xray:PutTraceSegments, xray:PutTelemetryRecords)
 
Check policy.json for a sample that you can use or extend.

* To execute the driver locally, make sure that you configure your AWS profile with access to: 
    * [S3](http://docs.aws.amazon.com/AmazonS3/latest/dev/example-policies-s3.html)
    * [Lambda](http://docs.aws.amazon.com/lambda/latest/dg/lambda-api-permissions-ref.html)
    * [X-Ray](https://docs.aws.amazon.com/xray/latest/devguide/xray-permissions.html)

### Quickstart::Step by Step  ###

To run the example, you must have the AWS CLI set up. Your credentials must have access to create and invoke Lambda and access to list, read, and write to a S3 bucket.

1. Create your S3 bucket to store the intermediaries and result
(remember to use your own bucket name due to S3 namespace)

  $ aws s3 mb s3://YOUR-BUCKET-NAME-HERE

2. Update the policy.json with your S3 bucket name

  $ sed -i 's/s3:::YOUR-BUCKET-NAME-HERE/s3:::biglambda-s3-bucket/' policy.json

3. Create the IAM role with respective policy

  $ python create-biglambda-role.py

4. Use the output ARN from the script. Set the serverless_mapreduce_role environment variable:

  $ export serverless_mapreduce_role=arn:aws:iam::MY-ACCOUNT-ID:role/biglambda_role

5. Make edits to driverconfig.json and verify

  $ cat driverconfig.json 

6. [Run AWS X-Ray Daemon locally](https://docs.aws.amazon.com/xray/latest/devguide/xray-daemon-local.html), otherwise you will not be able to see traces from the local driver in AWS X-Ray console. However, traces from Reducer Coordinator Lambda functions will be present.

7. Run the driver
 
	$ python driver.py

### Modifying the Job (driverconfig.json)

For the jobBucket field, enter an S3 bucket in your account that you wish to use for the example. Make changes to the other fields if you have different source data, or if you have renamed the files.

```

{
        "bucket": "big-data-benchmark",
        "prefix": "pavlo/text/1node/uservisits/",
        "jobBucket": "YOUR-BUCKET-NAME-HERE",
        "concurrentLambdas": 100,
        "mapper": {
            "name": "mapper.py",
            "handler": "mapper.lambda_handler",
            "zip": "mapper.zip"
        },
        "reducer":{
            "name": "reducer.py",
            "handler": "reducer.lambda_handler",
            "zip": "reducer.zip"
        },
        "reducerCoordinator":{
            "name": "reducerCoordinator.py",
            "handler": "reducerCoordinator.lambda_handler",
            "zip": "reducerCoordinator.zip"
        },
}

```

### Outputs 

```
smallya$ aws s3 ls s3://JobBucket/py-bl-1node-2 --recursive --human-readable --summarize

2016-09-26 15:01:17   69 Bytes py-bl-1node-2/jobdata
2016-09-26 15:02:04   74 Bytes py-bl-1node-2/reducerstate.1
2016-09-26 15:03:21   51.6 MiB py-bl-1node-2/result 
2016-09-26 15:01:46   18.8 MiB py-bl-1node-2/task/
….

smallya$ head –n 3 result 
67.23.87,5.874290244999999
30.94.22,96.25011190570001
25.77.91,14.262780186000002
```

### Cleaning up the example resources
To remove all resources created by this example, do the following:

1. Delete all objects from the S3 bucket listed in `jobBucket` created by the job.
1. Delete the Cloudwatch log groups for each of the Lambda functions created by the job. 
1. Delete the created IAM role

    $ python delete-biglambda-role.py

## Languages
* Python 2.7 (active development)
* Node.js

The Python version is under active development and feature enhancement.

## Benchmark

To compare this framework with other data processing frameworks, we ran a subset of the Amplab benchmark. The table below has the execution time for each workload in seconds: 

Dataset

s3n://big-data-benchmark/pavlo/[text|text-deflate|sequence|sequence-snappy]/[suffix].

S3 Suffix   Scale Factor    Rankings (rows) Rankings (bytes)    UserVisits (rows)   UserVisits (bytes)  Documents (bytes)
/5nodes/    5               90 Million      6.38 GB              775 Million         126.8 GB             136.9 GB

Queries:

* Scan query  (90 M Rows, 6.36 GB of data)
* SELECT pageURL, pageRank FROM rankings WHERE pageRank > X   ( X= {1000, 100, 10} )

    * 1a) SELECT pageURL, pageRank FROM rankings WHERE pageRank > 1000   
    * 1b) SELECT pageURL, pageRank FROM rankings WHERE pageRank > 100   


* Aggregation query on UserVisits ( 775M rows, ~127GB of data)
    * 2a) SELECT SUBSTR(sourceIP, 1, 8), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 8)


NOTE: Only a subset of the queries could be run, as Lambda currently supports a maximum container size of 1536 MB. The benchmark is designed to increase the output size by an order of magnitude for the a,b,c iterations. Given that the output size doesn't fit in Lambda memory, we currently can't process to compute the final output. 

```
|-----------------------|---------|---------|--------------|
| Technology            | Scan 1a | Scan 1b | Aggregate 2a | 
|-----------------------|---------|---------|--------------|
| Amazon Redshift (HDD) | 2.49    | 2.61    | 25.46        |
|-----------------------|---------|---------|--------------|
| Impala - Disk - 1.2.3 | 12.015  | 12.015  | 113.72       |
|-----------------------|---------|---------|--------------|
| Impala - Mem - 1.2.3  | 2.17    | 3.01    | 84.35        |
|-----------------------|---------|---------|--------------|
| Shark - Disk - 0.8.1  | 6.6     | 7       | 151.4        |
|-----------------------|---------|---------|--------------|
| Shark - Mem - 0.8.1   | 1.7     | 1.8     | 83.7         |
|-----------------------|---------|---------|--------------|
| Hive - 0.12 YARN      | 50.49   | 59.93   | 730.62       |
|-----------------------|---------|---------|--------------|
| Tez - 0.2.0           | 28.22   | 36.35   | 377.48       |
|-----------------------|---------|---------|--------------|
| Serverless MapReduce  | 39      | 47      | 200          |   
|-----------------------|---------|---------|--------------|

Serverless MapReduce Cost:

|---------|---------|--------------|
| Scan 1a | Scan 1b | Aggregate 2a | 
|---------|---------|--------------|
| 0.00477 | 0.0055  | 0.1129       |   
|---------|---------|--------------|
```

## License
This sample code is made available under the MIT-0 license. See the LICENSE file.  
`src/nodejs/s3utils.js` is made available under the MIT license. See the THIRD_PARTY file.
