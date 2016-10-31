var fs = require('fs');
var https = require('https');
var AWS = require('aws-sdk');

module.exports = LambdaManager;

function LambdaManager(lambda, s3, codepath, jobId, fname, handler){
    this.lambda = lambda;
    this.region = "us-east-1";
    this.s3 = s3;
    this.codefile = codepath;
    this.jobId = jobId;
    this.functionName = fname;
    this.handler = handler;
    this.role = 'ROLE_ARN_S3_ACESS';
    this.memory = 1536;
    this.timeout = 300;
    this.functionArn = null;
}

LambdaManager.prototype.createLambdaFunction = function(cb){
    var params = {
      Code: { 
        ZipFile: fs.readFileSync(this.codefile)
      },
      FunctionName: this.functionName, 
      Handler: this.handler,
      Role: this.role, 
      Runtime: 'nodejs4.3', 
      Description: this.fname,
      MemorySize: this.memory,
      Timeout: this.timeout
      /*VpcConfig: {
          SecurityGroupIds: [
              'sg-',
          ],
          SubnetIds: [
              'subnet-',
              'subnet-'
          ]
      }
      */
    };

    var myobj = this;
    this.lambda.createFunction(params, function(err, data) {
      if (err) console.log(err, err.stack); // an error occurred
      else{
         console.log(data);// successful response
         // save FunctionArn
         myobj.functionArn = data.FunctionArn;
      } 
      cb(err, data);
    
    });
}

LambdaManager.prototype.addLambdaPermission = function(sId, bucket, cb){
    var params = {
      Action: 'lambda:InvokeFunction', 
      FunctionName: this.functionName, 
      Principal: 's3.amazonaws.com', 
      StatementId: sId,
      SourceArn: 'arn:aws:s3:::' + bucket
    };
    this.lambda.addPermission(params, cb);
}

/*
 * Create S3 EventSource
 *
 * prefix: jobId/mapper*
*/
LambdaManager.prototype.createS3EventSourceNotification = function(bucket, prefix, cb){
    if (!prefix)
        prefix = this.jobId +"/task";

    var params = {
      Bucket: bucket, 
      NotificationConfiguration: { /* required */
        LambdaFunctionConfigurations: [
          {
            Events: [ 's3:ObjectCreated:*'],
            LambdaFunctionArn: this.functionArn,
            Filter: {
              Key: {
                FilterRules: [
                  {
                    Name: 'prefix',
                    Value: prefix
                  },
                ]
              }
            }
          }
        ],
        TopicConfigurations:[],
        QueueConfigurations:[]
      }
    };
    this.s3.putBucketNotificationConfiguration(params, cb);
}

LambdaManager.prototype.removePermission = function(sId, cb){
    var params = {
      FunctionName: this.functionName, 
      StatementId: sId
    };
    this.lambda.removePermission(params, cb);
}


LambdaManager.prototype.deleteFunction = function(cb){
    var params = {
        FunctionName: this.functionName 
    };
    this.lambda.deleteFunction(params, cb);
}

LambdaManager.prototype.getNotificationConfiguration = function(bucket, cb){
    var params = {
      Bucket: bucket 
    };
    this.s3.getBucketNotificationConfiguration(params, cb);
}

LambdaManager.prototype.invokeLambda = function(params, cb){
        var agent = new https.Agent();
        agent.maxSockets = 10;
        var lambda = new AWS.Lambda({
            region: this.region,
            httpOptions: {
                agent: agent,
            timeout: 5*60000
            }
        });

        var status = lambda.invoke(params, function(err, obj){
            if (err){
                console.error(err);
                cb(err, null);
                return;
            }
            cb(null, obj);
        });
}
