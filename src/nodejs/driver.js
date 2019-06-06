/*

 // Driver function to start the MapReduce Job

 Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 SPDX-License-Identifier: MIT-0
 */


var async = require('async');
var AWS = require('aws-sdk');
var https = require('https');
var fs = require('fs');
var lambdautils = require('./lambdautils');
var s3 = new AWS.S3();
var utils = require('./utils');
utils = new utils();
var s3utils = require('./s3utils');
s3utils = new s3utils();

var DEFAULT_REGION = 'us-east-1';
var jobId = "biglambda-1node-0"; //randomIdGenerator();

/// JSON DRIVER CONFIG FILE 
var config = require('config.json')('./driverconfig.json');
    
///// Helper functions /////
function randomIdGenerator(N){
    var text = "";
    if (!N)
        N = 8;
    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    for(var i=0; i<N; i++)
        text += possible.charAt(Math.floor(Math.random() * possible.length));
    return text;
}

// Write the current job state for the cooridinator in S3
function writeJobData(nMappers, nS3, bucket, fname, cb){
    var ts = new Date().getTime();
    var data = JSON.stringify({
                "mapCount": nMappers, 
                "totalS3Files": nS3,
                "startTime": ts 
               });
    utils.writeToS3(s3, bucket, fname, data, {}, cb);
}

/// Write the job config file
function writeJobConfig(jobId, jobBucket, nMappers, rFunc, rHandler){
    var fname = "jobinfo.json"; 
    data = JSON.stringify({
        "jobId": jobId,
        "jobBucket" : jobBucket,
        "mapCount": nMappers,
        "reducerFunction": rFunc,
        "reducerHandler": rHandler
        });
    fs.writeFileSync(fname, data); 
}

/////////////// Pseduo Lambda Handler  /////////////////

exports.handler = function(event, context) {
    
    var bucket = event.bucket;
    if (typeof(bucket) === 'undefined')
        context.fail('No bucket provided in params.');
    var allKeys = event.keys;
    if (typeof(allKeys) === 'undefined')
        context.fail('No keys provided.');
    var region = event.region;
    if (typeof(region) === 'undefined')
        region = DEFAULT_REGION;
    var batchSize = event.batchSize;
    if (typeof(batchSize) === 'undefined')
        batchSize = DEFAULT_BATCH_SIZE;

    // Additional Configs
    // Generate a unique ID for the JOB
    var statementId = randomIdGenerator(8);
    var mapperLambdaName = "BigLambda-mapperfunc-" + jobId;
    var reducerLambdaName = "BigLambda-reducerfunc-" + jobId;
    var coordinatorLambdaName = "BigLambda-reducercor-" + jobId;

    var lambdaBilledMS = 0;
    var mapperId = 1; //Id counter
    var agent = new https.Agent();
    agent.maxSockets = config.concurrentLambdas;
    var lambda = new AWS.Lambda({
            region: region,
            httpOptions: {
                agent: agent,
                timeout: 5*60000
            }
    });

    // Lambda Invoker
    function LambdaInvoker(keys, cb){
        var params = {
            FunctionName: mapperLambdaName,
            InvocationType: 'RequestResponse',
            LogType: 'Tail', // Tail the log 
            Payload: JSON.stringify({
                bucket: bucket,
                keys: keys,
                jobBucket: config.jobBucket,
                jobId: jobId,
                mapperId: mapperId 
            })
        };

        // Invoke the Lambda function
        var lStatus = lambda.invoke(params, function(err, obj){
            if (err){
                console.error(err);
                cb(err, null);
                return;
            }
            try{
                var log = (new Buffer(obj.LogResult, 'base64')).toString();
                console.log("Mapper log", log);
            }
            catch(err){
            }
            cb(null, JSON.parse(obj.Payload));
            return;
        });
        mapperId += 1;
    }

    // Get all the batches
    var batches = utils.batchCreator(allKeys, batchSize);
    
    // # of mappers
    var nMappers = batches.length; 

    // Write Job config data
    writeJobConfig(jobId, config.jobBucket, nMappers, reducerLambdaName, config.reducer.handler);

    // Zip Mappers & reducers
    utils.zipLambda(config.mapper.name, config.mapper.zip);
    utils.zipLambda(config.reducer.name, config.reducer.zip);
    utils.zipLambda(config.reducerCoordinator.name, config.reducerCoordinator.zip);

    // Upload Mapper function
    var lmapper = new lambdautils(lambda, s3, config.mapper.zip, jobId, mapperLambdaName, config.mapper.handler);
    var lreducer = new lambdautils(lambda, s3, config.reducer.zip, jobId, reducerLambdaName, config.reducer.handler);
    var lreducerMgr = new lambdautils(lambda, s3, config.reducerCoordinator.zip, jobId, coordinatorLambdaName, config.reducerCoordinator.handler);

    console.log("create Mapper Lambda");
    lmapper.createLambdaFunction(function(err, data){
        console.log("create reducer");
        lreducer.createLambdaFunction(function(err, data){ 
            console.log("create the Reducer Coordinator Lambda");
            lreducerMgr.createLambdaFunction(function(err, data){
                console.log("Add permission to the Reducer Coordinator Lambda");
                lreducerMgr.addLambdaPermission(statementId, config.jobBucket, function(err, data){
                   console.log("add event source to the Reducer Coordinator");
                    lreducerMgr.createS3EventSourceNotification(config.jobBucket, null, function(err, data){
                        
                        /// Write JobData to S3 
                        var fname = jobId + "/jobdata";
                        writeJobData(nMappers, allKeys.length, config.jobBucket, fname, function(err, data){ console.log(err, data);});
                        console.log("Total # of Lambdas", batches.length,"batch size", batchSize);
                        console.log("Invoke the mappers");
                        
                        // Invoke each batch in parallel, returning aggregated result when
                        async.map(batches, LambdaInvoker, function (err, results) {
                            if (err) {
                                console.error('error on invoke', err);
                                context.fail('async.map error: ' + err.toString());
                                return;
                            }
                            // TODO: Delete reducer function after completion of all the results
                            // TODO: Delete CloudwatchLog group
                            
                            console.log("delete Mapper Lambda: " + mapperLambdaName);
                            lmapper.deleteFunction(function(err, data){
                                var s3cost = 0;
                                var totalLines = 0;
                                var totalS3DownloadTime = 0;
                                var lambdaExecTime = 0;

                                for(var i=0; i<results.length; i++){
                                    s3cost += ((0.004/10000) * results[i][0]);
                                    totalLines += results[i][1];
                                    lambdaExecTime += results[i][2];
                                    totalS3DownloadTime += results[i][2];
                                }
                                
                                console.log("total lines", totalLines);

                                // Approx lambda execution time
                                var lambdaBilledMS = lambdaExecTime/1000000;
                                var lambdaCost = (lambdaBilledMS /1000) * 0.00001667 * lmapper.memory/ 1024.0; 
                                context.succeed([results, lambdaBilledMS, s3cost+lambdaCost, s3cost]);
                            });

                        });
                    }); //event source 
                });///add permission
            });///create reducer Coordinator lambda 
        });///create reducer lambda 
    }); ///create mapper lambda
};

////// TEST /////
var cx = {} 
cx.succeed = function(x){ console.log(x)}
cx.fail = function(x){ console.log(x)}

//// S3 Query ////
// Map intial set of keys to the mapper
// Get list of keys and the size of the input to decide on # of mappers

// Function to invoke the mappers once we have mapped the set of keys to run the map reduce job on
var startMappers = function(error, keys){
    if (error) {
        console.error(error);
    }

    var batchSize = utils.computeBatchSize(keys, 1536);
    console.log(batchSize, keys.length, keys.length/batchSize);

    var keyNames = [];
    for(var i=0; i<keys.length; i++){
        keyNames.push(keys[i].Key);   
    }
    
    var ev = {
        "bucket": s3params.bucket, 
        "jobBucket": "smallya-useast-1",
        "region": "us-east-1",
        "keys": keyNames,
        "batchSize": batchSize
    }

    console.log(config);
    exports.handler(ev, cx);
   
}

///// MAIN ////////
var s3params = { 
  bucket: config.sourceBucket, 
  prefix: config.prefix 
};
var keyset = s3utils.listKeys(s3params, startMappers);
