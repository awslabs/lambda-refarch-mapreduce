/*

    Reducer Coordinator: State Manager and scheduler for BigLambda job 

    If all mappers are done:
        NReducers = Get the number of the reducers needed;
        If  NReducers == 1:
           Invoke single reducer and write the results;
           Job done;
        Else create event source for reducer;
    Else:
        Return
   
   
 Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 SPDX-License-Identifier: MIT-0
 */


var async = require('async');
var AWS = require('aws-sdk');
var AWS = require('aws-sdk');
var async = require('async');
var config = require('config.json')('./jobinfo.json');
var es = require('event-stream');
var https = require('https');
var utils = require('./utils'); 
var zlib = require('zlib');
var s3 = new AWS.S3({ apiVersion: '2006-03-01' });
var utils = new utils();
var DEFAULT_REGION = "us-east-1";

/// STATES
var MAPPERS_DONE = 0;
var REDUCER_STEP = 1;

function invokeLambda(params, cb){
        var agent = new https.Agent();
        agent.maxSockets = 10;
        var lambda = new AWS.Lambda({
            region: DEFAULT_REGION,
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

function getJobFiles(lparams, cb){
    s3.listObjects(lparams, function (err, data) {
        if(err){cb(err, null); return}
        cb(null, data.Contents);
    });
}


function writeReducerState(nReducers, nS3, bucket, fname, cb){
    var ts = new Date().getTime();
    var data = JSON.stringify({
                "reducerCount": nReducers, 
                "totalS3Files": nS3,
                "startTime": ts 
               });
    utils.writeToS3(s3, bucket, fname, data, {}, cb);
}

exports.handler = function(event, context, callback){
    
    var startTime = process.hrtime();
    const bucket = event.Records[0].s3.bucket.name;
    var key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    console.log("Event Key", key);
    var jobId =  config.jobId; //"lmrtest2" ; //event.jobId;
    var mapCount = config.mapCount ; //267; //event.mapCount;
    var reducerFunctionName = config.reducerFunction; 
    var reducerHandler = config.reducerHandler; 

    //var mparams = {Bucket: bucket, Key: jobId + "/jobdata"};
    //s3.getObject(mparams, function(err, data){
    //    var contents = JSON.parse(data.Body.toString());
    //    console.log("----", contents);
    //});

    //// Reducer Coorindator Flow ////
    // 1. Check if all mappers are done
    // 2. Compute the # of the reducers needed
    // 3. If 1 reducer, then done
    // 4. Else create event source for reducer; Repeat!

    // Count mapper files
    function getMapperFiles(mappers){
        var ret = [];
        var re = /task\/mapper\/\d+/i;
        for (var i=0; i<mappers.length; i++){ 
            if (mappers[i].Key.match(re)){
                ret.push(mappers[i]);
            }
        }
        return ret;
    }

    function getReducerStateInfo(files, cb){
        var reducers = [];
        var maxIndex = 0;
        var reducerStep = false;
        var rIndex = 0;
   
        // CHECK if step is complete !

        // Check for the Reducer state
        for (var i=0; i<files.length; i++){
            var parts = files[i].Key.split('/');
            var re = /reducerstate.\d+/i;
            if(files[i].Key.match(re)){
                console.log("Found reducers -->", files[i].Key); 
                var idx = parseInt(files[i].Key.split('.')[1]);
                if (idx > rIndex)
                    rIndex = idx;
                reducerStep = true;
            }
        }
        
        // This is the first reducer
        if (reducerStep === false){
            var mapperOutputFiles = getMapperFiles(files);
            cb([MAPPERS_DONE, mapperOutputFiles]);
            return;
        }
        else{
            // Check if the reducer id done
            var params = {Bucket: bucket, Key: jobId + "/reducerstate." + rIndex}; 
            s3.getObject(params, function(err, data){
                var contents = JSON.parse(data.Body.toString());
                for (var i=0; i<files.length; i++){
                    // JobID/task/reducer{n}/1
                    var parts = files[i].Key.split('/');
                    if(parts.length < 3)
                        continue
                    var op = parts[2];
                    if (op.indexOf('reducer' + rIndex) >=0){
                        reducers.push(files[i]);
                    }
                }
                
                if(contents["reducerCount"] == reducers.length){
                    // Reducer step complete; move to the next reducer
                    cb([rIndex, reducers]);
                }else{
                    // not complete
                    cb([rIndex, []]);
                }

            });
            
        }
    }

    //
    function getReducerBatchSize(keys){
        // Try and create a balance distribution 
       
        //if (config.reducerBatchSize >= 2)
        //    return config.reducerBatchSize;
        
        //TODO: Read metadata & determine the # of reducers
        var batchSize = utils.computeBatchSize(keys, 1536); 
        if (batchSize >=2)
            return batchSize;
        return 2; // Atleast 2 so that reducer can terminate
    }

    function _check_job_done(files){
        var re = /result/i;
        for (var i=0; i<files.length; i++){ 
            if (files[i].Key.match(re)){
                return true; 
            }
        }
        return false;
    }
    
    function _get_keys(objs){
        keys = [];
        for(var i=0; i<objs.length; i++)
            keys.push(objs[i].Key);
        return keys;
    }

    function _get_s3_stats(cb){

        /// Get the average max & avg of 3 keys.
        params = {}
        s3.getObject(params, function(err, data){
            var size = data.ContentLength; 
            var linecount = data.Metadata.linecount; 
            cb(size, linecount);
        });
    }

    // Get Mapper Finished Count
    var lparams = {
        Bucket: bucket,
        Prefix: jobId 
    };

    getJobFiles(lparams, function(err, files){
        if(err){
            console.error("S3 Error: ", err);
            //?
        }

        // Check if Job is done
        if (true === _check_job_done(files)){
            // TODO: Unsubscribe from the event source
            callback(null, "Job done!!! Check the result file: " + jobId + "/result" );
            return;
        }

        // Stateless Coordinator logic
        var mapperKeys = getMapperFiles(files);
        console.log("Mappers Done so far ", mapperKeys.length);
        if (mapCount == mapperKeys.length){
            // All the mappers have finished, time to schedule the reducers

            getReducerStateInfo(files, function(stepInfo){

                var stepNumber = stepInfo[0];
                var reducerKeys = stepInfo[1];
               
                if(reducerKeys.length == 0){
                    callback(null, "Still waiting to finish Reducer step " + stepNumber);
                    return;
                }
                 
                // Compute this based on metadata of files
                var rBatchSize = getReducerBatchSize(reducerKeys); 
                
                console.log("Starting the the reducer step", stepNumber);
                console.log("Batch Size", rBatchSize);
                
                // Create Batch params for the Lambda function
                var rBatchParams = utils.batchCreator(reducerKeys, rBatchSize);
                
                /// Build the lambda parameters
                var nReducers = rBatchParams.length;
                var nS3 = nReducers * rBatchParams[0].length;
                var stepId = stepNumber +1;
                var lambdaBatchParams = [];
                for (var i=0; i<rBatchParams.length; i++){
                    var batch = rBatchParams[i];
                        var params = {
                            FunctionName: reducerFunctionName,
                            InvocationType: 'Event',
                            Payload: JSON.stringify({
                                bucket: bucket,
                                keys: _get_keys(batch),
                                jobBucket: bucket,
                                jobId: jobId,
                                nReducers: nReducers, 
                                stepId: stepId, 
                                reducerId: i 
                            })
                        };
                    lambdaBatchParams.push(params);
                }

                console.log("LBR", lambdaBatchParams);

                /// Stream the files from S3 & Reduce
                async.map(lambdaBatchParams, invokeLambda, function (err, rResults){
                    if (err){
                        console.error(err);
                        callback(null, 'Reducer invocation error');
                        return;
                    }

                    var fname = jobId + "/reducerstate." + stepId;
                    writeReducerState(nReducers, nS3, bucket, fname, function(err, data){
                        callback(null, 'Invoked Reducer Step');
                    });
                });


          });
        }else{
            ///
            console.log("Still waiting for all the mappers to finish ..");
            callback(null, "I am still waiting for all the mappers to finish ..");
        } 
    });
};

/*
 * var ev = {
    "bucket": "smallya-useast-1",
    "jobId": "jobid134",
    "mapCount": 1,
    "reducerFunctionName": "shell-exec",
    "reducerHandler": "index.handler"
}
var cx = {} 
cx.succeed = function(x){ console.log(x)}
callback = function(x, y){ console.log(x, y)}
exports.handler(ev, cx, callback);
*/
