/*
 * Reducer Function Handler
 *
 * */

var aws = require('aws-sdk');
var async = require('async');
var zlib = require('zlib');
var es = require('event-stream');
var s3 = new aws.S3({ apiVersion: '2006-03-01' });
var utils = require('./utils');
utils = new utils();

var TASK_REDUCER_PREFIX = "task/reducer"

exports.handler = function(event, context){
    console.log('Reducer Event invocation:', JSON.stringify(event, null, 2));

    var startTime = process.hrtime();
    // Get the object from the event and show its content type
    //const bucket =  event.Records[0].s3.bucket.name;
    //var key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    //var jobId =  key.split('/')[0];

    var bucket = event.bucket; 
    var jobBucket = event.jobBucket; 
    var jobId = event.jobId;
    var reducerKeys = event.keys;
    var rId = event.reducerId;
    var stepId = event.stepId;
    var nReducers = event.nReducers;
    
    // Aggregate: Return variables
    var results = {};
    var totalLines = 0;
    var totalTime = 0;
    var s3Files = 0;
    var warnings = [];
    var s3getMapperResult = function(srcKey, cb){
        try {      
            var warning = '';
            var pt = 0;
            // Create our processor that splits lines/words
            var lineSplitter = es.split();
            lineSplitter.on('data', function(line){
                /// YOUR REDUCER LOGIC ////
                var dataProcessingStartTime = process.hrtime(); 
                var data = line.split(',');
                var srcIp = data[0];
                if(srcIp.length >0){
                    totalLines ++;
                    if(!results[srcIp]){
                        results[srcIp] = 0;
                    }
                    results[srcIp] += parseFloat(data[1]); 
                    var et = process.hrtime(dataProcessingStartTime);
                    pt += (et[0] * 1e9 + et[1]);
                }
            })
            .on('error', function(err){
                console.error('Error in lineSplitter: ' + err);
            })
            .on('end', function(err){
                if (err){
                    console.log("Error", err);
                    cb(err, null);
                    return;
                }
                cb(null, [srcKey, warning, pt]);
            });

            // Create our pipeline
            if (srcKey.match(/\.gz/)){
                s3.getObject({
                    Bucket: bucket,
                    Key: srcKey
                })
                .createReadStream()
                    .pipe(zlib.createGunzip())
                    .pipe(lineSplitter);
            }
            else{
                s3.getObject({
                    Bucket: bucket,
                    Key: srcKey
                })
                .createReadStream()
                    .pipe(lineSplitter);
            }
        } catch (e) {
            console.error(e);
            cb(e, null);
        }
    };
    async.map(reducerKeys, s3getMapperResult, function (err, rResults){
        if (err){
            console.error(err);
        }
        console.log("Total lines", totalLines);
        console.log("Got s3 results", err, rResults.length);
        if (nReducers == 1){
            // Last reducer file, final result
            var fname = jobId + "/result"; 
        }else{
            var fname = jobId + "/" + TASK_REDUCER_PREFIX + stepId + rId;
        }
        
        console.log("# of reducers", nReducers, fname);
        
        // Time taken to process
        var et = process.hrtime(startTime);
        totalTime = (et[0] * 1e9 + et[1]);
        var metadata = {
            linecount: '' + totalLines,
            processingtime: '' + totalTime/1000000000,
            memoryUsage: JSON.stringify(process.memoryUsage())
        };
        
        utils.writeToS3(s3, jobBucket, fname, utils.objToCSVBuffer(results), metadata, function(err, data){
            if (err)
                console.error("S3 Error", err);
            var fname = jobId + "/reducerexectime/rtime_" + stepId + "_" + rId + "." + totalTime;
            utils.writeToS3(s3, jobBucket, fname, "" + totalTime, {}, function(err, data){

                // TODO: Delete RM & Reducer lambda
                context.succeed('Finished Reducer Step' + stepId + " reducer " + rId);
            });
        });
    });
}

/*
/// MAIN
var ev = {
    "bucket": "smallya-useast-1",
    "jobBucket": "smallya-useast-1",
    "jobId": "jobid134",
    "nReducers": 2,
    "keys": ["jobid134/task/mapper/1"],
    "reducerId": 1, 
    "stepId" : 1
}
var cx = {} 
cx.succeed = function(x){ console.log(x)}
callback = function(x, y){ console.log(x, y)}
exports.handler(ev, cx, callback);
*/
