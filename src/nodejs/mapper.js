/*
 * Mapper function handler
 *
 * */

var async = require('async');
var AWS = require('aws-sdk');
var zlib = require('zlib');
var es = require('event-stream');
var s3 = new AWS.S3();
var utils = require('./utils');
utils = new utils();

var procUtil = require('util');
var TASK_MAPPER_PREFIX = "task/mapper/";

exports.handler = function(event, context) {
    
    // calc execution time 
    var startTime = process.hrtime();
    
    var jobBucket = event.jobBucket;
    var srcBucket = event.bucket;
    var srcKeys = event.keys;
    var jobId = event.jobId;
    var mapperId = event.mapperId;
    
    // Counters
    var lineCount = 0;
    var totals = {};

    // CSV Processor to aggregate ad revenue for IPs 
    var mapfunc = function(srcKey, cb){
        //console.log("processing", srcKey);
        try {      
            var pt = 0; // processing time
           
            // Create our processor that splits lines/words
            var lineSplitter = es.split();
            lineSplitter.on('data', function(line){
                var dataProcessingStartTime = process.hrtime(); 
                lineCount++;
                data = line.split(',');
                srcIp = data[0].substr(1, 8);
                if(!totals[srcIp])
                    totals[srcIp] = 0;
                totals[srcIp] += parseFloat(data[3]);

                var et = process.hrtime(dataProcessingStartTime);
                pt += (et[0] * 1e9 + et[1]);
            })
            .on('error', function(err){
                console.error('Error in lineSplitter: ' + err);
            })
            .on('end', function(err){
                //console.log("End processing", procUtil.inspect(process.memoryUsage()));
                if (err){
                    console.error(err);
                    cb(err, null);
                    return;
                }
                cb(null, [srcKey, pt]);
            });

            // console.log('Downloading key ' + srcKey);
            
            ///// Create our pipeline /////
            //
            // If input files are gzipped
            // TODO: perhaps check content type as well?
            if (srcKey.match(/\.gz/)){
                s3.getObject({
                    Bucket: srcBucket,
                    Key: srcKey
                })
                .createReadStream()
                    .pipe(zlib.createGunzip())
                    .pipe(lineSplitter);
            }
            else {
                s3.getObject({
                    Bucket: srcBucket,
                    Key: srcKey
                })
                .createReadStream()
                .pipe(lineSplitter);
            }
        }catch (e){
            // Catch all during map processing
            console.error(e);
            cb(e, null);
        }
    };

    // Exec in Parallel
    async.map(srcKeys, mapfunc, function (err, results) {
        var tDiff = process.hrtime(startTime);
        var s3DownloadTime = tDiff[0] * 1e9 + tDiff[1];
        if (err){
            console.error(err);
            context.fail(err);
        }
        var totalProcessingTime = 0;  // Data processing time
        for (var i = 0, len = results.length; i < len; i++){
            totalProcessingTime +=  results[i][2];
        }
        var tDiff = process.hrtime(startTime);
        var timeTaken = tDiff[0] * 1e9 + tDiff[1];
        var timeInSecs = timeTaken/1000000000;

        // S3 File download time
        s3DownloadTime -= totalProcessingTime;

        //  S3Keys, TotalLines, TotalTime, S3DownloadTime, TotalDataProcessingTime
        pret = [srcKeys.length, lineCount, timeTaken, s3DownloadTime/1000000000, totalProcessingTime/1000000000]; //for print
        //console.log(pret);

        // Write mapper output
        var mapperFname = jobId + "/" + TASK_MAPPER_PREFIX + mapperId; 
        var metadata = {
                            linecount: '' + lineCount,
                            processingtime: '' + timeInSecs,
                            memoryUsage: JSON.stringify(process.memoryUsage())
                        };
        //console.log("before upload", procUtil.inspect(process.memoryUsage()));
        utils.writeToS3(s3, jobBucket, mapperFname, utils.objToCSVBuffer(totals), metadata, function(err, data){
            //console.log("after upload", procUtil.inspect(process.memoryUsage()));
            if(err)
                console.error("Write to S3 failed for " + mapperFname);
            context.succeed(pret);
        });
    });
};


/*
   var ev = {
   "bucket": "smallya-useast-1", 
   "keys": ["pavlo.sample"],
   "jobId": "jobid134",
   "mapperId": 1,
   "jobBucket": "smallya-useast-1"
   }
   var cx = {} 
   cx.succeed = function(x) { console.log(x);}
   exports.handler(ev,cx)
*/
