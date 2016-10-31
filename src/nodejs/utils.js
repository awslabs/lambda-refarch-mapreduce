module.exports = utils;

function utils(){
}

utils.prototype.writeToS3 = function(s3, bucket, key, data, m, callback){
    params = {
        Bucket: bucket,
        Key: key,
        Body: data,
        Metadata: m
    };
    s3.putObject(params, callback);
}

utils.prototype.batchCreator = function(allKeys, batchSize){

    var batches = [];
    var batch = [];
    for (var i = 0, len = allKeys.length; i < len; i++){
        batch.push(allKeys[i]);
        if (batch.length >= batchSize){
            batches.push(batch.slice());
            batch = [];
        }
    }
    if (batch.length){
        batches.push(batch.slice());
    }
    return batches;
}

utils.prototype.objToCSVBuffer = function(obj) {
    var str = '';
    for (var p in obj) {
        str += p + ',' + obj[p] + '\n';
    }
    return str; 
}

utils.prototype.zipLambda = function(file, target){
    require('child_process').execSync('zip ' + target  + ' -r node_modules/* utils.js jobinfo.json ' + file);
}

utils.prototype.computeBatchSize = function(keys, lambdaMemory, gzip){

    // NOTE: This is an approx computation of batchSize, there may be corner cases in your dataset
    // if a certain file or chunk results in a skewed reduce

    //TODO: Smarter allocation of mapper batches; Tackle corner cases
    var MAX_MEMORY_FOR_DATA = 0.6 * lambdaMemory * 1000 * 1000; // (conservative) 40% of mapper memory allocated for processing (in Bytes) 
    
    // If Gzip, should we reduce the allocation of memory? 
    if (gzip == true){
        MAX_MEMORY_FOR_DATA = 0.4 * lambdaMemory * 1000 * 1000; 
    }

    var size = 0;
    for(var i=0; i<keys.length; i++){
        size += keys[i].Size;
    }
    
    var avgObjectSize = size/keys.length;
    console.log("Dataset size, nKeys, avg", size, keys.length, avgObjectSize);
    var batchSize = Math.round(MAX_MEMORY_FOR_DATA/avgObjectSize);
    return batchSize;
}

utils.prototype.calcAverageLineSize = function(bucket, key, callback){
    var zlib = require('zlib');
    var es = require('event-stream');

    console.log(bucket, key);
    var maxLines = 100;
    var lineCount = 0;
    var size = 0;
    var lineSplitter = es.split();
    lineSplitter.on('data', function(line){
        if(lineCount++ < maxLines){
            size += line.length;
            return;
        }
        //callback(null, size/lineCount);
    })
    .on('error', function(err){
        console.error('Error in lineSplitter: ' + err);
        callback(err, null);
    })
    .on('end', function(err){
        callback(null, size/lineCount);
    });

    // Create our pipeline
    s3.getObject({
      Bucket: bucket,
      Key: key,
      Range: 'bytes=0-1024'
    })
    .createReadStream()
    .pipe(lineSplitter);
}



