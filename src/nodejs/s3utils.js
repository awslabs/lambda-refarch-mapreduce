/*
 *
 * Copyright (c) 2015 Reason [reason -A- exratione.com]
 * This code has been modified. Portions copyright 2016 Amazon.com, Inc. or its affiliates.
 * SPDX-License-Identifier: (MIT AND MIT-0)
 * */

var _ = require('underscore');
var AWS = require('aws-sdk');
AWS.config.update({
        region: "us-east-1",
});
var s3 = new AWS.S3();

module.exports = s3utils;
function s3utils(){
}

// Code below is modified from source under the MIT License:
// https://www.exratione.com/2015/05/listing-large-s3-buckets-with-the-aws-sdk-for-node-js/
s3utils.prototype.listKeys = function(options, callback) {
    var resultKeys = [];
    var listKeyPage = this.listKeyPage;
    function listKeysRecusively(marker) {
        options.marker = marker;
        listKeyPage(options, function(error, nextMarker, keyset){
            if (error) {
                return callback(error, resultKeys);
            }
            resultKeys = resultKeys.concat(keyset);
            if (nextMarker) {
                listKeysRecusively(nextMarker);
            } else {
                callback(null, resultKeys);
            }
        });
    }
    listKeysRecusively();
}

s3utils.prototype.listKeyPage = function(options, callback) {
    var params = {
        Bucket : options.bucket,
        Marker : options.marker,
        MaxKeys : 1000,
        Prefix : options.prefix
    };

    s3.listObjects(params, function (error, response) {
        if (error) {
            return callback(error);
        } else if (response.err) {
            return callback(new Error(response.err));
        }
        var keys = _.map(response.Contents, function (item) {
            return item;
        });

        var s3Marker = null;
        if (response.IsTruncated) {
            s3MarkerObj = keys[keys.length - 1];
            s3Marker = s3MarkerObj.Key;
        }
        callback(null, s3Marker, keys);
    });
}

/*
listKeys({
  bucket: 'X',
  prefix: 'X'

}, function (error, folders) {
  if (error) {
    console.error(error);
  }
  _.each(folders, function (folder) {
    console.log(folder);
  });
});
*/
