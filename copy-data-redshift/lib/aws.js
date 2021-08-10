"use strict";

const aws = require('aws-sdk');

module.exports.awsRedshift = new aws.RedshiftData({
    apiVersion : '2019-12-20',
    region : 'us-west-2'
});

module.exports.awsS3 = new aws.S3();