var fs = require('fs');
var util = require('./combine_spark_output');
var config = require('./config');
var path = require('path');
var expect = require('chai').expect;
var assert = require('chai').assert;
var moment = require('moment');
var summarizedDataDir = './test/data/summarized/';
var tempDir = './test/temp/';
var testFile = 'test.csv';
var origin = 0;
var destination = 1;
var count = 2;
var date_lookup = {'2015': '2015-01-05'};

// Test for utility.js
describe('testing utility', function() {
    // checking if any file is present in test/summarized/. If yes we need to delete it otherwise
    // we will get multiple similar entries.
    before(function() {
        if (fs.existsSync(summarizedDataDir)) {
            fs.readdirSync(summarizedDataDir).forEach(file => {
                fs.unlinkSync(summarizedDataDir + file);
            });
        }
    });

    it ('checking combine_spark_output', function(done) {
        // calling combine_spark_output method on test data
        util.combine_spark_output(testFile, tempDir, summarizedDataDir, date_lookup).then(data => {
            var fileList = fs.readdirSync(summarizedDataDir);
            var csvFiles = fileList.filter(file => {
                return (path.extname(file) === '.csv')
            });
            // checking that we have csv file generated after combining spark outout
            assert.isAbove(csvFiles.length, 0, "File not found in " + config.processed);
            var expectedData =  getExpectedData();
            // reading csv file generated
            var data = fs.readFileSync(summarizedDataDir + csvFiles[0], 'utf8');

            // checking each entry in csv file and verifying it with our expected results.
            data.split(/\n/).forEach(line => {
                    var values = line.split(/,/);
                    if (expectedData.has(values[origin] + values[destination])) {
                        assert.equal(values[count], expectedData.get(values[origin] + values[destination]),
                        "Cound did not match for " + values[origin] + '-' + values[destination]);
                    }
            });
        });
        done();
    });
});

/**
 * Makes and returns a Map holding expected result.
 * Key is a combination of origin and destination (e.g. 'USIN' when origin is USA and destination is India )
 * value is the count of travellers.
 * @return {Map} expectedData holds expected result of the test
 */
function getExpectedData() {
    var expectedData = new Map();
    expectedData.set('IQTR' , '2');
    expectedData.set('VNVN' , '16');
    expectedData.set('MXES' , '0');
    expectedData.set('BODK' , '0');
    expectedData.set('TRUA' , '2');
    expectedData.set('USPR' , '1');
    expectedData.set('USUA' , '0');
    expectedData.set('USUS' , '0');
    return expectedData;
}
