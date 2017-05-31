var aggregate = require('./aggregate/spark_aggregate');
var async = require('async');
var fs = require('fs');
var bluebird = require('bluebird');
var exec = require('child_process').exec;
var config = require('./config');
var zipped = fs.readdirSync(config.zipped).filter(f => { return f.match(/_W_/); });
var unzipped_files = fs.readdirSync(config.unzipped);
var path_unzipped = config.unzipped;
var path_processed = config.processed;
var path_temp = config.temp;
var util = require('./utility');

bluebird.each(zipped, file => {
  console.log('Processing', file);
  return process_file(file);
}, {concurreny: 1})
.then(process.exit);

function process_file(file) {
  return new Promise((resolve, reject) => {
    async.waterfall([

      function(callback) {
        var command = 'rm -rf ' + path_processed + '*';
        exec(command, (err, stdout, stderr) => {
          if (err) {
            console.error(err);
          }
          callback(null)
        });
      },

      function(callback) {
        console.log('delete path_temp:', path_temp);
        var command = 'rm -rf ' + path_temp + '*';
        exec(command, (err, stdout, stderr) => {
          if (err) {
            console.error(err);
          }
          callback(null)
        });
      },
      function(callback) {
        var command = 'rm ' + path_unzipped + '*';
        exec(command, (err, stdout, stderr) => {
          if (err) {
            console.error(err);
          }
          callback(null)
        });
      },
      function(callback) {
        var unzipped_file = file.replace(/.gz$/, '');
        var command = 'gunzip -c ' + config.zipped + file + ' > ' + path_unzipped + unzipped_file;
        exec(command, (err, stdout, stderr) => {
          if (err) {
            console.error(err);
          }
          callback(null, unzipped_file)
        });
      },

      function(unzipped_file, callback) {
        aggregate.aggregate(unzipped_file, path_unzipped, path_temp)
        .then(() => {
          callback(null, unzipped_file)
        })
      },

      function(unzipped_file, callback) {
        console.log('Start combining');
        util.combine_spark_output(file.replace(/.gz$/, ''))
        .catch(function(err) {
          console.log(err);
        })
        .then(function() {
          console.log('Done combining');
          callback(null, unzipped_file);
        });
      },

    ], () => {
        console.log("Done aggregating!!!");
        resolve();
      }, 1);
  })
}
