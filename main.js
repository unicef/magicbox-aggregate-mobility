// Aggregates aiport to airport mobility by country
// Amadeus provides a csv per month that has travel by week

// Has method to aggregate csv with spark.
var aggregate = require('./aggregate/spark_aggregate');
var async = require('async');
var fs = require('fs');
var bluebird = require('bluebird');
var exec = require('child_process').exec;
var config = require('./config');

// Get list of all zipped csv files.
var zipped_csv_files = fs.readdirSync(config.zipped).filter(f => {
  return f.match(/_W_/);
});

// Directory to unzip csvs to.
var path_unzipped = config.unzipped;

// Path to spark output directory
var path_temp = config.temp;

// Path to direcotry where spark output is summarized
var path_processed = config.processed;

// Has method to combine spark output
var util = require('./utility');

// Iterate through CSVs
// aggregate it with spark
// summarize output
bluebird.each(zipped_csv_files, file => {
  console.log('Processing', file);
  return process_file(file);
}, {concurreny: 1})
.then(process.exit);

/**
 * Iterate through CSVs, aggregate it with spark, summarize output
 * @param{String} file - name of CSV
 * @return{Promise} Fulfilled when records are returned
 */
function process_file(file) {
  return new Promise((resolve, reject) => {
    async.waterfall([
      // Delete everything in temp directory
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

      // Delete everything in unzipped directory
      function(callback) {
        var command = 'rm ' + path_unzipped + '*';
        exec(command, (err, stdout, stderr) => {
          if (err) {
            console.error(err);
          }
          callback(null)
        });
      },

      // Unzip file to process
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

      // Aggregate file.
      function(unzipped_file, callback) {
        aggregate.aggregate(unzipped_file, path_unzipped, path_temp)
        .then(() => {
          callback(null, unzipped_file)
        })
      },

      // Combine all the output
      function(unzipped_file, callback) {
        console.log('Start combining');
        console.log(file.replace(/.gz$/, ''),
        path_temp,
        path_processed
      )
        util.combine_spark_output(
          file.replace(/.gz$/, ''),
          path_temp,
          path_processed
        )
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
