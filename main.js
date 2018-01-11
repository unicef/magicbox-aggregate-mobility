// Aggregates aiport to airport mobility by country
// Amadeus provides a csv per month that has travel by week

// Has method to aggregate csv with spark.
const aggregate = require('./aggregate/spark_aggregate');
const async = require('async');
const fs = require('fs');
const bluebird = require('bluebird');
const exec = require('child_process').exec;
const config = require('./config');
const csv = require('csvtojson');
const util = require('./combine_spark_output');

// Directory to unzip csvs to.
let path_unzipped = config.unzipped;

// Path to spark output directory
let path_temp = config.temp;

// Path to direcotry where spark output is summarized
let path_processed = config.processed;

// Aggregation level at which the result will be generated
let aggregation_level = config.aggregation_level;

// Get list of all zipped csv files.
let zipped_csv_files = fs.readdirSync(config.zipped).filter(f => {
  return f.match(/_W_/);
});

// Get list of previously aggregated files.
let processed_files = fs.readdirSync(config.processed).filter(f => {
  return f.match(/.csv/);
});

// Filter out files that have already been processed
let zipped_csv_files_to_process = zipped_csv_files.filter(f => {
  let year_month = f.match(/\d{4}_\d{2}/);
  if (year_month) {
    year_month = year_month[0].replace('_', '-');
  } else {
    return false;
  }

  return !processed_files.find(p => {
    return p.match(year_month)
  })
})

async.waterfall([
  // Delete everything in temp directory
  function(callback) {
    get_date_lookup()
    .then(date_lookup => {
      callback(null, date_lookup);
    })
  },

  function(date_lookup, callback) {
    // Iterate through CSVs
    // aggregate it with spark
    // summarize output
    bluebird.each(zipped_csv_files_to_process, file => {
      console.log('Processing', file);
      return process_file(file, date_lookup);
    }, {concurreny: 1})
    .then(callback);
  }
], () => {
    console.log('Done with all!');
    process.exit();
  }, 1);

/**
 * Iterate through CSVs, aggregate it with spark, summarize output
 * @param{String} file - name of CSV
 * @param{Object} date_lookup - object that maps a year to a specific date 'YYYY-MM-DD'
 * @return{Promise} Fulfilled when records are returned
 */
function process_file(file, date_lookup) {
  return new Promise((resolve, reject) => {
    async.waterfall([
      // Delete everything in temp directory
      function(callback) {
        console.log('delete path_temp:', path_temp);
        let command = 'rm -rd ' + path_temp + ' && mkdir ' + path_temp;
        exec(command, (err, stdout, stderr) => {
          if (err) {
            console.error(err);
          }
          callback(null)
        });
      },

      function(callback) {
        clean_directories()
        .then(callback);
      },

      // Unzip file to process
      function(callback) {
        let unzipped_file = file.replace(/.gz$/, '');
        let command = 'gunzip -c ' + config.zipped + file +
          ' > ' + path_unzipped + unzipped_file;
        exec(command, (err, stdout, stderr) => {
          if (err) {
            console.error(err);
          }
          callback(null, unzipped_file)
        });
      },

      // Aggregate file.
      function(unzipped_file, callback) {
        aggregate.aggregate(
          unzipped_file,
          aggregation_level,
          path_unzipped,
          path_temp
        ).then(() => {
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
          path_processed,
          date_lookup
        )
        .catch(function(err) {
          console.log(err);
        })
        .then(function() {
          console.log('Done combining');
          callback(null, unzipped_file);
        });
      }
    ], () => {
        console.log('Done aggregating!!!');
        clean_directories()
        .then(resolve);
      }, 1);
  })
}

/**
 * Clean contents inside directories path_temp and path_unzipped
 * @return{Promise} Fulfilled when directories are cleaned
 */
function clean_directories() {
  return new Promise((resolve, reject) => {
    async.waterfall([
      // Delete everything in temp directory
      function(callback) {
        console.log('delete path_temp:', path_temp);
        let command = 'rm -rf ' + path_temp + '*';
        exec(command, (err, stdout, stderr) => {
          if (err) {
            console.error(err);
          }
          callback(null)
        });
      },

      // Delete everything in unzipped directory
      function(callback) {
        let command = 'rm ' + path_unzipped + '*';
        exec(command, (err, stdout, stderr) => {
          if (err) {
            console.error(err);
          }
          callback(null)
        });
      }
    ], () => {
        console.log('Done cleaning.');
        resolve();
      }, 1);
  })
}

/**
 * Amadeus weeks do not necessarily begin on the first of the year
 * Create lookup of first week to an exact date based on traffic_weeks_definitions.csv
 * @return{Promise} Fulfilled when date_lookup object is fully formed
 */
function get_date_lookup() {
  seen = {};
  let date_lookup = {};
  return new Promise((resolve, reject) => {
    const csvFilePath='./traffic_weeks_definitions.csv';
    csv({delimiter: ';'})
    .fromFile(csvFilePath)
    .on('json', (jsonObj) => {
      if (!seen[jsonObj.nYear]) {
        date_lookup[parseInt(jsonObj.nYear)] = jsonObj.datStart;
      }
      seen[jsonObj.nYear] = 1;
    })
    .on('done', (error) => {
      console.log('end read traffic_weeks_definitions')
      return resolve(date_lookup);
    })
  })
}
