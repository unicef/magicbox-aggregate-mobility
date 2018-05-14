// Aggregates aiport to airport mobility by country
// Amadeus provides a csv per month that has travel by week

// Has method to aggregate csv with spark.
const aggregate = require('./aggregate/spark_aggregate')
const async = require('async')
const fs = require('fs')
const path = require('path')
const tmp = require('tmp')
const zlib = require('zlib')
const bluebird = require('bluebird')
const config = require('./config')
const csv = require('csvtojson')
const util = require('./combine_spark_output')

// Directory to unzip csvs to.
let path_unzipped = tmp.dirSync({prefix: 'unzipped-', unsafeCleanup: true}).name

// Path to spark output directory
let path_temp = tmp.dirSync({prefix: 'spark_output-', unsafeCleanup: true}).name

// Path to direcotry where spark output is summarized
let path_processed = config.processed

// Aggregation level at which the result will be generated
let aggregation_level = config.aggregation_level

// Get list of all zipped csv files.
let zipped_csv_files = fs.readdirSync(config.zipped).filter(f => {
  return f.match(/_W_/)
})

// Get list of previously aggregated files.
let processed_files = fs.readdirSync(config.processed).filter(f => {
  return f.match(/.csv/)
})

// Filter out files that have already been processed
let zipped_csv_files_to_process = zipped_csv_files.filter(f => {
  let year_month = f.match(/\d{4}_\d{2}/)
  if (year_month) {
    year_month = year_month[0].replace('_', '-')
  } else {
    return false
  }

  return !processed_files.find(p => {
    return p.match(year_month)
  })
})

/**
 * Amadeus weeks do not necessarily begin on the first of the year
 * Create lookup of first week to an exact date based on traffic_weeks_definitions.csv
 * @return{Promise} Fulfilled when date_lookup object is fully formed
 */
const get_date_lookup = () => {
  seen = {}
  let date_lookup = {}
  return new Promise((resolve, reject) => {
    const csvFilePath='./traffic_weeks_definitions.csv'
    csv({delimiter: ''})
    .fromFile(csvFilePath)
    .on('json', (jsonObj) => {
      if (!seen[jsonObj.nYear]) {
        date_lookup[parseInt(jsonObj.nYear)] = jsonObj.datStart
      }
      seen[jsonObj.nYear] = 1
    })
    .on('done', (error) => {
      console.log('end read traffic_weeks_definitions')
      return resolve(date_lookup)
    })
  })
}

async.waterfall([
  // Delete everything in temp directory
  (callback) => {
    get_date_lookup()
    .then(date_lookup => {
      callback(null, date_lookup)
    })
  },

  (date_lookup, callback) => {
    // Iterate through CSVs
    // aggregate it with spark
    // summarize output
    bluebird.each(zipped_csv_files_to_process, file => {
      console.log('Processing', file)
      return process_file(file, date_lookup)
    }, {concurreny: 1})
    .then(callback)
    .catch( (err) => {
        console.log('Failed, bailing out...')
        process.exit(1)
    })
  }
], () => {
    console.log('Done with all!')
    process.exit()
  }, 1)

/**
 * Iterate through CSVs, aggregate it with spark, summarize output
 * @param{String} file - name of CSV
 * @param{Object} date_lookup - object that maps a year to a specific date 'YYYY-MM-DD'
 * @return{Promise} Fulfilled when records are returned
 */
const process_file = (file, date_lookup) => {
  return new Promise((resolve, reject) => {
    async.waterfall([
      // Unzip file to process
      (callback) => {
        let unzipped_file = file.replace(/.gz$/, '')
        let zippedFileStream = fs.createReadStream(path.join(
          config.zipped,
          file
        ))
        let unzippedFileStream = fs.createWriteStream(path.join(
          path_unzipped,
          unzipped_file
        ))

        zippedFileStream.pipe(zlib.createGunzip()).pipe(unzippedFileStream)
        callback(null, unzipped_file)
      },

      // Aggregate file.
      (unzipped_file, callback) => {
        aggregate.aggregate(
          unzipped_file,
          aggregation_level,
          path_unzipped,
          path_temp
        ).then(() => {
          callback(null, unzipped_file)
        }).catch((e) => {
          reject(e)
        })
      },

      // Combine all the output
      (unzipped_file, callback) => {
        console.log('Start combining')
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
        .catch((err) => {
          console.log(err)
          reject(err)
        })
        .then(() => {
          console.log('Done combining')
          callback(null, unzipped_file)
        })
      }
    ], () => {
        console.log('Done aggregating!!!')
        resolve()
      }, 1)
  })
}
