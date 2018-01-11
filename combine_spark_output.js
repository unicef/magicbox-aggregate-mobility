const config = require('./config');
const fs = require('fs');
const bluebird = require('bluebird');
const fields = config.fields;
const moment = require('moment');


/**
 * Combines spark output to files by week date
 * @param{String} file_named_dir - name of CSV that spark uses to create dir to store output
 * @param{String} path_temp - directory where spark stores output
 * @param{String} path_summarized - diretory to store summarized travel by week
 * @param{Object} date_lookup - object that maps a year to a specific date 'YYYY-MM-DD'
 * @return{Promise} Fulfilled when records are returned
 */
exports.combine_spark_output = (
  file_named_dir, // Example: unicef_traffic_W_2017_01.csv
  path_temp,
  path_summarized,
  date_lookup
) => {
  // Slurp file names of spark output: Example: part-r-00000-78d8da1e-4612-40fb-ad03-3eacfc0214d6.csv
  let files = fs.readdirSync(path_temp + file_named_dir).filter(f => {
    return f.match(/csv$/)
  });
  return new Promise((resolve, reject) => {
    bluebird.each(files, f => {
      console.log('File', f);

      return process_file(
        f,
        file_named_dir,
        path_temp,
        path_summarized,
        date_lookup
      );
    }).catch(reject)
    .then(() => {
      console.log('done all files');
      resolve();
    });
  });
};

/**
 * Create or append travel by file titled with date of week
 * Create hash with week number as key and array of records for that week as value
 * @param{String} f - file output by spark - part-r-00000-78d8da1e-4612-40fb-ad03-3eacfc0214d6.csv
 * @param{String} file_named_dir - name of CSV that spark uses to create dir to store output
 * @param{String} path_temp - directory where spark stores output
 * @param{String} path_summarized - diretory to store summarized travel by week
 * @param{Object} date_lookup - object that maps a year to a specific date 'YYYY-MM-DD'
 * @return{Promise} Fulfilled when records are returned
 */
const process_file = (
  f,
  file_named_dir,
  path_temp,
  path_summarized,
  date_lookup
) => {
  return new Promise((resolve, reject) => {
    let records = {};
    let data = fs.readFileSync(path_temp + file_named_dir + '/' + f, 'utf8');
    // Sometimes spark files are empty
    if (!data) {
      return resolve();
    }
    let lines = data.split(/\n/);
    lines.forEach(l => {
      // Split line by ',' and map items to fields: ['year', 'week', 'count', 'origin', 'destination']
      // vals is an object: {year: 2017, week: 3, origin: 'US', destination: 'MX', cnt: 100}
      let vals = l.split(/,/).reduce((h, val, index) => {
        h[fields[index++]] = val;
        return h
      }, {})

      if (vals.week) {
        if (!records[vals.week]) {
          records[vals.week] = [];
        }
        records[vals.week].push(vals);
      }
    });

    bluebird.each(Object.keys(records), week => {
      let year = records[week][0].year;
      let date = moment(date_lookup[year])
        .add(week -1, 'weeks')
        .format('YYYY-MM-DD');
      return create_or_append(path_summarized + date + '.csv', records[week])
    }).catch(console.log).then(resolve);
  })
}

/**
 * Create or append travel by file titled with date of week
 * @param{String} path - path to directory where records summarized by week are stored.
 * @param{Array} week_ary - array of records per that week
 * @return{Promise} Fulfilled when records are returned
 */
const create_or_append = (path, week_ary) => {
  console.log(path, '!!!!')
  let csv = week_ary.reduce((s, d) => {
    s += [d.origin, d.destination, d.count] + '\n';
    return s;
  }, '');

  return new Promise((resolve, reject) => {
    fs.exists(path, exists => {
      if (exists) {
        fs.appendFileSync(path, csv + '\n')
      } else {
        fs.writeFileSync(path, 'orig, dest, cnt' + '\n')
        fs.appendFileSync(path, csv + '\n')
      }
      resolve();
    });
  })
}
