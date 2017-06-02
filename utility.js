var config = require('./config');
var fs = require('fs');
var path_processed = config.processed;
var path_temp = config.temp;
var exec = require('child_process').exec;
var bluebird = require('bluebird');
var fields  = config.fields;
var moment = require('moment');
exports.combine_spark_output = dir => {
  var files = fs.readdirSync(path_temp + dir).filter(f => { return f.match(/csv$/) });
  return new Promise(function(resolve, reject) {
    bluebird.each(files, f => {
      console.log('File', f)
        return process_file(f, dir);
    }).catch((err) => { console.log(err, 'eeee');} ).then(() => {console.log('done all files'); resolve()});
  });
};

function process_file(f, dir) {
  return new Promise((resolve, reject) => {
    var records = {};
    var data = fs.readFileSync(path_temp + dir + '/' + f, 'utf8');
    if (!data) {
      return resolve();
    }
    var lines = data.split(/\n/);
    lines.forEach(l => {
      var vals = l.split(/,/).reduce((h, val, index) => {
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
      var year = records[week][0].year;
      var date = moment(year + '-01-01').add(week -1, 'weeks').format('YYYY-MM-DD');
      return create_or_append(config.processed + date + '.csv', records[week])
    }).catch(console.log).then(resolve);
  })
}

function create_or_append(path, week_ary) {
  var csv = week_ary.reduce((s, d) => { s += [d.origin, d.destination, d.count] + '\n'; return s;}, '');
  return new Promise((resolve, reject)  => {
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
