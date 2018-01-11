var exec = require('child_process').exec;
var aggregate = require('../aggregate/spark_aggregate');
var child;
var spark_path = require('../config').spark_path;

// executes `pwd`
exports.aggregate = function(file, aggregation_level, path_unzipped, path_temp) {

  console.log('SPARK CLI!', file);
  return new Promise(function(resolve, reject) {
    var command = spark_path + 'spark-shell -i ./spark/aggregate.scala --conf spark.driver.extraJavaOptions="-D' + file + ',' + aggregation_level +  ',' + path_unzipped + ',' + path_temp +  '"'

    child = exec(
      command, function (error, stdout, stderr) {
        console.log('stdout: ' + stdout);
        console.log('stderr: ' + stderr);
        if (error !== null) {
          console.log('exec error: ' + error);
        }
        console.log('DONE!');
        resolve();
      });
  });
};
