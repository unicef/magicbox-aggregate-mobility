var sys = require('sys')
var exec = require('child_process').exec;
var aggregate = require('../aggregate/spark_aggregate');
var child;

// executes `pwd`
exports.aggregate = function(file, path_unzipped, path_processed) {

  console.log('SPARK CLI!', file);
  return new Promise(function(resolve, reject) {
    var command = 'spark-shell -i ./spark/aggregate.scala --conf spark.driver.extraJavaOptions="-D' + file + ',' + path_unzipped + ',' + path_processed +  '"'

    child = exec(
      command, function (error, stdout, stderr) {
        sys.print('stdout: ' + stdout);
        sys.print('stderr: ' + stderr);
        if (error !== null) {
          console.log('exec error: ' + error);
        }
        console.log('DONE!');
        resolve();
      });
  });
};
