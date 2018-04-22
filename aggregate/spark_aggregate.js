const util = require('util')
const exec = require('child_process').exec
const spark_path = require('../config').spark_path

// executes `pwd`
exports.aggregate = (file, aggregation_level, path_unzipped, path_temp) => {
  console.log('SPARK CLI!', file)
  return new Promise((resolve, reject) => {
    let command = spark_path +
    'spark-shell -i ./spark/aggregate.scala --conf ' +
    'spark.driver.extraJavaOptions="-D' +
    file + ',' + aggregation_level +
    ',' + path_unzipped + ',' + path_temp +
    '" >> spark.output'

      try {
          exec(
              command, (error, stdout, stderr) => {
                  util.print('stdout: ' + stdout)
                  util.print('stderr: ' + stderr)
                  if (error !== null) {
                      throw error
                  }
                  console.log('DONE!')
                  resolve()
              })
      } catch (error) {
          console.log('exec error: ' + error)
          reject()
      }
  })
}
