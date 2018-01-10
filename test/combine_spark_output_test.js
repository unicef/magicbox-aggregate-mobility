const util = require('../combine_spark_output')
const fs = require('fs')
const path = require('path')
const expect = require('chai').expect

// Test for combine_spark_output.js
describe('combine_spark_output', function() {
  let sparkOutputDir = 'spark_output_sample'
  let tempDir = './test/data/'
  let processedDataDir = './test/data/processed/'
  let dateLookup = { '2015': '2015-01-05' }

  // checking if any file is present in ./test/data/processed. If yes we need to delete it otherwise
  // we will get multiple similar entries.
  before(function() {
    if (fs.existsSync(processedDataDir)) {
      fs.readdirSync(processedDataDir).forEach(file => fs.unlinkSync(processedDataDir + file) )
    }
  })

  it ('should combine all spark output files into one', function(done) {
    // calling combine_spark_output method on test data
    util.combine_spark_output(sparkOutputDir, tempDir, processedDataDir, dateLookup).then((data) => {
      // get all csv files inside processedDataDir 
      let csvFiles = fs.readdirSync(processedDataDir).filter(file => (path.extname(file) === '.csv'))

      // checking that we have csv file generated after combining spark outout
      expect(csvFiles.length).to.be.above(0, `File not found in ${processedDataDir}`)

      let expectedData = getExpectedData()

      // reading csv file generated
      let csvData = fs.readFileSync(processedDataDir + csvFiles[0], 'utf8')

      // checking each entry in csv file and verifying it with our expected results.
      csvData.split(/\n/).forEach((line, index) => {
        let [origin, destination, count] = line.split(/,/)

        // ignore header
        if (index == 0) {
          return
        }

        expect(count).to.equal(expectedData.get(origin + destination), `Count did not match for ${origin} - ${destination}`)
      })
      done()
    }).catch((err) => done(err))
  })
})

/**
 * Makes and returns a Map holding expected result.
 * Key is a combination of origin and destination (e.g. 'USIN' when origin is USA and destination is India )
 * value is the count of travellers.
 * @return {Map} expectedData holds expected result of the test
 */
function getExpectedData() {
  let expectedData = new Map()
  expectedData.set('IQTR' , '2')
  expectedData.set('VNVN' , '16')
  expectedData.set('MXES' , '0')
  expectedData.set('BODK' , '0')
  expectedData.set('TRUA' , '2')
  expectedData.set('USPR' , '1')
  expectedData.set('USUA' , '0')
  expectedData.set('USUS' , '0')
  return expectedData
}
