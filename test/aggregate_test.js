const combine_spark_output = require('../combine_spark_output').combine_spark_output
const mock = require('mock-require')
const fs = require('fs')
const path = require('path');
const expect = require('chai').expect;

// Test for aggregate.js
describe('aggregate', function() {
  // Custom configuration provided for the test
  let config = {
    zipped: './test/data/zipped/',
    unzipped: './test/data/unzipped/',
    processed: './test/data/processed/',
    temp: './test/data/temp/',
    spark_path: '',
    aggregation_level: 'admin0',
    fields: ['year', 'week', 'count', 'origin', 'destination']
  }

  let dateLookup = { '2015': '2015-01-05' }
  let inputFilename = 'unicef_traffic_W_2015_01.csv'
  let outputFilename = '2015-01-05.csv'


  // mock configuration
  mock('../config', config)

  const aggregate = require('../aggregate/spark_aggregate').aggregate

  // Helper function to compare output with expected data
  const compareResultWithExpected = (result, expected) => {
    // reading csv file generated
    let csvData = fs.readFileSync(config.processed + outputFilename, 'utf8')

    // checking each entry in csv file and verifying it with our expected results.
    csvData.split(/\n/).forEach((line, index) => {
      let [origin, destination, count] = line.split(/,/)

      // ignore header
      if (index == 0) {
        return
      }

      expect(count).to.equal(expected[origin + ',' + destination], `Count did not match for ${origin} - ${destination}`)
    })
  }

  // Helper function to cleanup directories
  const cleanupDirectory = (directory) => fs.readdirSync(directory).forEach((file) => {
    let currentPath = path.resolve(directory) + '/' + file

    if ( fs.statSync(currentPath).isDirectory() ) {
      cleanupDirectory(currentPath)
      fs.rmdirSync(currentPath)
    } else {
      fs.unlinkSync(currentPath)
    }
  })

  // Clean stuff before each test
  beforeEach(function () {
    let processedFilePath = config.processed + outputFilename

    cleanupDirectory(config.temp)

    if (fs.existsSync(processedFilePath)) {
      fs.unlinkSync(processedFilePath)
    }
  })

  // Tests

  it('should output aggregation by admin 0', function (done) {
    aggregate(inputFilename, 'admin0', config.unzipped, config.temp).then(function () {
      return combine_spark_output(inputFilename, config.temp, config.processed, dateLookup).then(function () {
        let expectedData = {
          'IQ,TR': '2',
          'US,US': '0',
          'BO,DK': '0',
          'VN,VN': '16',
          'US,UA': '0',
          'US,PR': '1',
          'MX,ES': '0',
          'TR,UA': '2'
        }

        compareResultWithExpected(config.processed + outputFilename, expectedData)
        done()
      }).catch(done)
    })
  })

  it('should output aggregation by admin 1', function (done) {
    aggregate(inputFilename, 'admin1', config.unzipped, config.temp).then(function () {
      return combine_spark_output(inputFilename, config.temp, config.processed, dateLookup).then(function () {
        let expectedData = {
          'mex_145_28_gadm2-8,esp_215_13_gadm2-8': '0',
          'usa_244_5_gadm2-8,ukr_240_12_gadm2-8': '0',
          'tur_235_55_gadm2-8,ukr_240_26_gadm2-8': '2',
          'usa_244_22_gadm2-8,pri_183_4_gadm2-8': '1',
          'irq_108_10_gadm2-8,tur_235_11_gadm2-8': '2',
          'vnm_250_57_gadm2-8,vnm_250_45_gadm2-8': '16',
          'bol_28_8_gadm2-8,dnk_64_1_gadm2-8': '0',
          'usa_244_35_gadm2-8,usa_244_6_gadm2-8': '0'
        }

        compareResultWithExpected(config.processed + outputFilename, expectedData)
        done()
      }).catch(done)
    })
  })

  it('should output aggregation by admin 2', function (done) {
    aggregate(inputFilename, 'admin2', config.unzipped, config.temp).then(function () {
      return combine_spark_output(inputFilename, config.temp, config.processed, dateLookup).then(function () {
        let expectedData = {
          'vnm_250_57_622_gadm2-8,vnm_250_45_482_gadm2-8': '16',
          'irq_108_10_55_gadm2-8,tur_235_11_131_gadm2-8': '2',
          'mex_145_28_1424_gadm2-8,esp_215_13_44_gadm2-8': '0',
          'usa_244_35_2040_gadm2-8,usa_244_6_291_gadm2-8': '0',
          'tur_235_55_650_gadm2-8,ukr_240_26_600_gadm2-8': '2',
          'bol_28_8_84_gadm2-8,dnk_64_1_29_gadm2-8': '0',
          'usa_244_5_195_gadm2-8,ukr_240_12_269_gadm2-8': '0',
          'usa_244_22_1229_gadm2-8,': '1'
        }

        compareResultWithExpected(config.processed + outputFilename, expectedData)
        done()
      }).catch(done)
    })
  })

  it('should output aggregation by admin 3', function (done) {
    aggregate(inputFilename, 'admin3', config.unzipped, config.temp).then(function () {
      return combine_spark_output(inputFilename, config.temp, config.processed, dateLookup).then(function () {
        let expectedData = {
          ',esp_215_13_44_328_gadm2-8': '0',
          'bol_28_8_84_291_gadm2-8,': '0',
          'vnm_250_57_622_9818_gadm2-8,vnm_250_45_482_7403_gadm2-8': '16',
          ',': '5'
        }

        compareResultWithExpected(config.processed + outputFilename, expectedData)
        done()
      }).catch(done)
    })
  })

})

