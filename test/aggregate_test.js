// Tests aggregation of airport mobility using spark.
// Tests:
// country to country
// admin 1 (using two data frames)
// Aggregation of airports without admin_id

const combine_spark_output =
require('../combine_spark_output').combine_spark_output
const mock = require('mock-require')
const fs = require('fs')
const path = require('path');
const chai = require('chai');

// Test for aggregate.js
describe('aggregate', () => {
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

  let dateLookup = {'2015': '2015-01-05'}
  let inputFilename = 'unicef_traffic_W_2015_01.csv'
  let outputFilename = '2015-01-05.csv'

  // mock configuration
  mock('../config', config)

  const aggregate = require('../aggregate/spark_aggregate').aggregate

  /**
   * Helper function to compare output with expected data
   * @param{Object} result
   * @param{Object} expected
   */
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

      chai.expect(count).to.equal(expected[origin + ',' + destination], `Count did not match for ${origin} - ${destination}`)
    })
  }

  /**
   * Helper function to cleanup directories
   * @param{String} directory
   */
  const cleanupDirectory = directory => {
    fs.readdirSync(directory).forEach(file => {
      let currentPath = path.resolve(directory) + '/' + file

      if ( fs.statSync(currentPath).isDirectory() ) {
        cleanupDirectory(currentPath)
        fs.rmdirSync(currentPath)
      } else {
        fs.unlinkSync(currentPath)
      }
    })
  }
  // Clean stuff before each test
  beforeEach(() => {
    let processedFilePath = config.processed + outputFilename
    cleanupDirectory(config.temp)
    if (fs.existsSync(processedFilePath)) {
      fs.unlinkSync(processedFilePath)
    }
  })

  // Tests
  it('should output aggregation by admin 0', (done) => {
    aggregate(
      inputFilename, 'admin0', config.unzipped, config.temp
    ).then(() => {
      return combine_spark_output(
        inputFilename, config.temp, config.processed, dateLookup
      ).then(() => {
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
        compareResultWithExpected(
          config.processed + outputFilename, expectedData
        )
        done()
      }).catch(done)
    })
  })

  it('should output aggregation by admin 1 using join select', (done) => {
    aggregate(
      inputFilename, 'admin1', config.unzipped, config.temp
    ).then(() => {
      return combine_spark_output(
        inputFilename, config.temp, config.processed, dateLookup
      ).then(() => {
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

        compareResultWithExpected(
          config.processed + outputFilename, expectedData
        )
        done()
      }).catch(done)
    })
  })

  it('Should output aggregation of airport without admins', (done) => {
    aggregate(
      inputFilename, 'admin3', config.unzipped, config.temp
    ).then(() => {
      return combine_spark_output(
        inputFilename, config.temp, config.processed, dateLookup
      ).then(() => {
        let expectedData = {
          ',esp_215_13_44_328_gadm2-8': '0',
          'bol_28_8_84_291_gadm2-8,': '0',
          'vnm_250_57_622_9818_gadm2-8,vnm_250_45_482_7403_gadm2-8': '16',
          ',': '5'
        }
        compareResultWithExpected(
          config.processed + outputFilename, expectedData
        )
        done()
      }).catch(done)
    })
  })
})
