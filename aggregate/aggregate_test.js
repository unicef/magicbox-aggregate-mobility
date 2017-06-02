var assert = require('chai').assert;
var aggregate = require('./spark_aggregate');


describe('Aggregate travel activty by country', function() {
    it(
      'should aggregate travels by country',
      (done) => {
        aggregate.aggregate(
          'test.csv',
          '../test/data/',
          '../test/temp/'
        )
        .then(results => {
          console.log(results, '!!!!!');
          // assert.strictEqual(Object.keys(country_codes).length, 1);
          // assert.strictEqual(results[0].count, 28);
        }).then(done);
      });
});
