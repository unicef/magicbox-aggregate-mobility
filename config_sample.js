module.exports = {
  zipped: '../../sandboxdata/mnt/amadeus/raw/traffic/',
  unzipped: '../../sandboxdata/mnt/traffic/unzipped/',
  processed: '../../sandboxdata/mnt/traffic/processed/',
  aggregated: '../../sandboxdata/aggregations/mobility/amadeus/traffic/country/',
  temp: '../../sandboxdata/aggregations/mobility/amadeus/traffic/country/',
  spark_path: '',
  aggregation_level: 'admin0',
  fields: ['year', 'week', 'count', 'origin', 'destination']
};
