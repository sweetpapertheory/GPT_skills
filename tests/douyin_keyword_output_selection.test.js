const test = require('node:test');
const assert = require('node:assert/strict');

const {
  chooseWorkbookOutputPath,
} = require('../douyin-keyword-excel-scripting/scripts/douyin_search_api_helpers.js');

test('chooseWorkbookOutputPath keeps the default daily path when the new run is not worse', () => {
  const choice = chooseWorkbookOutputPath({
    defaultPath:
      '/Users/XM/Desktop/Data/DY data/01_collection/douyin_topic_batches/douyin_energy_transition_20260308.xlsx',
    runStamp: '20260308_201500',
    existingDiscoveryCount: 28,
    newDiscoveryCount: 30,
  });

  assert.deepEqual(choice, {
    outputPath:
      '/Users/XM/Desktop/Data/DY data/01_collection/douyin_topic_batches/douyin_energy_transition_20260308.xlsx',
    overwriteAllowed: true,
    reason: 'new_run_not_worse',
  });
});

test('chooseWorkbookOutputPath switches to a timestamped file when the new run is worse', () => {
  const choice = chooseWorkbookOutputPath({
    defaultPath:
      '/Users/XM/Desktop/Data/DY data/01_collection/douyin_topic_batches/douyin_energy_transition_20260308.xlsx',
    runStamp: '20260308_201500',
    existingDiscoveryCount: 28,
    newDiscoveryCount: 20,
  });

  assert.deepEqual(choice, {
    outputPath:
      '/Users/XM/Desktop/Data/DY data/01_collection/douyin_topic_batches/douyin_energy_transition_20260308_201500.xlsx',
    overwriteAllowed: false,
    reason: 'new_run_worse_than_existing',
  });
});
