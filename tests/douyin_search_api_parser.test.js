const test = require('node:test');
const assert = require('node:assert/strict');

const {
  chooseBestSearchAttempt,
  chooseSearchAttemptWaitSeconds,
  extractSearchApiRows,
  extractSearchPagination,
  isReplayableSearchEndpoint,
  isSearchResultEndpoint,
  mergeSearchCrawlPayloads,
  mergeSearchRows,
  parseSearchResponseBody,
} = require('../douyin-keyword-excel-scripting/scripts/douyin_search_api_helpers.js');

const samplePayload = {
  status_code: 0,
  has_more: 1,
  cursor: 10,
  extra: {
    search_request_id: '',
  },
  log_pb: {
    impr_id: '20260309041956D0E74E3881E87A003F08',
  },
  path: '/aweme/v1/web/search/item/',
  data: [
    {
      type: 1,
      aweme_info: {
        aweme_id: '7614686266076803427',
        desc: '十五五转向与能源转型',
        create_time: 1772932306,
        author: {
          nickname: '洞见ALPHA',
        },
      },
    },
    {
      type: 1,
      aweme_info: {
        aweme_id: '7614076626724293915',
        desc: '2026年新能源机会在哪',
        create_time: 1772870000,
        author: {
          nickname: '马哥能源频道',
        },
      },
    },
  ],
};

test('extractSearchApiRows pulls video rows from data[].aweme_info', () => {
  const rows = extractSearchApiRows({
    payload: samplePayload,
    requestUrl:
      'https://www.douyin.com/aweme/v1/web/search/item/?keyword=%E8%83%BD%E6%BA%90%E8%BD%AC%E5%9E%8B&offset=0&count=10',
  });

  assert.equal(rows.length, 2);
  assert.deepEqual(rows[0], {
    video_id: '7614686266076803427',
    video_url: 'https://www.douyin.com/video/7614686266076803427',
    desc: '十五五转向与能源转型',
    author_name: '洞见ALPHA',
    source: 'search_api',
  });
});

test('extractSearchPagination keeps offset and cursor state from the request and payload', () => {
  const page = extractSearchPagination({
    payload: samplePayload,
    requestUrl:
      'https://www.douyin.com/aweme/v1/web/search/item/?keyword=%E8%83%BD%E6%BA%90%E8%BD%AC%E5%9E%8B&offset=10&count=10&search_id=abc123',
  });

  assert.deepEqual(page, {
    hasMore: true,
    cursor: 10,
    requestOffset: 10,
    count: 10,
    searchId: 'abc123',
  });
});

test('mergeSearchRows prefers api data and deduplicates by video_id then url', () => {
  const merged = mergeSearchRows({
    apiRows: [
      {
        video_id: '7614686266076803427',
        video_url: 'https://www.douyin.com/video/7614686266076803427',
        desc: 'api version',
        author_name: 'api author',
        source: 'search_api',
      },
    ],
    domRows: [
      {
        video_id: '',
        video_url: 'https://www.douyin.com/video/7614686266076803427',
        text: 'dom version',
        source: 'dom_fallback',
      },
      {
        video_id: '7614076626724293915',
        video_url: 'https://www.douyin.com/video/7614076626724293915',
        text: 'dom only',
        source: 'dom_fallback',
      },
    ],
  });

  assert.equal(merged.length, 2);
  assert.equal(merged[0].source, 'search_api');
  assert.equal(merged[1].video_id, '7614076626724293915');
});

test('chooseBestSearchAttempt keeps the highest merged-count attempt', () => {
  const best = chooseBestSearchAttempt([
    { liveJson: '/tmp/a.json', urlsJson: '/tmp/a_urls.json', mergedLinkCount: 20 },
    { liveJson: '/tmp/b.json', urlsJson: '/tmp/b_urls.json', mergedLinkCount: 30 },
    { liveJson: '/tmp/c.json', urlsJson: '/tmp/c_urls.json', mergedLinkCount: 28 },
  ]);

  assert.deepEqual(best, {
    liveJson: '/tmp/b.json',
    urlsJson: '/tmp/b_urls.json',
    mergedLinkCount: 30,
  });
});

test('chooseSearchAttemptWaitSeconds caps hidden-background search waits', () => {
  assert.equal(
    chooseSearchAttemptWaitSeconds({ manualUnlockWaitSeconds: 180, hiddenBackgroundMode: true }),
    15
  );
  assert.equal(
    chooseSearchAttemptWaitSeconds({ manualUnlockWaitSeconds: 8, hiddenBackgroundMode: true }),
    8
  );
  assert.equal(
    chooseSearchAttemptWaitSeconds({ manualUnlockWaitSeconds: 180, hiddenBackgroundMode: false }),
    180
  );
});

test('parseSearchResponseBody handles prefixed general-search payloads', () => {
  const payload = parseSearchResponseBody(
    'ae09 {"status_code":0,"has_more":1,"cursor":10,"data":[{"type":1,"aweme_info":{"aweme_id":"7614915579622722330","desc":"气候行动测试","author":{"nickname":"气候1.0"}}}]}'
  );

  assert.deepEqual(payload, {
    status_code: 0,
    has_more: 1,
    cursor: 10,
    data: [
      {
        type: 1,
        aweme_info: {
          aweme_id: '7614915579622722330',
          desc: '气候行动测试',
          author: {
            nickname: '气候1.0',
          },
        },
      },
    ],
  });
});

test('search endpoint helpers include general search results and exclude hot lists', () => {
  assert.equal(
    isSearchResultEndpoint('https://www.douyin.com/aweme/v1/web/search/item/?offset=0'),
    true
  );
  assert.equal(
    isSearchResultEndpoint('https://www.douyin.com/aweme/v1/web/general/search/stream/?offset=0'),
    true
  );
  assert.equal(
    isSearchResultEndpoint('https://www.douyin.com/aweme/v1/web/general/search/single/?offset=10'),
    true
  );
  assert.equal(
    isSearchResultEndpoint('https://www.douyin.com/aweme/v1/web/hot/search/list/?source=6'),
    false
  );

  assert.equal(
    isReplayableSearchEndpoint('https://www.douyin.com/aweme/v1/web/general/search/stream/?offset=0'),
    false
  );
  assert.equal(
    isReplayableSearchEndpoint('https://www.douyin.com/aweme/v1/web/general/search/single/?offset=10'),
    true
  );
});

test('mergeSearchCrawlPayloads unions general and video discovery sets', () => {
  const merged = mergeSearchCrawlPayloads([
    {
      keyword: '气候行动',
      search_page_type: 'general',
      search_endpoints: ['/aweme/v1/web/general/search/single/'],
      api_links: [
        { video_id: '1', video_url: 'https://www.douyin.com/video/1', source: 'search_api' },
        { video_id: '2', video_url: 'https://www.douyin.com/video/2', source: 'search_api' },
      ],
      dom_links: [],
      links: [
        { video_id: '1', video_url: 'https://www.douyin.com/video/1', source: 'search_api' },
        { video_id: '2', video_url: 'https://www.douyin.com/video/2', source: 'search_api' },
      ],
      api_pages_captured: 2,
      api_pagination_attempts: 1,
      api_page_records: [{ url: 'general-1' }],
      api_replay_failures: [],
    },
    {
      keyword: '气候行动',
      search_page_type: 'video',
      search_endpoints: ['/aweme/v1/web/search/item/'],
      api_links: [
        { video_id: '2', video_url: 'https://www.douyin.com/video/2', source: 'search_api' },
        { video_id: '3', video_url: 'https://www.douyin.com/video/3', source: 'search_api' },
      ],
      dom_links: [],
      links: [
        { video_id: '2', video_url: 'https://www.douyin.com/video/2', source: 'search_api' },
        { video_id: '3', video_url: 'https://www.douyin.com/video/3', source: 'search_api' },
      ],
      api_pages_captured: 3,
      api_pagination_attempts: 2,
      api_page_records: [{ url: 'video-1' }],
      api_replay_failures: [{ url: 'video-fail' }],
    },
  ]);

  assert.equal(merged.merged_link_count, 3);
  assert.deepEqual(merged.search_page_types, ['general', 'video']);
  assert.deepEqual(merged.search_endpoints, [
    '/aweme/v1/web/general/search/single/',
    '/aweme/v1/web/search/item/',
  ]);
  assert.equal(merged.api_pages_captured, 5);
  assert.equal(merged.api_pagination_attempts, 3);
  assert.equal(merged.api_replay_failures.length, 1);
});
