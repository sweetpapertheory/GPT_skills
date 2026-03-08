function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function parseSearchResponseBody(body, base64Encoded = false) {
  const raw = base64Encoded ? Buffer.from(String(body || ''), 'base64').toString('utf8') : String(body || '');
  const candidates = [raw, raw.replace(/^\s*[0-9a-fA-F]+\s+/, '')];

  for (const candidate of candidates) {
    const text = String(candidate || '').trim();
    if (!text) continue;
    try {
      return JSON.parse(text);
    } catch {
      // try next shape
    }

    const objectStart = text.indexOf('{');
    const arrayStart = text.indexOf('[');
    const start =
      objectStart >= 0 && arrayStart >= 0
        ? Math.min(objectStart, arrayStart)
        : Math.max(objectStart, arrayStart);
    if (start < 0) continue;

    const objectEnd = text.lastIndexOf('}');
    const arrayEnd = text.lastIndexOf(']');
    const end = Math.max(objectEnd, arrayEnd);
    if (end <= start) continue;

    try {
      return JSON.parse(text.slice(start, end + 1));
    } catch {
      // try next shape
    }
  }

  return null;
}

function normalizeUrl(url) {
  return String(url || '').split('?')[0].trim();
}

function parseVideoId(url) {
  const match = String(url || '').match(/\/video\/(\d{8,24})/);
  return match ? match[1] : '';
}

function buildVideoUrl(videoId) {
  const clean = String(videoId || '').trim();
  return clean ? `https://www.douyin.com/video/${clean}` : '';
}

function collectAwemeInfos(payload) {
  const out = [];

  if (!payload || typeof payload !== 'object') {
    return out;
  }

  if (Array.isArray(payload.aweme_list)) {
    out.push(...payload.aweme_list.filter((item) => item && typeof item === 'object'));
  }

  if (Array.isArray(payload.data)) {
    for (const item of payload.data) {
      if (!item || typeof item !== 'object') continue;
      if (item.aweme_info && typeof item.aweme_info === 'object') {
        out.push(item.aweme_info);
        continue;
      }
      const mixItems = item.aweme_mix_info?.mix_items;
      if (!Array.isArray(mixItems)) continue;
      for (const mix of mixItems) {
        const aweme = mix?.aweme || mix?.aweme_info || null;
        if (aweme && typeof aweme === 'object') {
          out.push(aweme);
        }
      }
    }
  }

  return out;
}

function extractSearchApiRows({ payload } = {}) {
  const infos = collectAwemeInfos(payload);
  const out = [];
  const seen = new Set();

  for (const aweme of infos) {
    const videoId = String(aweme?.aweme_id || aweme?.group_id || '').trim();
    const videoUrl = normalizeUrl(aweme?.share_url || buildVideoUrl(videoId));
    const key = videoId || videoUrl;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push({
      video_id: videoId,
      video_url: videoUrl,
      desc: String(aweme?.desc || aweme?.title || '').trim(),
      author_name: String(aweme?.author?.nickname || aweme?.author?.unique_id || '').trim(),
      source: 'search_api',
    });
  }

  return out;
}

function extractSearchPagination({ payload, requestUrl } = {}) {
  const url = new URL(String(requestUrl || 'https://www.douyin.com/'));
  const params = url.searchParams;
  return {
    hasMore: Boolean(toNumber(payload?.has_more ?? payload?.hasMore, 0)),
    cursor: toNumber(payload?.cursor, toNumber(params.get('offset'), 0)),
    requestOffset: toNumber(params.get('offset'), 0),
    count: toNumber(params.get('count'), 10),
    searchId: String(
      params.get('search_id') ||
        payload?.extra?.search_request_id ||
        payload?.extra?.logid ||
        payload?.log_pb?.impr_id ||
        ''
    ).trim(),
  };
}

function buildNextSearchPageUrl(requestUrl, pagination) {
  if (!pagination?.hasMore) return '';
  const url = new URL(String(requestUrl || 'https://www.douyin.com/'));
  const nextOffset = toNumber(pagination.cursor, toNumber(url.searchParams.get('offset'), 0) + toNumber(url.searchParams.get('count'), 10));
  url.searchParams.set('offset', String(nextOffset));
  if (pagination.count > 0) {
    url.searchParams.set('count', String(pagination.count));
  }
  if (pagination.searchId) {
    url.searchParams.set('search_id', pagination.searchId);
  }
  if (nextOffset > 0) {
    url.searchParams.set('need_filter_settings', '0');
  }
  return url.toString();
}

function isSearchResultEndpoint(requestUrl) {
  const url = String(requestUrl || '');
  return (
    url.includes('/aweme/v1/web/search/item/') ||
    url.includes('/aweme/v1/web/general/search/stream/') ||
    url.includes('/aweme/v1/web/general/search/single/')
  );
}

function isReplayableSearchEndpoint(requestUrl) {
  const url = String(requestUrl || '');
  return (
    url.includes('/aweme/v1/web/search/item/') ||
    url.includes('/aweme/v1/web/general/search/single/')
  );
}

function mergeSearchRows({ apiRows, domRows } = {}) {
  const out = [];
  const seen = new Set();

  const addRow = (row) => {
    if (!row || typeof row !== 'object') return;
    const videoUrl = normalizeUrl(row.video_url || row.url || '');
    const videoId = String(row.video_id || parseVideoId(videoUrl) || '').trim();
    const key = videoId || videoUrl;
    if (!key || seen.has(key)) return;
    seen.add(key);
    out.push({
      ...row,
      video_id: videoId,
      video_url: videoUrl,
    });
  };

  for (const row of Array.isArray(apiRows) ? apiRows : []) addRow(row);
  for (const row of Array.isArray(domRows) ? domRows : []) addRow(row);
  return out;
}

function mergeSearchCrawlPayloads(payloads) {
  const items = Array.isArray(payloads) ? payloads.filter((item) => item && typeof item === 'object') : [];
  const apiRows = [];
  const domRows = [];
  const searchPageTypes = new Set();
  const searchEndpoints = new Set();
  const apiPageRecords = [];
  const apiReplayFailures = [];
  let keyword = '';
  let extractedAt = '';

  for (const item of items) {
    if (!keyword && item.keyword) keyword = String(item.keyword);
    if (!extractedAt && item.extracted_at) extractedAt = String(item.extracted_at);
    if (item.search_page_type) searchPageTypes.add(String(item.search_page_type));
    for (const endpoint of Array.isArray(item.search_endpoints) ? item.search_endpoints : []) {
      if (endpoint) searchEndpoints.add(String(endpoint));
    }
    if (Array.isArray(item.api_page_records)) apiPageRecords.push(...item.api_page_records);
    if (Array.isArray(item.api_replay_failures)) apiReplayFailures.push(...item.api_replay_failures);
    apiRows.push(...(Array.isArray(item.api_links) ? item.api_links : []));
    domRows.push(...(Array.isArray(item.dom_links) ? item.dom_links : []));
  }

  const mergedApiLinks = mergeSearchRows({ apiRows, domRows: [] });
  const mergedDomLinks = mergeSearchRows({ apiRows: [], domRows });
  const mergedLinks = mergeSearchRows({ apiRows: mergedApiLinks, domRows: mergedDomLinks });

  return {
    keyword,
    extracted_at: extractedAt,
    search_page_types: Array.from(searchPageTypes.values()),
    search_endpoints: Array.from(searchEndpoints.values()),
    api_pages_captured: items.reduce((sum, item) => sum + toNumber(item.api_pages_captured, 0), 0),
    api_pagination_attempts: items.reduce((sum, item) => sum + toNumber(item.api_pagination_attempts, 0), 0),
    api_replay_failures: apiReplayFailures,
    api_page_records: apiPageRecords,
    api_links: mergedApiLinks,
    dom_links: mergedDomLinks,
    links: mergedLinks,
    link_count: mergedLinks.length,
    merged_link_count: mergedLinks.length,
    pagination_mode: mergedApiLinks.length > 0 ? 'search_api_plus_dom' : 'dom_only',
  };
}

function chooseWorkbookOutputPath({ defaultPath, runStamp, existingDiscoveryCount, newDiscoveryCount }) {
  const existing = toNumber(existingDiscoveryCount, -1);
  const current = toNumber(newDiscoveryCount, -1);
  if (existing >= 0 && current >= 0 && current < existing) {
    const stamp = String(runStamp || '').trim();
    const suffix = stamp.includes('_') ? stamp.split('_').slice(1).join('_') : stamp;
    return {
      outputPath: String(defaultPath || '').replace(/\.xlsx$/i, `_${suffix || stamp}.xlsx`),
      overwriteAllowed: false,
      reason: 'new_run_worse_than_existing',
    };
  }

  return {
    outputPath: String(defaultPath || ''),
    overwriteAllowed: true,
    reason: existing >= 0 ? 'new_run_not_worse' : 'no_existing_workbook',
  };
}

function chooseBestSearchAttempt(attempts) {
  let best = null;

  for (const attempt of Array.isArray(attempts) ? attempts : []) {
    if (!attempt || typeof attempt !== 'object') continue;
    const merged = toNumber(attempt.mergedLinkCount, -1);
    if (!best || merged > toNumber(best.mergedLinkCount, -1)) {
      best = {
        ...attempt,
        mergedLinkCount: merged,
      };
    }
  }

  return best;
}

function chooseSearchAttemptWaitSeconds({ manualUnlockWaitSeconds, hiddenBackgroundMode } = {}) {
  const wait = Math.max(toNumber(manualUnlockWaitSeconds, 180), 0);
  if (!hiddenBackgroundMode) {
    return wait;
  }
  return Math.min(wait, 15);
}

module.exports = {
  buildNextSearchPageUrl,
  chooseBestSearchAttempt,
  chooseSearchAttemptWaitSeconds,
  chooseWorkbookOutputPath,
  extractSearchApiRows,
  extractSearchPagination,
  isReplayableSearchEndpoint,
  isSearchResultEndpoint,
  mergeSearchCrawlPayloads,
  mergeSearchRows,
  normalizeUrl,
  parseSearchResponseBody,
  parseVideoId,
};
