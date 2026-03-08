#!/usr/bin/env node

const fs = require('fs');
const {
  buildNextSearchPageUrl,
  extractSearchApiRows,
  extractSearchPagination,
  isReplayableSearchEndpoint,
  isSearchResultEndpoint,
  mergeSearchRows,
  parseSearchResponseBody,
  parseVideoId,
} = require('./douyin_search_api_helpers.js');
const {
  chooseScrollTarget,
  summarizeScrollProgress,
} = require('./crawl_douyin_live_keyword_scroll_helpers.js');

const keyword = process.argv[2] || '碳排放';
const outJson = process.argv[3] || '/tmp/douyin_live_keyword.json';
const port = Number(process.argv[4] || 9222);
const maxRounds = Number(process.argv[5] || 500);
const stallLimit = Number(process.argv[6] || 40);
const initialWaitMs = Number(process.argv[7] || 11000);
const scrollWaitMs = Number(process.argv[8] || 1500);
const manualUnlockWaitSeconds = Number(process.argv[9] || process.env.DOUYIN_MANUAL_UNLOCK_WAIT_SECONDS || 180);
const searchPageType = process.argv[10] || process.env.DOUYIN_SEARCH_PAGE_TYPE || 'general';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function isVerificationGate(probe) {
  const merged = `${String(probe?.title || '')} ${String(probe?.body_sample || '')}`;
  return /验证码|验证中间页|请完成验证|安全验证|captcha|robot/i.test(merged);
}

function isLoginOrBlockedShell(probe) {
  if (!probe || typeof probe !== 'object') return false;
  const title = String(probe.title || '');
  const body = String(probe.body_sample || '');
  const videoAnchors = Number(probe.video_anchor_count || 0);
  const hasContentHints = /@\S+|#\S+|\b\d{1,2}:\d{2}\b|合集|直播中|分钟前|小时前|天前|周前|月前|年前/.test(body);
  if (videoAnchors > 0 || hasContentHints) return false;
  return /抖音搜索/.test(title) && /登录/.test(body) && /综合/.test(body) && /筛选/.test(body);
}

async function main() {
  const version = await fetch(`http://127.0.0.1:${port}/json/version`).then((r) => r.json());
  if (!version.webSocketDebuggerUrl) {
    throw new Error('Chrome DevTools websocket url missing. Is Chrome running with --remote-debugging-port?');
  }

  const ws = new WebSocket(version.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    ws.addEventListener('open', () => resolve(), { once: true });
    ws.addEventListener('error', reject, { once: true });
  });

  let id = 0;
  const pending = new Map();
  let activeSearchSessionId = '';
  const searchResponseQueue = [];
  const seenSearchRequestIds = new Set();

  ws.addEventListener('message', (event) => {
    const msg = JSON.parse(event.data);
    if (msg.id && pending.has(msg.id)) {
      const { resolve, reject, timer } = pending.get(msg.id);
      pending.delete(msg.id);
      clearTimeout(timer);

      if (msg.error) reject(new Error(msg.error.message || JSON.stringify(msg.error)));
      else resolve(msg);
      return;
    }

    if (
      activeSearchSessionId &&
      msg.sessionId === activeSearchSessionId &&
      msg.method === 'Network.responseReceived'
    ) {
      const params = msg.params || {};
      const requestId = String(params.requestId || '').trim();
      const url = String(params.response?.url || '');
      if (!requestId || !isSearchResultEndpoint(url)) return;
      if (seenSearchRequestIds.has(requestId)) return;
      seenSearchRequestIds.add(requestId);
      searchResponseQueue.push({
        requestId,
        url,
        status: Number(params.response?.status || 0),
      });
    }
  });

  function send(method, params = {}, sessionId) {
    const msgId = ++id;
    const payload = { id: msgId, method, params };
    if (sessionId) payload.sessionId = sessionId;
    ws.send(JSON.stringify(payload));

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (!pending.has(msgId)) return;
        pending.delete(msgId);
        reject(new Error(`Timeout for ${method}`));
      }, 70000);
      pending.set(msgId, { resolve, reject, timer });
    });
  }

  async function getSearchProbe(sid) {
    const snap = await send(
      'Runtime.evaluate',
      {
        expression: `(() => {
          const allAnchors = Array.from(document.querySelectorAll('a[href]'));
          const videoAnchors = allAnchors.filter((a) => String(a.getAttribute('href') || '').includes('/video/'));
          const body = (document.body?.innerText || '').replace(/\\s+/g, ' ').trim();
          return {
            title: document.title || '',
            url: location.href || '',
            anchor_count: allAnchors.length,
            video_anchor_count: videoAnchors.length,
            body_sample: body.slice(0, 800),
          };
        })()`,
        returnByValue: true,
        awaitPromise: true,
      },
      sid
    );
    return snap?.result?.result?.value || {};
  }

  async function waitForSearchOpen(sid, waitSeconds) {
    let probe = await getSearchProbe(sid);
    if (!isVerificationGate(probe) && !isLoginOrBlockedShell(probe)) {
      return probe;
    }

    if (waitSeconds <= 0) {
      return probe;
    }

    const gateKind = isVerificationGate(probe) ? 'verification' : 'login_shell';
    console.log(`search_gate_detected kind=${gateKind} title="${probe.title || ''}" wait_seconds=${waitSeconds}`);
    const deadline = Date.now() + waitSeconds * 1000;
    let poll = 0;
    while (Date.now() < deadline) {
      await sleep(5000);
      poll += 1;
      probe = await getSearchProbe(sid);
      const left = Math.max(Math.ceil((deadline - Date.now()) / 1000), 0);
      const nowKind = isVerificationGate(probe)
        ? 'verification'
        : isLoginOrBlockedShell(probe)
          ? 'login_shell'
          : 'open';
      console.log(
        `search_gate_wait poll=${poll} kind=${nowKind} title="${probe.title || ''}" video_anchor_count=${Number(
          probe.video_anchor_count || 0
        )} seconds_left=${left}`
      );
      if (!isVerificationGate(probe) && !isLoginOrBlockedShell(probe)) {
        console.log(`search_gate_cleared title="${probe.title || ''}"`);
        return probe;
      }
    }
    return probe;
  }

  const created = await send('Target.createTarget', { url: 'about:blank' });
  const targetId = created.result.targetId;
  const attached = await send('Target.attachToTarget', {
    targetId,
    flatten: true,
  });
  const sid = attached.result.sessionId;
  activeSearchSessionId = sid;

  const searchUrl = `https://www.douyin.com/search/${encodeURIComponent(keyword)}?type=${encodeURIComponent(searchPageType)}`;
  const finish = async () => {
    try {
      await send('Target.closeTarget', { targetId });
    } catch {
      // ignore cleanup failures
    }
    ws.close();
  };

  try {
    await send('Page.enable', {}, sid);
    await send('Runtime.enable', {}, sid);
    await send('Network.enable', {}, sid);
    await send('Page.navigate', { url: searchUrl }, sid);
    await sleep(initialWaitMs);

    let searchProbe = await waitForSearchOpen(sid, manualUnlockWaitSeconds);

    const domItems = new Map();
    const apiItems = new Map();
    const apiPageRecords = [];
    const apiReplayFailures = [];
    const searchEndpoints = new Set();
    const replayedSearchUrls = new Set();
    let latestSearchPage = null;
    let stall = 0;
    let selectedScrollTarget = chooseScrollTarget();
    let lastProgress = null;
    let stopReason = `max_rounds=${maxRounds}`;
    let lastMergedCount = 0;

    const domItemToRow = (item) => ({
      video_id: parseVideoId(item?.url || ''),
      video_url: String(item?.url || '').trim(),
      text: String(item?.text || '').trim(),
      author_name: String(item?.author || '').trim(),
      hashtags: String(item?.hashtags || '').trim(),
      metric_text: String(item?.metric_text || '').trim(),
      time_text: String(item?.time_text || '').trim(),
      source: 'dom_fallback',
    });

    const addApiRows = (rows) => {
      let added = 0;
      for (const row of Array.isArray(rows) ? rows : []) {
        const videoId = String(row?.video_id || '').trim();
        const videoUrl = String(row?.video_url || '').trim();
        const key = videoId || videoUrl;
        if (!key || apiItems.has(key)) continue;
        apiItems.set(key, row);
        added += 1;
      }
      return added;
    };

    const recordSearchPage = ({ source, requestUrl, payload, status }) => {
      const rows = extractSearchApiRows({ payload, requestUrl });
      const pagination = extractSearchPagination({ payload, requestUrl });
      const addedCount = addApiRows(rows);
      let endpointPath = '';
      try {
        endpointPath = new URL(requestUrl).pathname;
      } catch {
        endpointPath = '';
      }
      if (endpointPath) searchEndpoints.add(endpointPath);
      apiPageRecords.push({
        source,
        url: requestUrl,
        status,
        row_count: rows.length,
        added_count: addedCount,
        has_more: pagination.hasMore,
        cursor: pagination.cursor,
        request_offset: pagination.requestOffset,
        count: pagination.count,
        search_id: pagination.searchId,
      });
      if (
        isReplayableSearchEndpoint(requestUrl) &&
        (!latestSearchPage || pagination.requestOffset >= latestSearchPage.pagination.requestOffset)
      ) {
        latestSearchPage = { url: requestUrl, pagination };
      }
      return addedCount;
    };

    const drainSearchResponseQueue = async () => {
      let added = 0;
      while (searchResponseQueue.length > 0) {
        const entry = searchResponseQueue.shift();
        let payload = null;
        try {
          const bodyRes = await send('Network.getResponseBody', { requestId: entry.requestId }, sid);
          payload = parseSearchResponseBody(bodyRes?.result?.body || '', !!bodyRes?.result?.base64Encoded);
        } catch {
          payload = null;
        }
        if (!payload || typeof payload !== 'object') continue;
        added += recordSearchPage({
          source: 'passive',
          requestUrl: entry.url,
          payload,
          status: entry.status,
        });
      }
      return added;
    };

    const fetchSearchPageInBrowser = async (requestUrl) => {
      const response = await send(
        'Runtime.evaluate',
        {
          expression: `(() => fetch(${JSON.stringify(requestUrl)}, { credentials: 'include' })
            .then(async (r) => ({ status: r.status, body: await r.text() })))()`,
          returnByValue: true,
          awaitPromise: true,
        },
        sid
      );
      const value = response?.result?.result?.value || {};
      return {
        status: Number(value.status || 0),
        payload: parseSearchResponseBody(value.body || '', false),
      };
    };

    const replaySearchApiPages = async () => {
      let added = 0;
      let rounds = 0;
      while (latestSearchPage?.pagination?.hasMore && rounds < 20) {
        const nextUrl = buildNextSearchPageUrl(latestSearchPage.url, latestSearchPage.pagination);
        if (!nextUrl || replayedSearchUrls.has(nextUrl)) break;
        replayedSearchUrls.add(nextUrl);
        try {
          const replay = await fetchSearchPageInBrowser(nextUrl);
          if (replay.status !== 200 || !replay.payload || typeof replay.payload !== 'object') {
            apiReplayFailures.push({
              url: nextUrl,
              status: replay.status,
              error: 'non_200_or_non_json',
            });
            break;
          }
          added += recordSearchPage({
            source: 'replay',
            requestUrl: nextUrl,
            payload: replay.payload,
            status: replay.status,
          });
          rounds += 1;
          if (!latestSearchPage?.pagination?.hasMore) break;
        } catch (err) {
          apiReplayFailures.push({
            url: nextUrl,
            status: 0,
            error: String(err?.message || err || 'unknown error'),
          });
          break;
        }
      }
      return added;
    };

    for (let i = 0; i < maxRounds; i++) {
      const snap = await send(
        'Runtime.evaluate',
        {
          expression: `(() => {
            const toAbs = (h) => {
              try { return new URL(h, location.origin).href; } catch { return h || ''; }
            };
            const docHeight = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight || 0);
            document.querySelectorAll('[data-codex-scroll-candidate]').forEach((el) => {
              el.removeAttribute('data-codex-scroll-candidate');
            });
            const cards = Array.from(document.querySelectorAll('a[href*="/video/"]'));
            const items = [];
            const candidates = [{
              key: 'window',
              label: 'window',
              kind: 'window',
              anchorCount: cards.length,
              scrollHeight: docHeight,
              clientHeight: window.innerHeight || 0,
              scrollTop: window.scrollY || 0,
              rectHeight: window.innerHeight || 0,
              rectWidth: window.innerWidth || 0,
              overflowY: getComputedStyle(document.documentElement).overflowY || ''
            }];

            for (const a of cards) {
              const href = a.getAttribute('href') || '';
              const abs = toAbs(href).split('?')[0];
              if (!abs.includes('/video/')) continue;

              const card = a.closest('li,article,section,div') || a;
              const txt = ((card && card.innerText) ? card.innerText : (a.innerText || '')).replace(/\\s+/g, ' ').trim();

              const author = (txt.match(/@([^\\s]+)/) || [,''])[1];
              const tags = (txt.match(/#[^#\\s@]+/g) || []).slice(0, 25).join(' ');
              const metric = (txt.match(/(\\d+(?:\\.\\d+)?[万亿]?)/) || [,''])[1];
              const timeText = (txt.match(/(\\d+年前|\\d+月前|\\d+周前|\\d+天前|\\d+小时前|\\d+分钟前|昨天|前天)/) || [,''])[1];

              items.push({
                url: abs,
                text: txt.slice(0, 900),
                author,
                hashtags: tags,
                metric_text: metric,
                time_text: timeText
              });
            }

            const nodes = Array.from(document.querySelectorAll('div,section,main,article,aside,ul,ol'));
            for (const el of nodes) {
              if (!el || !el.isConnected) continue;
              const cs = getComputedStyle(el);
              const scrollHeight = el.scrollHeight || 0;
              const clientHeight = el.clientHeight || 0;
              const rect = el.getBoundingClientRect();
              if (rect.height < 220 || rect.width < 260) continue;
              if (scrollHeight <= clientHeight + 120 && !/(auto|scroll|overlay)/i.test(cs.overflowY || '')) continue;

              const anchorCount = el.querySelectorAll('a[href*="/video/"]').length;
              if (anchorCount <= 0) continue;

              const labelParts = [];
              let cursor = el;
              let depth = 0;
              while (cursor && cursor.nodeType === 1 && depth < 5) {
                let part = cursor.tagName.toLowerCase();
                if (cursor.id) {
                  part += '#' + String(cursor.id).slice(0, 40);
                  labelParts.unshift(part);
                  break;
                }
                const classNames = String(cursor.className || '')
                  .trim()
                  .split(/\\s+/)
                  .filter(Boolean)
                  .slice(0, 2);
                if (classNames.length > 0) {
                  part += '.' + classNames.join('.');
                }
                labelParts.unshift(part);
                cursor = cursor.parentElement;
                depth += 1;
              }

              const key = 'candidate-' + candidates.length;
              el.setAttribute('data-codex-scroll-candidate', key);
              candidates.push({
                key,
                label: labelParts.join(' > ').slice(0, 200) || (el.tagName.toLowerCase() + '[' + candidates.length + ']'),
                kind: 'element',
                anchorCount,
                scrollHeight,
                clientHeight,
                scrollTop: el.scrollTop || 0,
                rectHeight: Math.round(rect.height),
                rectWidth: Math.round(rect.width),
                overflowY: cs.overflowY || ''
              });
            }

            return {
              title: document.title || '',
              url: location.href || '',
              body_sample: (document.body?.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 700),
              items,
              anchor_count: document.querySelectorAll('a[href]').length,
              y: window.scrollY,
              h: docHeight,
              scroll_candidates: candidates.slice(0, 40)
            };
          })()`,
          returnByValue: true,
          awaitPromise: true,
        },
        sid
      );

      const val = snap.result.result.value || {};
      let apiAdded = 0;
      apiAdded += await drainSearchResponseQueue();
      apiAdded += await replaySearchApiPages();
      apiAdded += await drainSearchResponseQueue();
      const scrollTarget = chooseScrollTarget({ candidates: val.scroll_candidates });
      selectedScrollTarget = scrollTarget;
      searchProbe = {
        title: val.title || searchProbe.title || '',
        url: val.url || searchProbe.url || searchUrl,
        body_sample: val.body_sample || searchProbe.body_sample || '',
        anchor_count: Number(val.anchor_count || searchProbe.anchor_count || 0),
        video_anchor_count: Array.isArray(val.items) ? val.items.length : Number(searchProbe.video_anchor_count || 0),
        scroll_target: {
          key: scrollTarget.key,
          kind: scrollTarget.kind,
          anchor_count: scrollTarget.anchorCount,
          scroll_range: scrollTarget.scrollRange,
        },
      };

      const items = Array.isArray(val.items) ? val.items : [];
      let domAdded = 0;
      for (const item of items) {
        if (!item || !item.url) continue;
        if (!domItems.has(item.url)) {
          domItems.set(item.url, item);
          domAdded += 1;
        }
      }

      const mergedLinks = mergeSearchRows({
        apiRows: Array.from(apiItems.values()),
        domRows: Array.from(domItems.values()).map(domItemToRow),
      });
      const mergedCount = mergedLinks.length;
      const mergedAdded = Math.max(mergedCount - lastMergedCount, 0);
      lastMergedCount = mergedCount;

      console.log(
        `round=${i + 1} found_now=${items.length} dom_total=${domItems.size} dom_added=${domAdded} api_total=${apiItems.size} api_added=${apiAdded} merged_total=${mergedCount} merged_added=${mergedAdded} target=${scrollTarget.key} target_kind=${scrollTarget.kind} target_top=${scrollTarget.scrollTop} target_range=${scrollTarget.scrollRange} y=${val.y || 0} h=${val.h || 0}`
      );

      if (mergedAdded === 0) stall += 1;
      else stall = 0;

      const beforeProgress = {
        scrollTop: scrollTarget.scrollTop,
        scrollHeight: scrollTarget.scrollHeight,
        clientHeight: scrollTarget.clientHeight,
        windowY: val.y || 0,
      };
      const scrollRes = await send(
        'Runtime.evaluate',
        {
          expression: `(() => {
            const key = ${JSON.stringify(scrollTarget.key)};
            const step = Math.max(Math.floor(window.innerHeight * 0.98), 280);
            if (key && key !== 'window') {
              const el = document.querySelector('[data-codex-scroll-candidate="' + key + '"]');
              if (el) {
                const beforeTop = el.scrollTop || 0;
                const scrollHeight = el.scrollHeight || 0;
                const clientHeight = el.clientHeight || 0;
                const maxTop = Math.max(scrollHeight - clientHeight, 0);
                const nextTop = Math.min(beforeTop + step, maxTop + window.innerHeight);
                if (typeof el.scrollTo === 'function') el.scrollTo(0, nextTop);
                else el.scrollTop = nextTop;
                const afterTop = el.scrollTop || 0;
                if (Math.abs(afterTop - beforeTop) >= 1 || scrollHeight <= clientHeight + 1) {
                  return {
                    key,
                    kind: 'element',
                    scrollTop: afterTop,
                    scrollHeight,
                    clientHeight,
                    windowY: window.scrollY || 0
                  };
                }
              }
            }
            const h = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight || 0, window.innerHeight);
            window.scrollBy(0, step);
            if (window.scrollY + window.innerHeight + 32 >= h) {
              window.scrollTo(0, h + window.innerHeight);
            }
            return {
              key: 'window',
              kind: 'window',
              scrollTop: window.scrollY || 0,
              scrollHeight: h,
              clientHeight: window.innerHeight || 0,
              windowY: window.scrollY || 0
            };
          })()`,
          returnByValue: true,
        },
        sid
      );
      const afterProgress = scrollRes?.result?.result?.value || {};
      const progressTarget =
        afterProgress.kind && afterProgress.kind !== scrollTarget.kind
          ? afterProgress
          : scrollTarget;
      lastProgress = summarizeScrollProgress({
        target: progressTarget,
        before: beforeProgress,
        after: afterProgress,
      });
      console.log(
        `scroll_progress target=${lastProgress.key} kind=${lastProgress.kind} moved=${lastProgress.moved ? 1 : 0} delta=${lastProgress.delta} remaining=${lastProgress.remaining} at_end=${lastProgress.atEnd ? 1 : 0}`
      );

      await sleep(scrollWaitMs);

      const apiHasMore = Boolean(latestSearchPage?.pagination?.hasMore);
      if (mergedCount === 0 && i >= 5 && lastProgress.atEnd && !apiHasMore) {
        stopReason = `search_empty_at_end stall=${stall} round=${i + 1}`;
        console.log(`stop=${stopReason}`);
        break;
      }
      const effectiveStallLimit = latestSearchPage ? Math.min(stallLimit, 5) : stallLimit;
      if (stall >= effectiveStallLimit && i >= 5 && lastProgress.atEnd && !apiHasMore) {
        stopReason = `search_exhausted_at_end stall=${stall} round=${i + 1}`;
        console.log(`stop=${stopReason}`);
        break;
      }
    }

    const domLinks = Array.from(domItems.values()).map(domItemToRow);
    const apiLinks = Array.from(apiItems.values());
    const links = mergeSearchRows({ apiRows: apiLinks, domRows: domLinks });
    const out = {
      keyword,
      extracted_at: new Date().toISOString(),
      search_url: searchUrl,
      search_page_type: searchPageType,
      search_probe: searchProbe,
      stop_rule: `${stopReason};stall_limit=${stallLimit}`,
      pagination_mode: apiLinks.length > 0 ? 'search_api_plus_dom' : 'dom_only',
      selected_scroll_target: {
        key: selectedScrollTarget.key,
        kind: selectedScrollTarget.kind,
        anchor_count: selectedScrollTarget.anchorCount,
        scroll_range: selectedScrollTarget.scrollRange,
      },
      last_scroll_progress: lastProgress,
      search_endpoints: Array.from(searchEndpoints.values()),
      api_pages_captured: apiPageRecords.length,
      api_link_count: apiLinks.length,
      dom_link_count: domLinks.length,
      merged_link_count: links.length,
      api_pagination_attempts: replayedSearchUrls.size,
      api_replay_failures: apiReplayFailures,
      api_page_records: apiPageRecords,
      api_links: apiLinks,
      dom_links: domLinks,
      link_count: links.length,
      links,
    };
    fs.writeFileSync(outJson, JSON.stringify(out, null, 2), 'utf-8');

    if (links.length === 0 && isVerificationGate(searchProbe)) {
      throw new Error(
        `Douyin search blocked by verification page (title="${searchProbe?.title || ''}"). Please solve verification in Chrome and rerun.`
      );
    }
    if (links.length === 0 && isLoginOrBlockedShell(searchProbe)) {
      throw new Error(
        `Douyin search returned login shell with zero video links (title="${searchProbe?.title || ''}"). Please ensure Chrome is logged in and rerun.`
      );
    }
    if (links.length === 0) {
      throw new Error(
        `No video links were captured for keyword="${keyword}". Probe title="${searchProbe?.title || ''}" video_anchor_count=${Number(
          searchProbe?.video_anchor_count || 0
        )}.`
      );
    }

    console.log(`out_json=${outJson}`);
    console.log(`final_links=${links.length}`);
  } finally {
    await finish();
  }
}

main().catch((err) => {
  console.error('CRAWL_ERROR', err?.stack || String(err));
  process.exit(1);
});
