#!/usr/bin/env node

const fs = require('fs');

const inputUrlsJson = process.argv[2];
const outJson = process.argv[3] || '/tmp/douyin_aweme_detail.json';
const port = Number(process.argv[4] || 9222);
const maxVideos = Number(process.argv[5] || 0);
const concurrency = Number(process.argv[6] || 4);
const waitMs = Number(process.argv[7] || 12000);

if (!inputUrlsJson) {
  console.error(
    'Usage: crawl_douyin_aweme_detail_from_urls_cdp.js <input_urls_json> [out_json] [port] [max_videos] [concurrency] [wait_ms]'
  );
  process.exit(1);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function normalizeUrl(url) {
  return String(url || '').split('?')[0];
}

function parseVideoId(url) {
  const m = String(url || '').match(/\/video\/(\d{8,24})/);
  return m ? m[1] : '';
}

function safeJson(body, base64Encoded) {
  try {
    const text = base64Encoded ? Buffer.from(body, 'base64').toString('utf-8') : body;
    return JSON.parse(text);
  } catch {
    return null;
  }
}

async function main() {
  const raw = JSON.parse(fs.readFileSync(inputUrlsJson, 'utf-8'));
  const inputRows = Array.isArray(raw) ? raw : [];

  const dedup = [];
  const seen = new Set();
  for (const row of inputRows) {
    const videoUrl = normalizeUrl(row?.video_url);
    if (!videoUrl) continue;
    const key = String(row?.video_id || parseVideoId(videoUrl)).trim() || videoUrl;
    if (seen.has(key)) continue;
    seen.add(key);
    dedup.push({
      video_id: String(row?.video_id || parseVideoId(videoUrl)).trim(),
      video_url: videoUrl,
    });
    if (maxVideos > 0 && dedup.length >= maxVideos) break;
  }

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
  const sessionEventHandlers = new Map();

  ws.addEventListener('message', (event) => {
    const msg = JSON.parse(event.data);

    if (msg.id && pending.has(msg.id)) {
      const entry = pending.get(msg.id);
      pending.delete(msg.id);
      clearTimeout(entry.timer);
      if (msg.error) entry.reject(new Error(msg.error.message || JSON.stringify(msg.error)));
      else entry.resolve(msg);
      return;
    }

    if (msg.sessionId && sessionEventHandlers.has(msg.sessionId)) {
      try {
        sessionEventHandlers.get(msg.sessionId)(msg);
      } catch {
        // ignore per-session handler failures
      }
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

  async function getPageSnapshot(sessionId) {
    const snap = await send(
      'Runtime.evaluate',
      {
        expression: `(() => ({
          title: document.title || '',
          body_sample: (document.body?.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 500)
        }))()`,
        returnByValue: true,
        awaitPromise: true,
      },
      sessionId
    );
    return snap?.result?.result?.value || {};
  }

  async function captureOne(item) {
    const normalizedUrl = normalizeUrl(item.video_url);
    const videoId = String(item.video_id || parseVideoId(normalizedUrl)).trim();

    const created = await send('Target.createTarget', { url: 'about:blank' });
    const targetId = created.result.targetId;
    const attached = await send('Target.attachToTarget', { targetId, flatten: true });
    const sid = attached.result.sessionId;
    const queue = [];
    let awemeDetail = null;
    let detailRequestUrl = '';

    sessionEventHandlers.set(sid, (msg) => {
      if (msg.method !== 'Network.responseReceived') return;
      const p = msg.params || {};
      const resp = p.response || {};
      const url = String(resp.url || '');
      if (!url.includes('/aweme/v1/web/aweme/detail/')) return;
      queue.push({ requestId: p.requestId, url });
    });

    async function drainQueue() {
      while (queue.length > 0) {
        const entry = queue.shift();
        let payload = null;
        try {
          const bodyRes = await send('Network.getResponseBody', { requestId: entry.requestId }, sid);
          payload = safeJson(bodyRes?.result?.body || '', !!bodyRes?.result?.base64Encoded);
        } catch {
          payload = null;
        }
        if (payload && payload.aweme_detail && typeof payload.aweme_detail === 'object') {
          awemeDetail = payload.aweme_detail;
          detailRequestUrl = entry.url;
        }
      }
    }

    try {
      await send('Page.enable', {}, sid);
      await send('Runtime.enable', {}, sid);
      await send('Network.enable', {}, sid);
      await send('Page.navigate', { url: normalizedUrl }, sid);

      const startedAt = Date.now();
      while (!awemeDetail && Date.now() - startedAt < waitMs) {
        await sleep(800);
        await drainQueue();
      }
      await drainQueue();

      const snap = await getPageSnapshot(sid);
      return {
        video_url: normalizedUrl,
        video_id: videoId,
        ok: !!awemeDetail,
        title: String(snap.title || ''),
        body_sample: String(snap.body_sample || ''),
        detail_request_url: detailRequestUrl,
        aweme_detail: awemeDetail,
      };
    } finally {
      sessionEventHandlers.delete(sid);
      try {
        await send('Target.closeTarget', { targetId });
      } catch {
        // ignore cleanup errors
      }
    }
  }

  const results = new Array(dedup.length);
  let nextIndex = 0;

  async function worker(workerId) {
    while (true) {
      const current = nextIndex++;
      if (current >= dedup.length) return;
      const item = dedup[current];
      try {
        const result = await captureOne(item);
        results[current] = result;
        console.log(
          `worker=${workerId} index=${current + 1}/${dedup.length} ok=${result.ok ? 1 : 0} video_id=${result.video_id}`
        );
      } catch (err) {
        results[current] = {
          video_url: item.video_url,
          video_id: item.video_id,
          ok: false,
          error: String(err?.message || err || 'unknown error'),
        };
        console.log(
          `worker=${workerId} index=${current + 1}/${dedup.length} ok=0 video_id=${item.video_id} error=${String(
            err?.message || err || 'unknown error'
          )}`
        );
      }
    }
  }

  const workerCount = Math.max(1, Math.min(concurrency, dedup.length || 1));
  await Promise.all(Array.from({ length: workerCount }, (_, i) => worker(i + 1)));

  const records = results.filter((row) => row && row.ok && row.aweme_detail);
  const failures = results.filter((row) => row && !row.ok);

  fs.writeFileSync(
    outJson,
    JSON.stringify(
      {
        extracted_at: new Date().toISOString(),
        input_count: dedup.length,
        ok_count: records.length,
        failure_count: failures.length,
        records,
        failures,
      },
      null,
      2
    ),
    'utf-8'
  );

  console.log(`output=${outJson}`);
  console.log(`records=${records.length}`);
  console.log(`failures=${failures.length}`);

  ws.close();
}

main().catch((err) => {
  console.error('AWEME_DETAIL_CRAWL_ERROR', err?.stack || String(err));
  process.exit(1);
});
