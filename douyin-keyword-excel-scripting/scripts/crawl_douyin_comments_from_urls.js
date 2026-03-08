#!/usr/bin/env node

const fs = require('fs');

const inputUrlsJson = process.argv[2];
const outJson = process.argv[3] || '/tmp/douyin_comments.json';
const port = Number(process.argv[4] || 9222);
const maxVideos = Number(process.argv[5] || 0); // 0 => all
const maxScrollRounds = Number(process.argv[6] || 35);
const idleRounds = Number(process.argv[7] || 12);
const initialWaitMs = Number(process.argv[8] || 6000);
const stepWaitMs = Number(process.argv[9] || 1300);
const concurrency = Number(process.argv[10] || process.env.DOUYIN_COMMENT_CONCURRENCY || 4);
const perVideoTimeoutMs = Number(process.env.DOUYIN_COMMENT_PER_VIDEO_TIMEOUT_MS || 120000);

if (!inputUrlsJson) {
  console.error(
    'Usage: crawl_douyin_comments_from_urls.js <input_urls_json> [out_json] [port] [max_videos] [max_scroll_rounds] [idle_rounds] [initial_wait_ms] [step_wait_ms] [concurrency]'
  );
  process.exit(1);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function withTimeout(promise, timeoutMs, label) {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return promise;
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`${label || 'operation'} timeout after ${timeoutMs}ms`)), timeoutMs)
    ),
  ]);
}

function parseVideoId(url) {
  const m = String(url || '').match(/\/video\/(\d{8,24})/);
  return m ? m[1] : '';
}

function toCreateTime(v) {
  if (typeof v === 'number' && Number.isFinite(v) && v > 0) {
    return new Date(v * 1000).toISOString();
  }
  return '';
}

function safeJson(body, base64Encoded) {
  try {
    const txt = base64Encoded ? Buffer.from(body, 'base64').toString('utf-8') : body;
    return JSON.parse(txt);
  } catch {
    return null;
  }
}

function extractCommentsFromPayload(payload, fallbackVideoId) {
  const out = [];
  if (!payload || typeof payload !== 'object') return out;

  const comments = Array.isArray(payload.comments) ? payload.comments : [];
  for (const c of comments) {
    const commentId = String(c.cid || c.comment_id || '').trim();
    if (!commentId) continue;

    const videoId = String(c.aweme_id || c.item_id || fallbackVideoId || '').trim();
    const parentId = String(c.reply_id || c.reply_to_reply_id || c.parent_comment_id || '').trim();
    const text = String(c.text || '').replace(/\s+/g, ' ').trim();
    const likeCount = Number(c.digg_count ?? c.like_count ?? 0) || 0;
    const replyCount = Number(c.reply_comment_total ?? c.reply_count ?? 0) || 0;

    out.push({
      video_id: videoId,
      comment_id: commentId,
      create_time: toCreateTime(c.create_time),
      like_count: likeCount,
      reply_count: replyCount,
      parent_comment_id: parentId,
      text,
      user_name: String(c.user?.nickname || c.user?.unique_id || '').trim(),
    });

    const children = Array.isArray(c.reply_comment) ? c.reply_comment : [];
    for (const rc of children) {
      const rcid = String(rc.cid || rc.comment_id || '').trim();
      if (!rcid) continue;
      out.push({
        video_id: videoId,
        comment_id: rcid,
        create_time: toCreateTime(rc.create_time),
        like_count: Number(rc.digg_count ?? rc.like_count ?? 0) || 0,
        reply_count: Number(rc.reply_comment_total ?? rc.reply_count ?? 0) || 0,
        parent_comment_id: commentId,
        text: String(rc.text || '').replace(/\s+/g, ' ').trim(),
        user_name: String(rc.user?.nickname || rc.user?.unique_id || '').trim(),
      });
    }
  }

  return out;
}

async function main() {
  const urls = JSON.parse(fs.readFileSync(inputUrlsJson, 'utf-8'));
  const videos = (Array.isArray(urls) ? urls : []).filter((x) => x && x.video_url);
  const list = maxVideos > 0 ? videos.slice(0, maxVideos) : videos;

  const version = await fetch(`http://127.0.0.1:${port}/json/version`).then((r) => r.json());
  if (!version.webSocketDebuggerUrl) {
    throw new Error('DevTools websocket not found. Start Chrome with --remote-debugging-port and keep logged-in session.');
  }

  const ws = new WebSocket(version.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    ws.addEventListener('open', resolve, { once: true });
    ws.addEventListener('error', reject, { once: true });
  });

  let id = 0;
  const pending = new Map();
  const sessionEventHandlers = new Map();

  ws.addEventListener('message', (event) => {
    const msg = JSON.parse(event.data);

    if (msg.id && pending.has(msg.id)) {
      const p = pending.get(msg.id);
      pending.delete(msg.id);
      clearTimeout(p.timer);
      if (msg.error) p.reject(new Error(msg.error.message || JSON.stringify(msg.error)));
      else p.resolve(msg);
      return;
    }

    if (msg.sessionId && sessionEventHandlers.has(msg.sessionId)) {
      try {
        sessionEventHandlers.get(msg.sessionId)(msg);
      } catch {
        // ignore handler errors
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

  async function collectCommentsForVideo(v) {
    const videoUrl = String(v.video_url || '').trim();
    const fallbackVideoId = String(v.video_id || parseVideoId(videoUrl));
    if (!videoUrl) return [];

    const created = await send('Target.createTarget', { url: 'about:blank' });
    const targetId = created.result.targetId;
    const attached = await send('Target.attachToTarget', {
      targetId,
      flatten: true,
    });
    const sid = attached.result.sessionId;

    const responseQueue = [];
    const localComments = [];
    const localSeen = new Set();

    sessionEventHandlers.set(sid, (msg) => {
      if (msg.method === 'Network.responseReceived') {
        const p = msg.params || {};
        const resp = p.response || {};
        const url = String(resp.url || '');
        if (!url.includes('/aweme/v1/web/comment/list')) return;
        responseQueue.push({ requestId: p.requestId, url });
      }
    });

    async function drainQueue() {
      while (responseQueue.length) {
        const item = responseQueue.shift();
        let bodyObj;
        try {
          const bodyRes = await send('Network.getResponseBody', { requestId: item.requestId }, sid);
          bodyObj = safeJson(bodyRes.result?.body || '', !!bodyRes.result?.base64Encoded);
        } catch {
          bodyObj = null;
        }

        const extracted = extractCommentsFromPayload(bodyObj, fallbackVideoId);
        for (const c of extracted) {
          const key = `${c.video_id}:${c.comment_id}`;
          if (!key || localSeen.has(key)) continue;
          localSeen.add(key);
          localComments.push(c);
        }
      }
    }

    try {
      await send('Page.enable', {}, sid);
      await send('Runtime.enable', {}, sid);
      await send('Network.enable', {}, sid);
      await send('Page.navigate', { url: videoUrl }, sid);
      await sleep(initialWaitMs);

      let idle = 0;
      await drainQueue();
      for (let round = 0; round < maxScrollRounds; round++) {
        const before = localComments.length;
        await send(
          'Runtime.evaluate',
          {
            expression: `(() => {
              window.scrollBy(0, Math.floor(window.innerHeight * 0.95));
              const h = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight || 0, window.innerHeight);
              if (window.scrollY + window.innerHeight + 20 >= h) window.scrollTo(0, h + window.innerHeight);
              return { y: window.scrollY, h };
            })()`,
            returnByValue: true,
          },
          sid
        );

        await sleep(stepWaitMs);
        await drainQueue();
        const gained = localComments.length - before;
        if (gained === 0) idle += 1;
        else idle = 0;
        if (idle >= idleRounds) break;
      }
    } finally {
      sessionEventHandlers.delete(sid);
      try {
        await send('Target.closeTarget', { targetId });
      } catch {
        // ignore close failures
      }
    }

    return localComments;
  }

  const allComments = [];
  const commentSeen = new Set();
  const failures = [];
  let nextIndex = 0;

  function mergeComments(comments) {
    let added = 0;
    for (const c of comments) {
      const key = `${c.video_id}:${c.comment_id}`;
      if (!key || commentSeen.has(key)) continue;
      commentSeen.add(key);
      allComments.push(c);
      added += 1;
    }
    return added;
  }

  async function worker(workerId) {
    while (true) {
      const current = nextIndex++;
      if (current >= list.length) return;
      const v = list[current];
      try {
        const comments = await withTimeout(
          collectCommentsForVideo(v),
          perVideoTimeoutMs,
          `video ${current + 1}`
        );
        const added = mergeComments(comments);
        console.log(
          `worker=${workerId} video ${current + 1}/${list.length} comments_added=${added} total_comments=${allComments.length}`
        );
      } catch (err) {
        failures.push({
          video_id: String(v.video_id || parseVideoId(v.video_url || '')).trim(),
          video_url: String(v.video_url || '').trim(),
          error: String(err?.message || err || 'unknown error'),
        });
        console.log(
          `worker=${workerId} video ${current + 1}/${list.length} comments_added=0 total_comments=${allComments.length} error=${String(
            err?.message || err || 'unknown error'
          )}`
        );
      }
    }
  }

  const workerCount = Math.max(1, Math.min(concurrency, list.length || 1));
  await Promise.all(Array.from({ length: workerCount }, (_, i) => worker(i + 1)));

  const out = {
    extracted_at: new Date().toISOString(),
    videos_processed: list.length,
    comments_count: allComments.length,
    failure_count: failures.length,
    failures,
    comments: allComments,
  };

  fs.writeFileSync(outJson, JSON.stringify(out, null, 2), 'utf-8');
  console.log(`output=${outJson}`);
  console.log(`comments=${allComments.length}`);
  console.log(`failures=${failures.length}`);

  ws.close();
  process.exit(0);
}

main().catch((err) => {
  console.error('COMMENT_CRAWL_ERROR', err?.stack || String(err));
  process.exit(1);
});
