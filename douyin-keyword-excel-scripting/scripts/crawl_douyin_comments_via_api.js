#!/usr/bin/env node
const fs = require('fs');

const inputUrlsJson = process.argv[2];
const outJson = process.argv[3] || '/tmp/douyin_comments.json';
const port = Number(process.argv[4] || 9222);
const maxVideos = Number(process.argv[5] || 0);
const maxCommentsPerVideo = Number(process.argv[6] || 40);
const pageSize = Number(process.argv[7] || 20);
const concurrency = Number(process.argv[8] || 4);
const bootUrl = process.argv[9] || 'https://www.douyin.com/';

if (!inputUrlsJson) {
  console.error('Usage: douyin_comment_api_fetch.js <input_urls_json> [out_json] [port] [max_videos] [max_comments_per_video] [page_size] [concurrency] [boot_url]');
  process.exit(1);
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
  const raw = JSON.parse(fs.readFileSync(inputUrlsJson, 'utf-8'));
  const urls = (Array.isArray(raw) ? raw : []).filter((x) => x && x.video_url);
  const dedup = [];
  const seen = new Set();
  for (const row of urls) {
    const videoId = String(row.video_id || parseVideoId(row.video_url || '')).trim();
    const videoUrl = String(row.video_url || '').trim();
    const key = videoId || videoUrl;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    dedup.push({ video_id: videoId, video_url: videoUrl });
    if (maxVideos > 0 && dedup.length >= maxVideos) break;
  }

  const version = await fetch(`http://127.0.0.1:${port}/json/version`).then((r) => r.json());
  if (!version.webSocketDebuggerUrl) {
    throw new Error('Chrome DevTools websocket not found on requested port.');
  }

  const ws = new WebSocket(version.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    ws.addEventListener('open', resolve, { once: true });
    ws.addEventListener('error', reject, { once: true });
  });

  let id = 0;
  const pending = new Map();
  ws.addEventListener('message', (event) => {
    const msg = JSON.parse(event.data);
    if (!msg.id || !pending.has(msg.id)) return;
    const p = pending.get(msg.id);
    pending.delete(msg.id);
    clearTimeout(p.timer);
    if (msg.error) p.reject(new Error(msg.error.message || JSON.stringify(msg.error)));
    else p.resolve(msg);
  });

  function send(method, params = {}, sessionId) {
    const msgId = ++id;
    const payload = { id: msgId, method, params };
    if (sessionId) payload.sessionId = sessionId;
    ws.send(JSON.stringify(payload));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        pending.delete(msgId);
        reject(new Error(`Timeout for ${method}`));
      }, 70000);
      pending.set(msgId, { resolve, reject, timer });
    });
  }

  async function makeSession() {
    const created = await send('Target.createTarget', { url: 'about:blank' });
    const targetId = created.result.targetId;
    const attached = await send('Target.attachToTarget', { targetId, flatten: true });
    const sid = attached.result.sessionId;
    await send('Page.enable', {}, sid);
    await send('Runtime.enable', {}, sid);
    await send('Page.navigate', { url: bootUrl }, sid);
    await new Promise((r) => setTimeout(r, 4000));
    return { sid, targetId };
  }

  async function evalJson(sessionId, expression) {
    const res = await send('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true }, sessionId);
    return res.result.result.value;
  }

  async function fetchCommentPage(sessionId, awemeId, cursor) {
    const url = `https://www.douyin.com/aweme/v1/web/comment/list/?device_platform=webapp&aid=6383&channel=channel_pc_web&aweme_id=${encodeURIComponent(awemeId)}&cursor=${encodeURIComponent(String(cursor))}&count=${encodeURIComponent(String(pageSize))}&item_type=0`;
    const expr = `(() => fetch(${JSON.stringify(url)}, {credentials:'include'})
      .then(async r => ({status:r.status, json: JSON.parse(await r.text())})))()`;
    return await evalJson(sessionId, expr);
  }

  const allComments = [];
  const seenComments = new Set();
  const failures = [];
  let nextIndex = 0;

  function addComments(rows) {
    let added = 0;
    for (const c of rows) {
      const key = `${c.video_id}:${c.comment_id}`;
      if (!key || seenComments.has(key)) continue;
      seenComments.add(key);
      allComments.push(c);
      added += 1;
    }
    return added;
  }

  async function processVideo(sessionId, item) {
    const awemeId = item.video_id || parseVideoId(item.video_url || '');
    if (!awemeId) return { total: 0, added: 0, api_total: 0 };
    let cursor = 0;
    let hasMore = true;
    let apiTotal = 0;
    let totalAdded = 0;
    let pages = 0;
    while (hasMore && totalAdded < maxCommentsPerVideo) {
      const payload = await fetchCommentPage(sessionId, awemeId, cursor);
      if (!payload || Number(payload.status) !== 200 || !payload.json || typeof payload.json !== 'object') {
        throw new Error(`comment api failed aweme_id=${awemeId} status=${payload ? payload.status : 'NA'}`);
      }
      const j = payload.json;
      apiTotal = Number(j.total || apiTotal || 0);
      const rows = extractCommentsFromPayload(j, awemeId);
      const remaining = maxCommentsPerVideo - totalAdded;
      const kept = rows.slice(0, Math.max(remaining, 0));
      totalAdded += addComments(kept);
      cursor = Number(j.cursor || 0);
      hasMore = Boolean(j.has_more) && kept.length > 0 && pages < 50;
      pages += 1;
      if (!hasMore) break;
    }
    return { total: totalAdded, api_total: apiTotal };
  }

  async function worker(workerId) {
    const { sid, targetId } = await makeSession();
    try {
      while (true) {
        const current = nextIndex++;
        if (current >= dedup.length) return;
        const item = dedup[current];
        try {
          const result = await processVideo(sid, item);
          console.log(`worker=${workerId} video ${current + 1}/${dedup.length} comments_added=${result.total} api_total=${result.api_total} total_comments=${allComments.length}`);
        } catch (err) {
          failures.push({ video_id: item.video_id, video_url: item.video_url, error: String(err?.message || err || 'unknown error') });
          console.log(`worker=${workerId} video ${current + 1}/${dedup.length} comments_added=0 total_comments=${allComments.length} error=${String(err?.message || err || 'unknown error')}`);
        }
      }
    } finally {
      try { await send('Target.closeTarget', { targetId }); } catch {}
    }
  }

  const workerCount = Math.max(1, Math.min(concurrency, dedup.length || 1));
  await Promise.all(Array.from({ length: workerCount }, (_, i) => worker(i + 1)));

  fs.writeFileSync(outJson, JSON.stringify({
    extracted_at: new Date().toISOString(),
    videos_processed: dedup.length,
    comments_count: allComments.length,
    failure_count: failures.length,
    failures,
    comments: allComments,
  }, null, 2), 'utf-8');
  console.log(`output=${outJson}`);
  console.log(`comments=${allComments.length}`);
  console.log(`failures=${failures.length}`);
  ws.close();
}

main().catch((err) => {
  console.error('COMMENT_API_FETCH_ERROR', err?.stack || String(err));
  process.exit(1);
});
