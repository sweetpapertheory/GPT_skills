function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function normalizeCandidate(candidate, index = 0) {
  const kind = candidate?.kind === 'element' ? 'element' : 'window';
  const scrollHeight = Math.max(toNumber(candidate?.scrollHeight), 0);
  const clientHeight = Math.max(toNumber(candidate?.clientHeight), 0);
  const scrollTop = Math.max(toNumber(candidate?.scrollTop), 0);
  const anchorCount = Math.max(toNumber(candidate?.anchorCount), 0);
  const rectHeight = Math.max(toNumber(candidate?.rectHeight), clientHeight, 0);
  const rectWidth = Math.max(toNumber(candidate?.rectWidth), 0);
  const scrollRange = Math.max(scrollHeight - clientHeight, 0);
  const overflowY = String(candidate?.overflowY || '').toLowerCase();
  const isExplicitlyScrollable = /(auto|scroll|overlay)/.test(overflowY);
  const overflowBonus = isExplicitlyScrollable ? 180 : 0;
  const densityScore = Math.min(anchorCount, 120) * 100;
  const rangeScore = Math.min(scrollRange, 25000) / 10;
  const viewportScore = Math.min(rectHeight, 2000) / 4;
  const widthPenalty = rectWidth > 0 && rectWidth < 320 ? 250 : 0;
  const kindBonus = kind === 'element' ? 120 : 0;

  return {
    key: String(candidate?.key || `${kind}:${index}`),
    kind,
    anchorCount,
    clientHeight,
    scrollHeight,
    scrollTop,
    rectHeight,
    rectWidth,
    overflowY,
    isExplicitlyScrollable,
    scrollRange,
    score: densityScore + rangeScore + viewportScore + overflowBonus + kindBonus - widthPenalty,
  };
}

function chooseScrollTarget({ candidates } = {}) {
  const normalized = Array.isArray(candidates)
    ? candidates.map((candidate, index) => normalizeCandidate(candidate, index))
    : [];

  const windowCandidate =
    normalized.find((candidate) => candidate.kind === 'window') ||
    normalizeCandidate({ key: 'window', kind: 'window' });

  const sorted = normalized
    .filter((candidate) => candidate.scrollRange > 0 || candidate.anchorCount > 0)
    .sort((left, right) => right.score - left.score);

  const best = sorted[0] || windowCandidate;
  if (best.kind !== 'element') {
    return best;
  }

  const weakElement =
    best.anchorCount < Math.max(3, Math.ceil(windowCandidate.anchorCount * 0.35)) &&
    best.scrollRange < Math.max(windowCandidate.scrollRange * 1.5, 1200);

  const marginalElementWithoutAffordance =
    !best.isExplicitlyScrollable &&
    best.anchorCount <= windowCandidate.anchorCount + 2 &&
    best.scrollRange <= windowCandidate.scrollRange + 800;

  if (weakElement || marginalElementWithoutAffordance || best.score < windowCandidate.score + 120) {
    return windowCandidate;
  }

  return best;
}

function summarizeScrollProgress({ target, before, after } = {}) {
  const chosen = normalizeCandidate(target || after || before);
  const beforeTop = toNumber(before?.scrollTop ?? before?.y ?? before?.windowY);
  const afterTop = toNumber(after?.scrollTop ?? after?.y ?? after?.windowY);
  const beforeHeight = Math.max(toNumber(before?.scrollHeight ?? before?.h), 0);
  const afterHeight = Math.max(toNumber(after?.scrollHeight ?? after?.h), beforeHeight);
  const clientHeight = Math.max(toNumber(after?.clientHeight), chosen.clientHeight, 0);
  const delta = afterTop - beforeTop;
  const remaining = Math.max(afterHeight - (afterTop + clientHeight), 0);

  return {
    key: chosen.key,
    kind: chosen.kind,
    delta,
    moved: Math.abs(delta) >= 1 || afterHeight > beforeHeight + 1,
    atEnd: remaining <= 48,
    remaining,
    scrollTop: afterTop,
    scrollHeight: afterHeight,
    clientHeight,
  };
}

module.exports = {
  chooseScrollTarget,
  normalizeCandidate,
  summarizeScrollProgress,
};
