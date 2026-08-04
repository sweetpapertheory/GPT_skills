---
name: plain-publication-writing
description: Write, rewrite, polish, or simplify English academic publication text (manuscript sections, abstracts, cover letters, responses to reviewers) into plain, simple, logically connected prose. Use when the user asks to write, revise, polish, simplify, or proofread English for a journal paper, thesis, manuscript, or academic publication, especially when they want simple vocabulary, basic transition words, no dashes, no italics, no added summary ending, and clear cause-effect logic between sentences and paragraphs.
---

# Plain Publication Writing

Write academic English so that every sentence is short, simple, and logically connected. Prefer clarity over style. A reader should never need to re-read a sentence to understand it.

## Hard Rules

1. Do not use em dashes or en dashes to insert clauses. Split into separate sentences or use commas. Keep a dash only when no alternative exists (almost never).
2. Do not use "yet" as a conjunction or adverb. Use "but", "however", or "still".
3. Do not use italics unless required (for example species names, gene names, journal titles, or the statistical symbol p). Remove `*...*`, `_..._`, `\emph{}`, and `\textit{}` elsewhere.
4. Use only basic transition words. Replace all others using the whitelist below.
5. Use simple, common vocabulary. Replace complex words using `references/plain-vocabulary.md`.
6. End the text at the last substantive point. Do not add a summary or conclusion paragraph. Delete any added restating ending unless it contains new information.
7. Preserve meaning exactly. Never change numbers, units, data, citations, equations, technical terms, or figure and table references. Do not add or remove claims.

## Transition Whitelist

Use one fixed connective per logical relation and keep it consistent across the whole text.

| Relation | Use | Replace |
|---|---|---|
| Cause | because, since | due to the fact that, owing to, in light of, given that |
| Result | so, as a result | consequently, hence, thus, accordingly, thereby |
| Contrast | but, however | nevertheless, nonetheless, whereas, albeit, on the contrary |
| Addition | also | moreover, furthermore, additionally, in addition, besides |
| Sequence | first, second, next, then, finally | subsequently, thereafter, firstly, in the first place |
| Example | for example, such as | for instance, to illustrate, exemplified by |
| Comparison | similarly, in contrast | likewise, conversely, on the other hand, by the same token |
| Concession | although | despite the fact that, notwithstanding, while it is true that |

## Sentence Rules

- One idea per sentence. Aim for 15 to 25 words. Split sentences longer than about 30 words.
- Prefer active voice ("we measured X"). Use passive voice only when the actor does not matter or when field convention requires it (often in Methods).
- Prefer verbs over nouns: "we analyzed" not "an analysis was performed", "to reduce" not "in order to reduce".
- Remove filler words: "very", "really", "quite", "clearly", "obviously", "it is worth noting that", "it is important to note that".
- Never leave a bare "this", "that", or "these" without a noun: write "this result", not "this".
- Repeat the key noun across sentences instead of stacking connectives.

## Paragraph Rules

- One main idea per paragraph. State it in the first sentence.
- Order content so each paragraph follows from the previous one: problem, then method, then result, then meaning; or cause, then effect.
- If the draft order weakens the logic, reorder sentences or paragraphs and report the change in the change summary.

## Consistency

- Keep verb tense consistent: past tense for what was done and found (Methods, Results), present tense for established knowledge and for what results mean.
- Keep the spelling convention of the original text (US or UK) consistent.
- Keep LaTeX commands, citation keys, math mode, and figure and table labels unchanged.

## Workflow

1. Read the full text first and identify the logical chain: what causes what and what follows what.
2. Fix paragraph order if needed, then rewrite paragraph by paragraph.
3. Apply the hard rules to every sentence.
4. Run the self-check below before delivery.
5. Return the full revised text plus a short change summary: what was simplified, reordered, or deleted.

## Self-Check

Scan the final text for every item:

- No `—` or `–` used as sentence punctuation.
- No "yet".
- No italics markup without a required reason.
- No word from the avoid lists in `references/plain-vocabulary.md`.
- No sentence longer than about 30 words.
- No summary or conclusion paragraph added at the end.
- No changed numbers, citations, technical terms, or LaTeX commands.
- Every paragraph starts with a clear idea and connects to the previous one.
