// Run with Node from any directory. The Markdown list, not the SVG, is the source.
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const visuals = dirname(fileURLToPath(import.meta.url));
const guides = dirname(visuals);
const heading = '## Flow at a glance';
const usage = 'Usage: node render-flows.mjs [--check]';

function visibleLines(source) {
  let fence;
  return source.replace(/^\uFEFF/, '').split(/\r\n?|\n/u).map(line => {
    if (fence) {
      if (fence.test(line)) fence = undefined;
      return '';
    }
    const opening = /^ {0,3}(`{3,}|~{3,})/u.exec(line);
    if (!opening) return line;
    const run = opening[1];
    fence = new RegExp(`^ {0,3}${run[0]}{${run.length},}[ \\t]*$`, 'u');
    return '';
  });
}

function parseGuide(file, source) {
  const lines = visibleLines(source);
  const start = lines.indexOf(heading);
  if (start < 0) throw new Error(`${file}: missing exact heading "${heading}".`);
  const duplicate = lines.indexOf(heading, start + 1);
  if (duplicate >= 0) {
    throw new Error(`${file}:${duplicate + 1}: duplicate "${heading}" (first at ${start + 1}).`);
  }

  const title = lines.find(line => /^# +\S/u.test(line))?.replace(/^# +| +#+ *$/gu, '')
    ?? file.slice(0, -3);
  const steps = [];
  let afterStep = false;
  for (let i = start + 1; i < lines.length; i++) {
    const line = lines[i].trimEnd();
    const where = `${file}:${i + 1}`;
    if (/^ {0,3}#{1,2}(?:\s|$)/u.test(line)) break;
    if (/^\s*\d+[.)](?!\d)|^\s*[-+*]\s+\*\*/u.test(line)) {
      const n = steps.length + 1;
      if (n > 3) throw new Error(`${where}: extra flow step; exactly three are required.`);
      const match = /^([1-3])\. \*\*([^*]+)\*\*: (\S.*\.)$/u.exec(line);
      if (!match || Number(match[1]) !== n || match[2] !== match[2].trim()) {
        throw new Error(`${where}: expected "${n}. **Actor or state**: Plain concise action."`);
      }
      const [, , actor, action] = match;
      if (/`|\*\*|\[[^\]]*\][[(]/u.test(`${actor} ${action}`)) {
        throw new Error(`${where}: use plain text, without code spans, emphasis or links.`);
      }
      steps.push({ actor, action });
      afterStep = true;
    } else {
      const otherBlock = /^(?: {0,3}#{3,6}\s|!\[)/u.test(line);
      if (line && !otherBlock && (afterStep || (steps.length && /^\s/u.test(line)))) {
        throw new Error(`${where}: keep step ${steps.length} on one line; separate prose with a blank line.`);
      }
      afterStep = false;
    }
  }
  if (steps.length !== 3) {
    throw new Error(`${file}:${start + 1}: expected three numbered flow steps; found ${steps.length}.`);
  }
  return { title, steps };
}

const entities = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;' };
function xml(text) {
  if (/[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD\u{10000}-\u{10FFFF}]/u.test(text)) {
    throw new Error('text contains a character not representable in XML 1.0.');
  }
  return text.replace(/[&<>"']/gu, ch => entities[ch]);
}

// Use a narrow intrinsic canvas rather than shrinking a desktop diagram on phones.
const width = 360, cardX = 48, textX = 62, right = width - 10;
const actorSize = 19, actionSize = 18, leading = 23;
const graphemes = new Intl.Segmenter('en', { granularity: 'grapheme' });

// Conservative em advances for the sans-serif stack. Unknown glyphs get a full
// wide cell; unusually long identifiers wrap without dropping any characters.
function advance(ch) {
  if (/^[il.,'`:;!|]$/u.test(ch)) return 0.36;
  if (/^[ \-tfrj()[\]{}]$/u.test(ch)) return 0.48;
  if (/^[MW@%&]$/u.test(ch)) return 1.05;
  if (/^[mw]$/u.test(ch)) return 0.90;
  if (/^[a-z0-9]$/u.test(ch)) return 0.65;
  if (/^[\x20-\x7e]$/u.test(ch)) return 0.86;
  return 1.18;
}

function wrap(text, size) {
  const limit = (right - textX - 14) / size;
  const lines = [];
  let line = '', used = 0;
  for (const word of text.split(/\s+/u)) {
    const chars = Array.from(graphemes.segment(word), part => part.segment);
    const wordWidth = chars.reduce((sum, ch) => sum + advance(ch), 0);
    if (line && used + advance(' ') + wordWidth <= limit) {
      line += ` ${word}`;
      used += advance(' ') + wordWidth;
      continue;
    }
    if (line) lines.push(line);
    line = '';
    used = 0;
    for (const ch of chars) {
      const w = advance(ch);
      if (line && used + w > limit) {
        lines.push(line);
        line = '';
        used = 0;
      }
      line += ch;
      used += w;
    }
  }
  if (line) lines.push(line);
  return lines;
}

function spans(lines, firstY) {
  return lines.map((line, i) =>
    `<tspan x="${textX}" y="${firstY + leading * i}">${xml(line)}</tspan>`).join('');
}

function render({ title, steps }) {
  let top = 10;
  const stops = [];
  const cards = steps.map(({ actor, action }, i) => {
    const label = wrap(actor, actorSize), body = wrap(action, actionSize);
    const labelY = top + 26, stop = labelY - 6;
    const bodyY = labelY + leading * label.length + 2;
    const bottom = bodyY + leading * (body.length - 1) + 14;
    stops.push(stop);
    const card = `  <rect x="${cardX}" y="${top}" width="${right - cardX}" height="${bottom - top}" rx="9" fill="#fff" stroke="#a8bdbb"/>
  <circle cx="24" cy="${stop}" r="12" fill="#184d50"/>
  <text x="24" y="${stop + 6}" text-anchor="middle" font-size="17" font-weight="700" fill="#fff">${i + 1}</text>
  <text font-size="${actorSize}" font-weight="700" fill="#12383d">${spans(label, labelY)}</text>
  <text font-size="${actionSize}" fill="#223a40">${spans(body, bodyY)}</text>`;
    top = bottom + 10;
    return card;
  }).join('\n');
  const height = top, [first, middle, last] = stops;
  const description = steps.map(({ actor, action }, i) => `${i + 1}. ${actor}: ${action}`).join(' ');
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="flow-title" aria-describedby="flow-desc">
<title id="flow-title">${xml(title)}: Flow at a glance</title>
<desc id="flow-desc">${xml(description)}</desc>
<style>
@keyframes handoff {
  0%, 18% { transform: translateY(0); }
  40%, 58% { transform: translateY(${middle - first}px); }
  82%, 100% { transform: translateY(${last - first}px); }
}
@media (prefers-reduced-motion: no-preference) {
  #focus { display: inline; animation: handoff 4.6s ease-in-out 1 both; }
}
@media (prefers-reduced-motion: reduce) { #focus { display: none; animation: none; } }
</style>
<rect x="1" y="1" width="${width - 2}" height="${height - 2}" rx="13" fill="#f2f7f6" stroke="#b8cbc8"/>
<path d="M24 ${first} V${last}" fill="none" stroke="#577d7b" stroke-width="2"/>
<g font-family="Arial, Helvetica, sans-serif">
${cards}
</g>
<g id="focus" display="none" aria-hidden="true">
  <circle cx="24" cy="${first}" r="16" fill="none" stroke="#17484d" stroke-width="2"/>
  <path d="M40 ${first - 5} l6 5 -6 5 z" fill="#17484d"/>
</g>
</svg>
`;
}

function run(args) {
  if (args.length === 1 && ['--help', '-h'].includes(args[0])) {
    console.log(`${usage}\nBuild all top-level guide SVGs; --check compares without writing.`);
    return;
  }
  const check = args.length === 1 && args[0] === '--check';
  if (args.length && !check) throw new Error(usage);

  const files = readdirSync(guides, { withFileTypes: true })
    .filter(entry => entry.isFile() && /\.md$/iu.test(entry.name)).map(entry => entry.name).sort();
  if (!files.length) throw new Error(`No top-level Markdown guides found in ${guides}.`);

  const expected = new Map(), sources = new Map(), errors = [];
  for (const file of files) {
    const name = `${/^readme\.md$/iu.test(file) ? 'overview' : file.slice(0, -3)}.svg`;
    const key = name.toLowerCase();
    if (sources.has(key)) {
      errors.push(`${file} and ${sources.get(key)} map to the same asset: ${name}.`);
      continue;
    }
    sources.set(key, file);
    try {
      const flow = parseGuide(file, readFileSync(join(guides, file), 'utf8'));
      expected.set(name, render(flow));
    } catch (error) {
      errors.push(error.message.startsWith(`${file}:`) ? error.message : `${file}: ${error.message}`);
    }
  }
  // An invalid guide never leaves a partially regenerated set of diagrams.
  if (errors.length) throw new Error(errors.join('\n'));

  const present = new Set(), changed = [];
  for (const entry of readdirSync(visuals, { withFileTypes: true })) {
    if (!/\.svg$/iu.test(entry.name)) continue;
    if (!expected.has(entry.name)) {
      errors.push(`visuals/${entry.name}: orphaned SVG; restore its guide or remove it manually.`);
    } else if (!entry.isFile()) {
      errors.push(`visuals/${entry.name}: expected a regular SVG file.`);
    } else {
      present.add(entry.name);
    }
  }
  for (const [name, svg] of expected) {
    const existing = present.has(name) ? readFileSync(join(visuals, name), 'utf8') : null;
    if (existing?.replace(/\r\n/gu, '\n') === svg) continue;
    changed.push(name);
    if (check) errors.push(`visuals/${name}: ${existing === null ? 'missing' : 'stale'} (${sources.get(name.toLowerCase())}).`);
  }
  if (check && changed.length) errors.push('Run the generator without --check to rebuild.');
  if (errors.length) throw new Error(errors.join('\n'));

  if (check) {
    console.log(`Checked ${expected.size} flow SVGs; all are current.`);
  } else {
    for (const name of changed) writeFileSync(join(visuals, name), expected.get(name), 'utf8');
    console.log(`Built ${expected.size} flow SVGs (${changed.length} updated).`);
  }
}

try {
  run(process.argv.slice(2));
} catch (error) {
  console.error(`render-flows: ${error.message}`);
  process.exitCode = 1;
}
