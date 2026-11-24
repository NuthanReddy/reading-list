// Dependency-free generator for Blueprint Spatial Multi-Component Topology SVGs.
// Run with Node from any directory. Source of truth: visuals/topology-specs.json.
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const visualsDir = dirname(fileURLToPath(import.meta.url));
const specsPath = join(visualsDir, 'topology-specs.json');
const USAGE = 'Usage: node render-flows.mjs [--check] [--help]';

const CANVAS_W = 960;
const CANVAS_H = 580;
const STAGE_X = 16;
const STAGE_Y = 54;
const STAGE_W = 928;
const STAGE_H = 350;
const LEGEND_X = 16;
const LEGEND_Y = 412;
const LEGEND_W = 928;
const LEGEND_H = 158;

const NODE_W = 154;
const NODE_H = 46;
const ZONE_HEADER_H = 22;
const ZONE_GAP = 12;
const ROW_GAP = 12;
const ZONE_PAD_X = 14;
const ZONE_PAD_Y = 12;

const SANS_FONT =
  "system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif";
const MONO_FONT =
  "ui-monospace, 'Cascadia Code', 'SFMono-Regular', Menlo, Consolas, monospace";

const ARCHITECTURAL_TIERS = [
  ['submit-host', 'yarn-control', 'storage-history'],
  ['driver-jvm'],
  ['worker-node-a', 'worker-node-b'],
];

const HOST_ZONE_LABELS = {
  'submit-host': 'SUBMIT HOST',
  'yarn-control': 'YARN CONTROL',
  'storage-history': 'HDFS & HISTORY',
  'driver-jvm': 'AM / DRIVER CONTAINER',
  'worker-node-a': 'WORKER NODE A',
  'worker-node-b': 'WORKER NODE B',
};

const CHANNEL_STYLES = {
  yarn: { color: '#7aa2ff', label: 'YARN', dash: null },
  rpc: { color: '#5bd3a8', label: 'RPC', dash: null },
  data: { color: '#f2b950', label: 'DATA', dash: null },
  python: { color: '#ec82b3', label: 'PYTHON', dash: null },
  internal: { color: '#bfa6ff', label: 'IN-JVM', dash: '4 3' },
  error: { color: '#f87171', label: 'ERROR', dash: '6 3' },
};

const CHANNEL_ORDER = ['yarn', 'rpc', 'data', 'python', 'internal', 'error'];

const SUBSYSTEM_ACCENTS = {
  submission: '#7aa2ff',
  'cluster-hdfs': '#7aa2ff',
  scheduling: '#5bd3a8',
  'executor-rpc': '#5bd3a8',
  'block-storage': '#f2b950',
  shuffle: '#f2b950',
  'python-output': '#ec82b3',
  observability: '#bfa6ff',
  'driver-runtime': '#7aa2ff',
  'sql-execution': '#7aa2ff',
};

const XML_ENTITIES = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&apos;',
};

function xml(text) {
  const str = String(text ?? '');
  if (/[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD\u{10000}-\u{10FFFF}]/u.test(str)) {
    throw new Error(`Text contains invalid XML 1.0 character: ${JSON.stringify(str)}`);
  }
  return str.replace(/[&<>"']/gu, ch => XML_ENTITIES[ch]);
}

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

function num(v) {
  return Number(v.toFixed(1));
}

function approxTextWidth(text, fontSize, isMono = false) {
  if (!text) return 0;
  if (isMono) return text.length * fontSize * 0.625;
  let units = 0;
  for (const ch of text) {
    if ('ilI.,:;!|\' '.includes(ch)) units += 0.34;
    else if ('fjrt()-/[]'.includes(ch)) units += 0.44;
    else if ('MW@%&'.includes(ch)) units += 0.96;
    else if ('mw'.includes(ch)) units += 0.84;
    else if (ch >= 'A' && ch <= 'Z') units += 0.72;
    else if (ch.codePointAt(0) > 0x7f) units += 0.92;
    else units += 0.58;
  }
  return units * fontSize;
}

function truncatePx(text, maxPx, fontSize, isMono = false) {
  const str = String(text ?? '');
  if (approxTextWidth(str, fontSize, isMono) <= maxPx) return str;
  const suffix = '...';
  const suffixW = approxTextWidth(suffix, fontSize, isMono);
  let out = '';
  for (const ch of str) {
    if (approxTextWidth(out + ch, fontSize, isMono) + suffixW > maxPx) break;
    out += ch;
  }
  return `${out.trimEnd()}${suffix}`;
}

function hopStyle(hop) {
  if (hop.isError || hop.channel === 'error') return CHANNEL_STYLES.error;
  return CHANNEL_STYLES[hop.channel] ?? CHANNEL_STYLES.rpc;
}

function hopChannelKey(hop) {
  return hop.isError || hop.channel === 'error' ? 'error' : hop.channel;
}

function subsystemAccent(subsystem) {
  return SUBSYSTEM_ACCENTS[subsystem] ?? '#7aa2ff';
}

function computeLayout(spec) {
  const byZone = new Map();
  for (const node of spec.nodes) {
    const list = byZone.get(node.hostZone) ?? [];
    list.push(node);
    byZone.set(node.hostZone, list);
  }

  const selfLoopIds = new Set();
  for (const hop of spec.hops) {
    if (hop.from === hop.to) selfLoopIds.add(hop.from);
  }

  const activeTiers = ARCHITECTURAL_TIERS.map(tier =>
    tier.filter(z => (byZone.get(z)?.length ?? 0) > 0),
  ).filter(tier => tier.length > 0);

  const tierSpecs = activeTiers.map(tierZones => {
    const availW = STAGE_W - ZONE_GAP * (tierZones.length - 1);
    const minZoneW = Math.min(NODE_W + ZONE_PAD_X * 2, availW / tierZones.length);
    const weights = tierZones.map(z => Math.max(1, byZone.get(z).length));
    const weightSum = weights.reduce((a, b) => a + b, 0);
    const extraW = Math.max(0, availW - minZoneW * tierZones.length);

    return tierZones.map((zoneId, idx) => {
      const nodes = byZone.get(zoneId);
      const w = minZoneW + (extraW * weights[idx]) / weightSum;
      const innerW = Math.max(NODE_W, w - ZONE_PAD_X * 2);
      const minGapX = 14;
      const fitCols = Math.max(1, Math.floor((innerW + minGapX) / (NODE_W + minGapX)));
      let maxCols = fitCols;
      if (activeTiers.length <= 2 && nodes.length >= 5) {
        maxCols = Math.min(fitCols, 3);
      }
      const gridRows = Math.max(1, Math.ceil(nodes.length / maxCols));
      const cols = Math.max(1, Math.ceil(nodes.length / gridRows));
      const hasTopSelfLoop = nodes.slice(0, cols).some(n => selfLoopIds.has(n.id));
      const topLoopExtra = hasTopSelfLoop ? 26 : 0;
      const minGapY = gridRows > 1 ? 26 : 0;
      const minH =
        ZONE_HEADER_H +
        ZONE_PAD_Y * 2 +
        topLoopExtra +
        gridRows * NODE_H +
        (gridRows - 1) * minGapY;
      return { zoneId, w, nodes, cols, gridRows, hasTopSelfLoop, topLoopExtra, minH };
    });
  });

  const tierMinHeights = tierSpecs.map(tier => Math.max(...tier.map(z => z.minH)));
  const totalGaps = ROW_GAP * Math.max(0, tierSpecs.length - 1);
  const sumMinH = tierMinHeights.reduce((a, b) => a + b, 0);
  const availH = STAGE_H - totalGaps;

  const tierHeights = tierMinHeights.map(mh => {
    if (sumMinH <= availH) {
      const extra = availH - sumMinH;
      return mh + (extra * mh) / Math.max(1, sumMinH);
    }
    return (mh * availH) / Math.max(1, sumMinH);
  });

  const heightTotal = tierHeights.reduce((a, b) => a + b, 0) + totalGaps;
  if (tierHeights.length > 0) {
    tierHeights[tierHeights.length - 1] += STAGE_H - heightTotal;
  }

  const zones = [];
  const placedNodes = [];
  const nodeById = new Map();

  let currY = STAGE_Y;
  tierSpecs.forEach((tier, tIdx) => {
    const rh = tierHeights[tIdx];
    let currX = STAGE_X;

    tier.forEach(zs => {
      zones.push({
        id: zs.zoneId,
        label: HOST_ZONE_LABELS[zs.zoneId] ?? zs.zoneId.toUpperCase(),
        x: currX,
        y: currY,
        w: zs.w,
        h: rh,
        headerH: ZONE_HEADER_H,
      });

      const bodyTop = currY + ZONE_HEADER_H + ZONE_PAD_Y;
      const bodyH = Math.max(NODE_H, rh - ZONE_HEADER_H - ZONE_PAD_Y * 2);
      const bodyLeft = currX + ZONE_PAD_X;
      const bodyW = Math.max(NODE_W, zs.w - ZONE_PAD_X * 2);

      const topExtra = zs.hasTopSelfLoop
        ? Math.min(26, Math.max(12, (bodyH - zs.gridRows * NODE_H) * 0.48))
        : 0;
      const availYForGaps = Math.max(0, bodyH - topExtra - zs.gridRows * NODE_H);
      const maxFitGapY = zs.gridRows > 1 ? availYForGaps / (zs.gridRows - 1) : 0;
      const gapY =
        zs.gridRows > 1
          ? clamp(availYForGaps / (zs.gridRows + 0.35), Math.min(24, maxFitGapY), Math.min(64, maxFitGapY))
          : 0;
      const totalGridH = zs.gridRows * NODE_H + (zs.gridRows - 1) * gapY;
      const gridStartY = bodyTop + topExtra + Math.max(0, (bodyH - topExtra - totalGridH) / 2);

      for (let r = 0; r < zs.gridRows; r++) {
        const rowNodes = zs.nodes.slice(r * zs.cols, (r + 1) * zs.cols);
        const count = rowNodes.length;
        const availX = Math.max(0, bodyW - count * NODE_W);
        const maxFitGapX = count > 1 ? availX / (count - 1) : 0;
        const gapX =
          count > 1
            ? clamp(availX / (count + 0.45), Math.min(16, maxFitGapX), Math.min(72, maxFitGapX))
            : 0;
        const totalRowW = count * NODE_W + (count - 1) * gapX;
        const rowStartX = bodyLeft + Math.max(0, (bodyW - totalRowW) / 2);
        const cy = gridStartY + r * (NODE_H + gapY) + NODE_H / 2;

        rowNodes.forEach((node, cIdx) => {
          const cx = rowStartX + cIdx * (NODE_W + gapX) + NODE_W / 2;
          const placed = {
            ...node,
            cx,
            cy,
            x: cx - NODE_W / 2,
            y: cy - NODE_H / 2,
            w: NODE_W,
            h: NODE_H,
            isFocal: spec.focusComponentId === node.componentId,
          };
          placedNodes.push(placed);
          nodeById.set(node.id, placed);
        });
      }

      currX += zs.w + ZONE_GAP;
    });

    currY += rh + ROW_GAP;
  });

  return { zones, nodes: placedNodes, nodeById };
}

function rayBoxBorderPoint(cx, cy, hw, hh, ox, oy, dirX, dirY) {
  let tx = Number.POSITIVE_INFINITY;
  let ty = Number.POSITIVE_INFINITY;
  if (dirX > 1e-6) tx = (cx + hw - ox) / dirX;
  else if (dirX < -1e-6) tx = (cx - hw - ox) / dirX;
  if (dirY > 1e-6) ty = (cy + hh - oy) / dirY;
  else if (dirY < -1e-6) ty = (cy - hh - oy) / dirY;
  const t = Math.min(tx, ty);
  if (!Number.isFinite(t) || t < 0) return [ox, oy];
  return [ox + dirX * t, oy + dirY * t];
}

function pointInsidePaddedBox(x, y, node, pad) {
  return (
    x >= node.cx - (node.w / 2 + pad) &&
    x <= node.cx + (node.w / 2 + pad) &&
    y >= node.cy - (node.h / 2 + pad) &&
    y <= node.cy + (node.h / 2 + pad)
  );
}

function computeAnchors(fromNode, toNode, pad = 3, lateralShift = 0, ctrl = null) {
  const dx = toNode.cx - fromNode.cx;
  const dy = toNode.cy - fromNode.cy;
  const dist = Math.hypot(dx, dy);
  if (dist < 1) {
    return { from: [fromNode.cx, fromNode.cy], to: [toNode.cx, toNode.cy] };
  }
  const ux = dx / dist;
  const uy = dy / dist;
  const nx = -uy;
  const ny = ux;

  const fromHw = fromNode.w / 2 + pad;
  const fromHh = fromNode.h / 2 + pad;
  const toHw = toNode.w / 2 + pad;
  const toHh = toNode.h / 2 + pad;

  const maxShiftX =
    Math.abs(nx) > 1e-6 ? (Math.min(fromHw, toHw) - 8) / Math.abs(nx) : Number.POSITIVE_INFINITY;
  const maxShiftY =
    Math.abs(ny) > 1e-6 ? (Math.min(fromHh, toHh) - 8) / Math.abs(ny) : Number.POSITIVE_INFINITY;
  const maxShift = Math.max(0, Math.min(maxShiftX, maxShiftY));
  const safeShift = clamp(lateralShift, -maxShift, maxShift);

  const sfX = fromNode.cx + nx * safeShift;
  const sfY = fromNode.cy + ny * safeShift;
  const stX = toNode.cx + nx * safeShift;
  const stY = toNode.cy + ny * safeShift;

  const useCtrl =
    ctrl &&
    !pointInsidePaddedBox(ctrl[0], ctrl[1], fromNode, pad) &&
    !pointInsidePaddedBox(ctrl[0], ctrl[1], toNode, pad);

  const p1 = useCtrl
    ? rayBoxBorderPoint(fromNode.cx, fromNode.cy, fromHw, fromHh, sfX, sfY, ctrl[0] - sfX, ctrl[1] - sfY)
    : rayBoxBorderPoint(fromNode.cx, fromNode.cy, fromHw, fromHh, sfX, sfY, ux, uy);
  const p2 = useCtrl
    ? rayBoxBorderPoint(toNode.cx, toNode.cy, toHw, toHh, stX, stY, ctrl[0] - stX, ctrl[1] - stY)
    : rayBoxBorderPoint(toNode.cx, toNode.cy, toHw, toHh, stX, stY, -ux, -uy);

  return { from: p1, to: p2 };
}

function makeQuadCurve(p1, p2, ctrl) {
  const [x1, y1] = p1;
  const [x2, y2] = p2;
  const [cx, cy] = ctrl;
  const sample = t => {
    const u = clamp(t, 0, 1);
    const inv = 1 - u;
    return [
      inv * inv * x1 + 2 * inv * u * cx + u * u * x2,
      inv * inv * y1 + 2 * inv * u * cy + u * u * y2,
    ];
  };
  return {
    path: `M ${num(x1)} ${num(y1)} Q ${num(cx)} ${num(cy)} ${num(x2)} ${num(y2)}`,
    sample,
  };
}

function buildSelfLoopCurve(node) {
  // Shift slightly left of the top-right FOCUS badge so they never collide.
  const x1 = node.cx - 26;
  const x2 = node.cx + 12;
  const y = node.y - 2;
  const ctrl = [(x1 + x2) / 2, node.y - 46];
  return makeQuadCurve([x1, y], [x2, y], ctrl);
}

function pointHitsNode(x, y, nodes, clearance, ignoreIds = null) {
  for (const n of nodes) {
    if (ignoreIds && ignoreIds.has(n.id)) continue;
    if (
      x >= n.x - clearance &&
      x <= n.x + n.w + clearance &&
      y >= n.y - clearance &&
      y <= n.y + n.h + clearance
    ) {
      return true;
    }
    if (n.isFocal) {
      // Protect the top-right FOCUS micro-badge on focal cards.
      const bx = n.x + n.w - 44;
      const by = n.y - 10;
      if (x >= bx - clearance && x <= bx + 42 + clearance && y >= by - clearance && y <= by + 16 + clearance) {
        return true;
      }
    }
  }
  return false;
}

function pointHitsZoneHeader(x, y, zones, clearance = 6) {
  for (const z of zones) {
    const labelEndX = Math.min(z.x + z.w, z.x + 172);
    if (
      x >= z.x - clearance &&
      x <= labelEndX + clearance &&
      y >= z.y - clearance &&
      y <= z.y + z.headerH + clearance
    ) {
      return true;
    }
  }
  return false;
}

function buildRouteCurve(fromNode, toNode, hasReverse, allNodes, zones) {
  if (fromNode.id === toNode.id) {
    return buildSelfLoopCurve(fromNode);
  }
  const pad = 3;
  const lateralShift = hasReverse ? 8.5 : 0;
  const baseAnchors = computeAnchors(fromNode, toNode, pad, lateralShift);
  const [bx1, by1] = baseAnchors.from;
  const [bx2, by2] = baseAnchors.to;
  const dx = bx2 - bx1;
  const dy = by2 - by1;
  const dist = Math.hypot(dx, dy);
  const nx = dist > 0.01 ? -dy / dist : 0;
  const ny = dist > 0.01 ? dx / dist : -1;
  const mx = (bx1 + bx2) / 2;
  const my = (by1 + by2) / 2;

  const ignore = new Set([fromNode.id, toNode.id]);
  let skipsInline = false;
  if (Math.abs(fromNode.cy - toNode.cy) < 20) {
    const minX = Math.min(fromNode.cx, toNode.cx);
    const maxX = Math.max(fromNode.cx, toNode.cx);
    skipsInline = allNodes.some(
      n => !ignore.has(n.id) && Math.abs(n.cy - fromNode.cy) < 28 && n.cx > minX + 16 && n.cx < maxX - 16,
    );
  }

  const baseCurv = hasReverse ? 0.21 : skipsInline ? 0.28 : 0.14;
  const baseOffset = clamp(dist * baseCurv, skipsInline ? 52 : 20, 92);

  const offsets = hasReverse
    ? [
        baseOffset,
        baseOffset * 0.55,
        baseOffset * 0.28,
        baseOffset * 1.45,
        baseOffset * 1.9,
        125,
        165,
        215,
        265,
      ]
    : skipsInline
      ? [-baseOffset, baseOffset, -baseOffset * 1.35, baseOffset * 1.35, -140, 140]
      : [
          baseOffset,
          -baseOffset,
          baseOffset * 0.4,
          -baseOffset * 0.4,
          baseOffset * 1.45,
          -baseOffset * 1.45,
          125,
          -125,
          165,
          -165,
          215,
          -215,
          265,
          -265,
        ];

  // Build candidate control points including midpoint biases away from host-zone header text.
  const ctrlCandidates = [];
  const biases = [
    [0, 0],
    [32, 16],
    [-36, 16],
    [56, 22],
    [-64, 18],
    [32, -16],
    [-36, -16],
    [-84, 24],
    [84, -20],
  ];
  for (const offset of offsets) {
    const cx = mx + nx * offset;
    const cy = my + ny * offset;
    for (const [bx, by] of biases) {
      ctrlCandidates.push([cx + bx, cy + by]);
    }
  }

  const endpointNodes = [fromNode, toNode];
  let bestCurve = null;
  let bestScore = Number.POSITIVE_INFINITY;

  for (const [cx, cy] of ctrlCandidates) {
    const anchors = computeAnchors(fromNode, toNode, pad, lateralShift, [cx, cy]);
    const curve = makeQuadCurve(anchors.from, anchors.to, [cx, cy]);
    let penalty = 0;
    for (let step = 1; step <= 19; step++) {
      const t = step / 20;
      const [px, py] = curve.sample(t);
      if (pointHitsNode(px, py, allNodes, 6, ignore)) penalty += 15;
      if (pointHitsNode(px, py, endpointNodes, 0.5)) penalty += 25;
      if (pointHitsZoneHeader(px, py, zones, 5)) penalty += 12;
      if (
        px < STAGE_X + 10 ||
        px > STAGE_X + STAGE_W - 10 ||
        py < STAGE_Y + 14 ||
        py > STAGE_Y + STAGE_H - 8
      ) {
        penalty += 6;
      }
    }
    const biasPenalty = Math.hypot(cx - mx, cy - my) * 0.002;
    const score = penalty + biasPenalty;
    if (score < bestScore) {
      bestScore = score;
      bestCurve = curve;
      if (penalty === 0 && biasPenalty < 0.08) break;
    }
  }

  return bestCurve;
}

function sampleCurveWithNormal(curve, t) {
  const [x, y] = curve.sample(t);
  const dt = 0.025;
  const [x0, y0] = curve.sample(Math.max(0, t - dt));
  const [x1, y1] = curve.sample(Math.min(1, t + dt));
  const dx = x1 - x0;
  const dy = y1 - y0;
  const len = Math.hypot(dx, dy);
  if (len < 1e-4) return { x, y, nx: 0, ny: -1 };
  return { x, y, nx: -dy / len, ny: dx / len };
}

function placeHopBadges(routes, hops, nodes, zones) {
  const minX = STAGE_X + 14;
  const maxX = STAGE_X + STAGE_W - 14;
  const minY = STAGE_Y + 12;
  const maxY = STAGE_Y + STAGE_H - 12;
  const minDist = 23;
  const minDistSq = minDist * minDist;
  const nodeClearance = 13;

  const raw = [];
  for (const route of routes) {
    const count = route.hopIndices.length;
    route.hopIndices.forEach((hopIdx, posIdx) => {
      const defaultT = count === 1 ? 0.5 : clamp(0.28 + (0.44 * posIdx) / (count - 1), 0.25, 0.75);
      const [bx, by] = route.curve.sample(defaultT);
      raw.push({
        hopIdx,
        hop: hops[hopIdx],
        curve: route.curve,
        defaultT,
        baseX: bx,
        baseY: by,
        x: clamp(bx, minX, maxX),
        y: clamp(by, minY, maxY),
      });
    });
  }

  raw.sort((a, b) => a.hopIdx - b.hopIdx);

  const isClearOfPlaced = (x, y, placedCount) => {
    for (let k = 0; k < placedCount; k++) {
      const dx = raw[k].x - x;
      const dy = raw[k].y - y;
      if (dx * dx + dy * dy < minDistSq) return false;
    }
    return true;
  };

  const isValid = (x, y, placedCount, avoidHeaders) =>
    !pointHitsNode(x, y, nodes, nodeClearance) &&
    (!avoidHeaders || !pointHitsZoneHeader(x, y, zones, 5)) &&
    isClearOfPlaced(x, y, placedCount);

  const candidateTs = [0.5, 0.36, 0.64, 0.26, 0.74, 0.2, 0.8];
  const perpOffsets = [0, 16, -16, 26, -26, 38, -38];

  for (let i = 0; i < raw.length; i++) {
    const cur = raw[i];
    const sx = clamp(cur.baseX, minX, maxX);
    const sy = clamp(cur.baseY, minY, maxY);

    if (isValid(sx, sy, i, true)) {
      cur.x = sx;
      cur.y = sy;
      continue;
    }

    let found = false;
    let bestX = sx;
    let bestY = sy;

    for (const avoidHeaders of [true, false]) {
      let bestDistSq = Number.POSITIVE_INFINITY;
      const ts = [cur.defaultT, ...candidateTs];
      for (const t of ts) {
        const { x: px, y: py, nx, ny } = sampleCurveWithNormal(cur.curve, t);
        for (const off of perpOffsets) {
          const cx = clamp(px + nx * off, minX, maxX);
          const cy = clamp(py + ny * off, minY, maxY);
          if (!isValid(cx, cy, i, avoidHeaders)) continue;
          const d2 = (cx - cur.baseX) ** 2 + (cy - cur.baseY) ** 2;
          if (d2 < bestDistSq) {
            bestDistSq = d2;
            bestX = cx;
            bestY = cy;
            found = true;
          }
        }
      }
      if (found) break;
    }

    if (!found) {
      for (let r = minDist; r <= 240 && !found; r += 6) {
        let bestDistSq = Number.POSITIVE_INFINITY;
        for (let s = 0; s < 36; s++) {
          const ang = (s / 36) * Math.PI * 2;
          const cx = clamp(cur.baseX + Math.cos(ang) * r, minX, maxX);
          const cy = clamp(cur.baseY + Math.sin(ang) * r, minY, maxY);
          if (!isValid(cx, cy, i, false)) continue;
          const d2 = (cx - cur.baseX) ** 2 + (cy - cur.baseY) ** 2;
          if (d2 < bestDistSq) {
            bestDistSq = d2;
            bestX = cx;
            bestY = cy;
            found = true;
          }
        }
      }
    }

    cur.x = bestX;
    cur.y = bestY;
  }

  return raw;
}

function buildPacketKeyframes(hops, routeByHopIdx) {
  if (!hops.length) return '';
  const slice = 100 / hops.length;
  const frames = [];
  const firstStart = routeByHopIdx.get(0).curve.sample(0);
  const firstColor = hopStyle(hops[0]).color;

  frames.push(
    `  0% { transform: translate(${num(firstStart[0])}px, ${num(firstStart[1])}px); color: ${firstColor}; opacity: 0; }`,
  );

  for (let i = 0; i < hops.length; i++) {
    const hop = hops[i];
    const nextHop = hops[(i + 1) % hops.length];
    const curve = routeByHopIdx.get(i).curve;
    const nextCurve = routeByHopIdx.get((i + 1) % hops.length).curve;
    const color = hopStyle(hop).color;
    const nextColor = hopStyle(nextHop).color;

    const p0 = curve.sample(0);
    const p1 = curve.sample(0.33);
    const p2 = curve.sample(0.67);
    const p3 = curve.sample(1);
    const nextP0 = nextCurve.sample(0);

    const base = i * slice;
    const t0 = (base + slice * 0.04).toFixed(2);
    const t1 = (base + slice * 0.31).toFixed(2);
    const t2 = (base + slice * 0.58).toFixed(2);
    const t3 = (base + slice * 0.85).toFixed(2);
    const tFade = (base + slice * 0.92).toFixed(2);
    const tPrep = Math.min(99.6, base + slice * 0.96).toFixed(2);

    frames.push(
      `  ${t0}% { transform: translate(${num(p0[0])}px, ${num(p0[1])}px); color: ${color}; opacity: 1; }`,
      `  ${t1}% { transform: translate(${num(p1[0])}px, ${num(p1[1])}px); color: ${color}; opacity: 1; }`,
      `  ${t2}% { transform: translate(${num(p2[0])}px, ${num(p2[1])}px); color: ${color}; opacity: 1; }`,
      `  ${t3}% { transform: translate(${num(p3[0])}px, ${num(p3[1])}px); color: ${color}; opacity: 1; }`,
      `  ${tFade}% { transform: translate(${num(p3[0])}px, ${num(p3[1])}px); color: ${color}; opacity: 0; }`,
    );
    if (i < hops.length - 1) {
      frames.push(
        `  ${tPrep}% { transform: translate(${num(nextP0[0])}px, ${num(nextP0[1])}px); color: ${nextColor}; opacity: 0; }`,
      );
    }
  }

  frames.push(
    `  100% { transform: translate(${num(firstStart[0])}px, ${num(firstStart[1])}px); color: ${firstColor}; opacity: 0; }`,
  );

  return `@keyframes packet-travel {\n${frames.join('\n')}\n}`;
}

function formatTraceLine(hop, nodeById) {
  const fromLabel = nodeById.get(hop.from)?.shortLabel ?? hop.from;
  const toLabel = nodeById.get(hop.to)?.shortLabel ?? hop.to;
  const routeText = hop.from === hop.to ? `${fromLabel} (self)` : `${fromLabel} -> ${toLabel}`;
  return {
    routeText,
    detailText: String(hop.label ?? ''),
    portText: String(hop.port ?? ''),
  };
}

function renderSvg(spec) {
  const layout = computeLayout(spec);
  const { zones, nodes, nodeById } = layout;

  // Group hops into directed routes; split if distinct channels share a pair.
  const pairSet = new Set(spec.hops.map(h => `${h.from}::${h.to}`));
  const routeMap = new Map();
  spec.hops.forEach((hop, idx) => {
    const key = `${hop.from}::${hop.to}::${hopChannelKey(hop)}`;
    const existing = routeMap.get(key);
    if (existing) {
      existing.hopIndices.push(idx);
    } else {
      routeMap.set(key, {
        key,
        from: hop.from,
        to: hop.to,
        channelKey: hopChannelKey(hop),
        style: hopStyle(hop),
        hopIndices: [idx],
      });
    }
  });

  const routes = [];
  const routeByHopIdx = new Map();
  for (const route of routeMap.values()) {
    const fromNode = nodeById.get(route.from);
    const toNode = nodeById.get(route.to);
    if (!fromNode || !toNode) {
      throw new Error(`${spec.slug}: hop references unknown node (${route.from} -> ${route.to}).`);
    }
    const isSelf = route.from === route.to;
    const hasReverse = !isSelf && pairSet.has(`${route.to}::${route.from}`);
    const curve = buildRouteCurve(fromNode, toNode, hasReverse, nodes, zones);
    const pEnd = curve.sample(1);
    const pPrev = curve.sample(0.94);
    const arrowAngle = (Math.atan2(pEnd[1] - pPrev[1], pEnd[0] - pPrev[0]) * 180) / Math.PI;
    const fullRoute = { ...route, isSelf, hasReverse, curve, arrowEnd: pEnd, arrowAngle };
    routes.push(fullRoute);
    for (const idx of route.hopIndices) routeByHopIdx.set(idx, fullRoute);
  }

  const badges = placeHopBadges(routes, spec.hops, nodes, zones);
  const keyframesCss = buildPacketKeyframes(spec.hops, routeByHopIdx);

  // Active channel legend items in header
  const usedChannels = CHANNEL_ORDER.filter(ch =>
    spec.hops.some(h => hopChannelKey(h) === ch),
  );
  const legendItems = [];
  let lx = CANVAS_W - 16;
  for (let i = usedChannels.length - 1; i >= 0; i--) {
    const ch = usedChannels[i];
    const st = CHANNEL_STYLES[ch];
    const labelW = approxTextWidth(st.label, 8.5, true);
    const itemW = 14 + labelW + 12;
    lx -= itemW;
    legendItems.unshift({ ch, st, x: lx, labelW });
  }
  const titleMaxPx = Math.max(300, (legendItems.length ? legendItems[0].x : CANVAS_W - 24) - 44);
  const titleText = truncatePx(spec.title, titleMaxPx, 13.5, false);
  const subtitleText = truncatePx(spec.subtitle, titleMaxPx, 9.5, false);

  const legendSvg = legendItems
    .map(({ st, x }) => {
      const lineDash = st.dash ? ` stroke-dasharray="${st.dash}"` : '';
      return [
        `    <line x1="${num(x)}" y1="24" x2="${num(x + 11)}" y2="24" stroke="${st.color}" stroke-width="2"${lineDash} stroke-linecap="round"/>`,
        `    <circle cx="${num(x + 11)}" cy="24" r="2.2" fill="${st.color}"/>`,
        `    <text class="f-mono" x="${num(x + 17)}" y="27" fill="#93a4c2" font-size="8.5" font-weight="600">${xml(st.label)}</text>`,
      ].join('\n');
    })
    .join('\n');

  const zonesSvg = zones
    .map(z => {
      const label = truncatePx(z.label, z.w - 30, 9.2, false);
      return [
        `  <g class="host-zone" data-zone="${xml(z.id)}">`,
        `    <rect x="${num(z.x)}" y="${num(z.y)}" width="${num(z.w)}" height="${num(z.h)}" rx="7" fill="#101727" stroke="#283349" stroke-width="1"/>`,
        `    <line x1="${num(z.x)}" y1="${num(z.y + z.headerH)}" x2="${num(z.x + z.w)}" y2="${num(z.y + z.headerH)}" stroke="#212d45" stroke-width="1"/>`,
        `    <circle cx="${num(z.x + 11)}" cy="${num(z.y + z.headerH / 2)}" r="2.4" fill="#7aa2ff" fill-opacity="0.85"/>`,
        `    <text class="f-sans" x="${num(z.x + 19)}" y="${num(z.y + z.headerH / 2 + 3.2)}" fill="#8998b4" font-size="9.2" font-weight="700" letter-spacing="0.09em">${xml(label)}</text>`,
        '  </g>',
      ].join('\n');
    })
    .join('\n');

  const routesSvg = routes
    .map(r => {
      const dashAttr = r.style.dash ? ` stroke-dasharray="${r.style.dash}"` : '';
      return [
        `  <g class="topo-edge" data-from="${xml(r.from)}" data-to="${xml(r.to)}">`,
        `    <path d="${r.curve.path}" fill="none" stroke="${r.style.color}" stroke-width="1.8" stroke-opacity="0.88"${dashAttr} stroke-linecap="round"/>`,
        `    <polygon points="-8,-3.4 0,0 -8,3.4" fill="${r.style.color}" transform="translate(${num(r.arrowEnd[0])}, ${num(r.arrowEnd[1])}) rotate(${num(r.arrowAngle)})"/>`,
        '  </g>',
      ].join('\n');
    })
    .join('\n');

  const nodesSvg = nodes
    .map(n => {
      const accent = subsystemAccent(n.subsystem);
      const shortText = truncatePx(n.shortLabel, n.w - 18, 11.5, false);
      const hostText = truncatePx(n.hostLabel, n.w - 18, 9, true);
      const focalGlow = n.isFocal
        ? `    <rect x="${num(n.x - 3)}" y="${num(n.y - 3)}" width="${num(n.w + 6)}" height="${num(n.h + 6)}" rx="8" fill="none" stroke="#7aa2ff" stroke-opacity="0.28" stroke-width="2.5"/>\n`
        : '';
      const focalBadge = n.isFocal
        ? [
            `    <g transform="translate(${num(n.x + n.w - 42)}, ${num(n.y - 7)})">`,
            '      <rect x="0" y="0" width="38" height="13" rx="3" fill="#7aa2ff"/>',
            '      <text class="f-mono" x="19" y="9.4" text-anchor="middle" fill="#0b1020" font-size="7.8" font-weight="700" letter-spacing="0.06em">FOCUS</text>',
            '    </g>',
          ].join('\n') + '\n'
        : '';
      return [
        `  <g class="topo-node" data-id="${xml(n.id)}">`,
        focalGlow +
          `    <rect x="${num(n.x)}" y="${num(n.y)}" width="${num(n.w)}" height="${num(n.h)}" rx="6" fill="#141e32" stroke="${n.isFocal ? '#7aa2ff' : '#2e3d59'}" stroke-width="${n.isFocal ? '1.8' : '1.15'}"/>`,
        `    <rect x="${num(n.x + 6)}" y="${num(n.y + 1)}" width="${num(n.w - 12)}" height="3" rx="1.5" fill="${accent}"/>`,
        `    <text class="f-sans" x="${num(n.cx)}" y="${num(n.y + 23)}" text-anchor="middle" fill="#d0d9ec" font-size="11.5" font-weight="700">${xml(shortText)}</text>`,
        `    <text class="f-mono" x="${num(n.cx)}" y="${num(n.y + 37)}" text-anchor="middle" fill="#8998b4" font-size="9">${xml(hostText)}</text>`,
        focalBadge + '  </g>',
      ].join('\n');
    })
    .join('\n');

  const badgesSvg = badges
    .map(b => {
      const color = hopStyle(b.hop).color;
      return [
        `  <g class="hop-badge" data-hop="${b.hop.index}">`,
        `    <circle cx="${num(b.x)}" cy="${num(b.y)}" r="9" fill="#0d1526" stroke="${color}" stroke-width="1.6"/>`,
        `    <text class="f-mono" x="${num(b.x)}" y="${num(b.y + 3.2)}" text-anchor="middle" fill="${color}" font-size="9.5" font-weight="700">${b.hop.index}</text>`,
        '  </g>',
      ].join('\n');
    })
    .join('\n');

  // Bottom wire-trace strip (2 columns, crisp 2-line rows, zero truncation)
  const rowsPerCol = Math.max(1, Math.ceil(spec.hops.length / 2));
  const traceBodyTop = LEGEND_Y + 24;
  const traceBodyH = LEGEND_H - 28;
  const rowStep = traceBodyH / Math.max(rowsPerCol, 3);
  const colW = (LEGEND_W - 24) / 2;

  const traceRowsSvg = spec.hops
    .map((hop, idx) => {
      const col = idx < rowsPerCol ? 0 : 1;
      const row = col === 0 ? idx : idx - rowsPerCol;
      const colLeft = LEGEND_X + 10 + col * (colW + 10);
      const rowTop = traceBodyTop + row * rowStep;
      const rowCenterY = rowTop + rowStep / 2;
      const color = hopStyle(hop).color;
      const { routeText, detailText, portText } = formatTraceLine(hop, nodeById);
      const divider =
        row > 0
          ? `    <line x1="${num(colLeft)}" y1="${num(rowTop)}" x2="${num(colLeft + colW - 4)}" y2="${num(rowTop)}" stroke="#182236" stroke-width="0.75"/>\n`
          : '';
      const portEl = portText
        ? `\n    <text class="f-mono" x="${num(colLeft + colW - 6)}" y="${num(rowCenterY - 2.5)}" text-anchor="end" fill="#6f80a0" font-size="8.2">${xml(portText)}</text>`
        : '';
      return [
        `  <g class="trace-row" data-hop="${hop.index}">`,
        `${divider}    <rect x="${num(colLeft)}" y="${num(rowCenterY - 8)}" width="18" height="16" rx="3.5" fill="#141e32" stroke="${color}" stroke-width="1.15"/>`,
        `    <text class="f-mono" x="${num(colLeft + 9)}" y="${num(rowCenterY + 3)}" text-anchor="middle" fill="${color}" font-size="8.5" font-weight="700">${hop.index}</text>`,
        `    <text class="f-mono" x="${num(colLeft + 24)}" y="${num(rowCenterY - 2.5)}" fill="#d0d9ec" font-size="8.5" font-weight="600">${xml(routeText)}</text>${portEl}`,
        `    <text class="f-mono" x="${num(colLeft + 24)}" y="${num(rowCenterY + 9.5)}" fill="#93a4c2" font-size="8.5">${xml(detailText)}</text>`,
        '  </g>',
      ].join('\n');
    })
    .join('\n');

  const descHops = spec.hops
    .map(h => {
      const f = nodeById.get(h.from)?.shortLabel ?? h.from;
      const t = nodeById.get(h.to)?.shortLabel ?? h.to;
      return `${h.index}. ${f} to ${t}: ${h.label} (${h.port})`;
    })
    .join('; ');
  const descText = `${spec.subtitle}. ${descHops}`;

  return `<svg xmlns="http://www.w3.org/2000/svg" width="${CANVAS_W}" height="${CANVAS_H}" viewBox="0 0 ${CANVAS_W} ${CANVAS_H}" role="img" aria-labelledby="topo-title" aria-describedby="topo-desc">
<title id="topo-title">${xml(spec.title)}</title>
<desc id="topo-desc">${xml(descText)}</desc>
<defs>
  <pattern id="bp-grid" width="20" height="20" patternUnits="userSpaceOnUse">
    <circle cx="10" cy="10" r="0.9" fill="#1a253b"/>
  </pattern>
</defs>
<style>
.f-sans { font-family: ${SANS_FONT}; }
.f-mono { font-family: ${MONO_FONT}; }
${keyframesCss}
#packet-orb {
  opacity: 0;
  animation: packet-travel 9s linear infinite;
}
@media (prefers-reduced-motion: reduce) {
  #packet-orb { display: none; animation: none; }
}
</style>
<rect x="0" y="0" width="${CANVAS_W}" height="${CANVAS_H}" rx="10" fill="#0b1020"/>
<rect x="1" y="1" width="${CANVAS_W - 2}" height="${CANVAS_H - 2}" rx="9" fill="url(#bp-grid)"/>
<rect x="1" y="1" width="${CANVAS_W - 2}" height="${CANVAS_H - 2}" rx="9" fill="none" stroke="#283349" stroke-width="1.5"/>
<g stroke="#344666" stroke-width="1" fill="none" opacity="0.75">
  <path d="M 8 18 V 8 H 18"/>
  <path d="M ${CANVAS_W - 18} 8 H ${CANVAS_W - 8} V 18"/>
  <path d="M 8 ${CANVAS_H - 18} V ${CANVAS_H - 8} H 18"/>
  <path d="M ${CANVAS_W - 18} ${CANVAS_H - 8} H ${CANVAS_W - 8} V ${CANVAS_H - 18}"/>
</g>
<g class="header-band">
  <path d="M 10 2 H ${CANVAS_W - 10} A 8 8 0 0 1 ${CANVAS_W - 2} 10 V 48 H 2 V 10 A 8 8 0 0 1 10 2 Z" fill="#0e1526"/>
  <line x1="2" y1="48" x2="${CANVAS_W - 2}" y2="48" stroke="#283349" stroke-width="1"/>
  <circle cx="19" cy="20" r="3.4" fill="#7aa2ff"/>
  <text class="f-sans" x="30" y="24" fill="#e2e8f5" font-size="13.5" font-weight="700">${xml(titleText)}</text>
  <text class="f-sans" x="30" y="39" fill="#8998b4" font-size="9.5">${xml(subtitleText)}</text>
  <g class="channel-legend">
${legendSvg}
  </g>
</g>
<g class="topology-stage">
${zonesSvg}
${routesSvg}
${nodesSvg}
${badgesSvg}
  <g id="packet-orb" aria-hidden="true">
    <circle cx="0" cy="0" r="11" fill="currentColor" fill-opacity="0.22"/>
    <circle cx="0" cy="0" r="6.2" fill="currentColor" fill-opacity="0.48"/>
    <circle cx="0" cy="0" r="3.4" fill="#ffffff" stroke="currentColor" stroke-width="1.5"/>
  </g>
</g>
<g class="wire-trace-strip">
  <rect x="${LEGEND_X}" y="${LEGEND_Y}" width="${LEGEND_W}" height="${LEGEND_H}" rx="7" fill="#101727" stroke="#283349" stroke-width="1"/>
  <line x1="${LEGEND_X}" y1="${LEGEND_Y + 21}" x2="${LEGEND_X + LEGEND_W}" y2="${LEGEND_Y + 21}" stroke="#212d45" stroke-width="1"/>
  <text class="f-sans" x="${LEGEND_X + 12}" y="${LEGEND_Y + 14}" fill="#8998b4" font-size="8.8" font-weight="700" letter-spacing="0.1em">WIRE TRACE</text>
  <text class="f-mono" x="${LEGEND_X + LEGEND_W - 12}" y="${LEGEND_Y + 14}" text-anchor="end" fill="#6f80a0" font-size="8.5">${spec.hops.length} HOPS \u00B7 ${nodes.length} NODES</text>
  <line x1="${LEGEND_X + LEGEND_W / 2}" y1="${LEGEND_Y + 25}" x2="${LEGEND_X + LEGEND_W / 2}" y2="${LEGEND_Y + LEGEND_H - 6}" stroke="#1e2940" stroke-width="1"/>
${traceRowsSvg}
</g>
</svg>
`;
}

function validateSpec(key, spec) {
  if (!spec || typeof spec !== 'object') {
    throw new Error(`topology-specs.json["${key}"]: expected an object.`);
  }
  if (spec.slug !== key) {
    throw new Error(`topology-specs.json["${key}"]: slug "${spec.slug}" does not match key.`);
  }
  if (!Array.isArray(spec.nodes) || spec.nodes.length === 0) {
    throw new Error(`topology-specs.json["${key}"]: expected non-empty nodes array.`);
  }
  if (!Array.isArray(spec.hops) || spec.hops.length === 0) {
    throw new Error(`topology-specs.json["${key}"]: expected non-empty hops array.`);
  }
  const nodeById = new Map();
  for (const node of spec.nodes) {
    if (!HOST_ZONE_LABELS[node.hostZone]) {
      throw new Error(`topology-specs.json["${key}"]: unknown hostZone "${node.hostZone}".`);
    }
    nodeById.set(node.id, node);
  }
  for (const hop of spec.hops) {
    const fromNode = nodeById.get(hop.from);
    const toNode = nodeById.get(hop.to);
    if (!fromNode || !toNode) {
      throw new Error(
        `topology-specs.json["${key}"]: hop #${hop.index} references unknown node (${hop.from} -> ${hop.to}).`,
      );
    }
    if (!CHANNEL_STYLES[hop.channel]) {
      throw new Error(`topology-specs.json["${key}"]: unknown channel "${hop.channel}".`);
    }
    if (hop.kind === 'local' && fromNode.hostZone !== toNode.hostZone) {
      throw new Error(
        `topology-specs.json["${key}"]: hop #${hop.index} has kind="local" across different hostZones (${hop.from} [${fromNode.hostZone}] -> ${hop.to} [${toNode.hostZone}]).`,
      );
    }
  }
}

function run(args) {
  if (args.length === 1 && ['--help', '-h'].includes(args[0])) {
    console.log(
      `${USAGE}\nGenerate all Blueprint Spatial Topology SVGs from topology-specs.json.\n` +
        'Pass --check to verify every SVG is present, current, and free of orphans without writing.',
    );
    return;
  }
  const check = args.length === 1 && args[0] === '--check';
  if (args.length && !check) throw new Error(USAGE);

  const raw = readFileSync(specsPath, 'utf8');
  const specs = JSON.parse(raw);
  const entries = Object.entries(specs);
  if (!entries.length) throw new Error(`No topology entries found in ${specsPath}.`);

  const expected = new Map();
  const errors = [];
  for (const [key, spec] of entries) {
    try {
      validateSpec(key, spec);
      const fileName = `${spec.slug}.svg`;
      expected.set(fileName, renderSvg(spec));
    } catch (error) {
      errors.push(error.message);
    }
  }
  if (errors.length) throw new Error(errors.join('\n'));

  const present = new Set();
  const changed = [];
  for (const entry of readdirSync(visualsDir, { withFileTypes: true })) {
    if (!/\.svg$/iu.test(entry.name)) continue;
    if (!expected.has(entry.name)) {
      errors.push(
        `visuals/${entry.name}: orphaned SVG; not present in topology-specs.json.`,
      );
    } else if (!entry.isFile()) {
      errors.push(`visuals/${entry.name}: expected a regular SVG file.`);
    } else {
      present.add(entry.name);
    }
  }

  for (const [name, svg] of expected) {
    const existing = present.has(name) ? readFileSync(join(visualsDir, name), 'utf8') : null;
    if (existing?.replace(/\r\n/gu, '\n') === svg) continue;
    changed.push(name);
    if (check) {
      errors.push(`visuals/${name}: ${existing === null ? 'missing' : 'stale'}.`);
    }
  }

  if (check && changed.length) {
    errors.push('Run node render-flows.mjs without --check to rebuild.');
  }
  if (errors.length) throw new Error(errors.join('\n'));

  if (check) {
    console.log(`Checked ${expected.size} topology SVGs; all are current.`);
  } else {
    for (const name of changed) {
      writeFileSync(join(visualsDir, name), expected.get(name), 'utf8');
    }
    console.log(`Built ${expected.size} topology SVGs (${changed.length} updated).`);
  }
}

try {
  run(process.argv.slice(2));
} catch (error) {
  console.error(`render-flows: ${error.message}`);
  process.exitCode = 1;
}
