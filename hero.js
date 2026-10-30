/*
 * hero.js: a "simulated trace" console for <canvas id="hero-viz">.
 * It cycles through four systems from the resume, one at a time. Each shows a service map with live
 * packets and a span waterfall:
 *   sephora  · email-personalization  Kafka, Cosmos DB, Drools, Azure Batch, Contentful, Epsilon
 *   express  · retail-search          search API, Redis cache, Google Retail Search, product feed
 *   paypal   · authn/authz            gateway, Cassandra tokens, Redis sessions across DCs, breaker
 *   commerce · product-sync           storefront, Commercetools, subscriptions, product sync
 * Every number is simulated. amber = request/API, sage = event/pipeline, rose = data/stores/security.
 * Plain ES2017, no dependencies, decorative (the canvas is aria-hidden).
 */
(() => {
  'use strict';

  const canvas = document.getElementById('hero-viz');
  const ctx = canvas && canvas.getContext ? canvas.getContext('2d') : null;
  const wrap = canvas ? canvas.parentElement : null;
  if (!ctx || !wrap) return;

  // Always animate, by owner's request, even when the OS asks for reduced motion.
  const REDUCED = false;
  const TAU = Math.PI * 2;
  const FONT = 'px "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, monospace';
  const REQ = 0, EVT = 1, DATA = 2;
  const BRANDS = ['sephora', 'uppababy', 'express', 'wild-fork'];

  /* ---------- the four flows ----------
   * node: [label, short label (narrow), sub, column 0-4, row 0-2, type, kind, states?]
   * edge: [from, to, label, kind, label position 0-1 along the curve]
   * step: one span. path = edges the packet travels; round = request and response; ms = simulated timing.
   */
  const FLOWS = [
    {
      tab: 'sephora', title: 'sephora · email-personalization',
      nodes: {
        cdb: ['cosmos-db', 'cosmos-db', 'customer-profiles', 0, 0, 'store', DATA],
        rules: ['rules-engine', 'drools', 'drools', 0, 2, 'svc', EVT],
        t1: ['cosmos-db.updates', 'kafka', '', 1, 0, 'topic', EVT],
        t2: ['ruleset.updates', 'kafka', '', 1, 2, 'topic', EVT],
        batch: ['azure-batch', 'azure-batch', '', 2, 1, 'batch', EVT],
        cms: ['contentful', 'contentful', 'email layouts', 3, 0, 'ext', DATA],
        csv: ['csv-generator', 'csv', 'part-{task}.csv', 3, 1, 'svc', EVT],
        eps: ['epsilon', 'epsilon', 'email delivery', 4, 1, 'ext', REQ],
      },
      edges: {
        e1: ['cdb', 't1', 'change feed', EVT],
        e2: ['t1', 'batch', '', EVT],
        e3: ['rules', 't2', 'ruleset v142', EVT],
        e4: ['t2', 'batch', '', EVT],
        e5: ['batch', 'cms', 'GET /entries', REQ, 0.35],
        e6: ['batch', 'csv', 'render rows', EVT],
        e7: ['csv', 'eps', 'upload part-{task}.csv', REQ],
      },
      root: { name: 'batch.task email-{job}/{task}', path: [], k: EVT },
      steps: [
        { name: 'kafka.consume cosmos-db.updates', path: ['e1', 'e2'], k: EVT, ms: [0, 38], d: 1 },
        { name: 'kafka.consume ruleset.updates', path: ['e3', 'e4'], k: EVT, ms: [6, 41], d: 1 },
        { name: 'drools.evaluate eligibility', at: 'batch', k: EVT, ms: [44, 212], d: 1 },
        { name: 'contentful GET /entries', path: ['e5'], round: true, k: REQ, ms: [215, 302], d: 1, chip: ['cms', '200 OK', REQ] },
        { name: 'csv.render part-{task}.csv', path: ['e6'], k: EVT, ms: [305, 1388], d: 1 },
        { name: 'epsilon upload part-{task}.csv', path: ['e7'], round: true, k: REQ, ms: [1392, 1781], d: 1, chip: ['eps', '202 ACCEPTED', REQ] },
      ],
      amb: [['e1', 2.4, 1.1], ['e3', 3.3, 2.2]],
    },
    {
      tab: 'express', title: 'express · retail-search',
      nodes: {
        sf: ['storefront', 'storefront', 'web + mobile', 0, 1, 'client', REQ],
        api: ['search-api', 'search-api', 'spring boot', 1, 1, 'svc', REQ],
        redis: ['redis', 'redis', 'query-cache', 2, 0, 'store', DATA],
        facet: ['facet-optimizer', 'facets', 'facets · ranking', 2, 2, 'svc', REQ],
        grs: ['google-retail-search', 'retail-search', 'retail api v2', 3, 1, 'ext', REQ],
        feed: ['product-feed', 'feed', 'catalog → index', 4, 2, 'svc', EVT],
      },
      edges: {
        e1: ['sf', 'api', 'GET /search?q=linen', REQ],
        e2: ['api', 'redis', 'GET q:linen', DATA, 0.42],
        e3: ['api', 'grs', 'POST …:search', REQ],
        e4: ['api', 'facet', 'rank + facets', REQ, 0.62],
        e5: ['feed', 'grs', 'products:import', EVT, 0.3],
      },
      root: { name: 'GET /search?q=linen+shirt', path: ['e1'], k: REQ, chip: ['sf', '200 OK', REQ] },
      steps: [
        { name: 'redis GET q:linen-shirt', path: ['e2'], round: true, k: DATA, ms: [3.1, 4], d: 1, chip: ['redis', 'MISS', DATA] },
        { name: 'retail-search :search', path: ['e3'], round: true, k: REQ, ms: [6, 132], d: 1, chip: ['grs', '200 OK', REQ] },
        { name: 'facet-optimizer rank', path: ['e4'], round: true, k: REQ, ms: [134, 158], d: 1 },
        { name: 'redis SET q:linen-shirt EX 300', path: ['e2'], el: 'SET EX 300', k: DATA, ms: [159, 160.4], d: 1 },
      ],
      amb: [['e5', 1.9, 0.4]],
    },
    {
      tab: 'paypal', title: 'paypal · authn/authz',
      nodes: {
        cl: ['web-login', 'login', 'checkout', 0, 1, 'client', REQ],
        gw: ['api-gateway', 'gateway', '', 1, 1, 'svc', REQ],
        authn: ['authn-service', 'authn', '', 2, 0, 'svc', DATA, [['breaker HALF_OPEN', REQ], ['breaker CLOSED', EVT]]],
        authz: ['authz-service', 'authz', 'scopes', 2, 2, 'svc', DATA],
        r1: ['redis · dc-1', 'redis-dc1', 'sessions', 3, 1, 'store', DATA],
        cass: ['cassandra', 'cassandra', 'token-store', 4, 0, 'store', DATA],
        r2: ['redis · dc-2', 'redis-dc2', 'sessions', 4, 1, 'store', DATA],
      },
      edges: {
        e1: ['cl', 'gw', 'POST /v1/oauth2/token', REQ],
        e2: ['gw', 'authn', 'POST /authenticate', REQ, 0.4],
        e3: ['authn', 'cass', 'INSERT tokens', DATA],
        e4: ['authn', 'r1', 'SET sess EX 900', DATA, 0.6],
        e5: ['r1', 'r2', 'replicate', DATA],
        e6: ['gw', 'authz', 'authorize scopes', REQ],
        e7: ['authz', 'r1', 'GET sess', DATA],
      },
      root: { name: 'POST /v1/oauth2/token', path: ['e1'], k: REQ, chip: ['cl', '200 OK', REQ] },
      steps: [
        { name: 'authn-service authenticate', path: ['e2'], k: REQ, ms: [2, 52], d: 1 },
        { name: 'cassandra INSERT tokens', path: ['e3'], round: true, k: DATA, ms: [9, 27], d: 2, flip: 'authn' },
        { name: 'redis SET sess dc-1 → dc-2', path: ['e4', 'e5'], k: DATA, ms: [29, 34], d: 2 },
        { name: 'authz-service authorize', path: ['e6'], k: REQ, ms: [55, 86], d: 1 },
        { name: 'redis GET sess', path: ['e7'], round: true, k: DATA, ms: [60, 60.9], d: 2, chip: ['r1', 'HIT', EVT] },
      ],
      amb: [],
    },
    {
      tab: 'commerce', title: 'commerce · product-sync',
      nodes: {
        sf: ['storefront', 'storefront', '{brand}', 0, 1, 'client', REQ],
        api: ['commerce-api', 'api', 'cart · checkout', 1, 1, 'svc', REQ],
        ct: ['commercetools', 'commercetools', 'carts', 2, 1, 'ext', REQ],
        sub: ['ct-subscription', 'events', '', 3, 0, 'topic', EVT],
        db: ['backend-db', 'mysql', 'mysql', 3, 2, 'store', DATA],
        sync: ['product-sync', 'product-sync', '', 4, 1, 'svc', EVT],
      },
      edges: {
        e1: ['sf', 'api', 'POST /checkout', REQ],
        e2: ['api', 'ct', 'POST /carts/{id}', REQ],
        e3: ['ct', 'sub', 'OrderCreated', EVT],
        e4: ['sub', 'sync', '', EVT],
        e5: ['sync', 'db', 'UPSERT stock', DATA],
        e6: ['sync', 'ct', 'POST /inventory/{id}', REQ],
      },
      root: { name: 'POST /checkout', path: ['e1'], k: REQ, chip: ['sf', '201 Created', REQ], after: 1 },
      steps: [
        { name: 'commercetools POST /carts/{id}', path: ['e2'], round: true, k: REQ, ms: [6, 44], d: 1, sub: 'carts', chip: ['ct', '200 OK', REQ] },
        { name: 'commercetools POST /orders', path: ['e2'], round: true, el: 'POST /orders', k: REQ, ms: [47, 118], d: 1, sub: 'orders', chip: ['ct', '201 Created', REQ] },
        { name: 'subscription OrderCreated', path: ['e3', 'e4'], k: EVT, ms: [124, 139], d: 1 },
        { name: 'product-sync UPSERT stock', path: ['e5'], round: true, k: DATA, ms: [141, 156], d: 2 },
        { name: 'commercetools POST /inventory/{id}', path: ['e6'], round: true, k: REQ, ms: [158, 183], d: 2, sub: 'inventory', chip: ['ct', '200 OK', REQ] },
      ],
      amb: [],
    },
  ];

  /* ---------- seeded rng (per-cycle jitter) and a stateless hash (per-frame flicker) ---------- */
  let seed = 1;
  const rand = () => {
    seed = (seed + 0x6d2b79f5) >>> 0;
    let t = seed;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const rr = (a, b) => a + (b - a) * rand();
  const hash = (a, b) => {
    let h = Math.imul(a | 0, 374761393) ^ Math.imul(b | 0, 668265263);
    h = Math.imul(h ^ (h >>> 13), 1274126177);
    return ((h ^ (h >>> 16)) >>> 0) / 4294967296;
  };
  const clamp = (v, a, b) => (v < a ? a : v > b ? b : v);
  const smooth = (v) => { v = clamp(v, 0, 1); return v * v * (3 - 2 * v); };
  const fmtMs = (d) => (d < 10 ? d.toFixed(1) : Math.round(d).toLocaleString('en-US')) + ' ms';
  const font = (px) => px + FONT;

  /* ---------- palette ---------- */
  const col = { ground: '', ink: '', mute: '', kind: ['', '', ''] };
  const glow = [null, null, null];
  const probe = document.createElement('canvas');
  probe.width = probe.height = 1;
  const pctx = probe.getContext('2d', { willReadFrequently: true });
  function rgbOf(c) { // resolve any CSS colour to "r,g,b" so the glow gradient can carry alpha
    pctx.clearRect(0, 0, 1, 1);
    pctx.fillStyle = '#000';
    pctx.fillStyle = c;
    pctx.fillRect(0, 0, 1, 1);
    const d = pctx.getImageData(0, 0, 1, 1).data;
    return d[0] + ',' + d[1] + ',' + d[2];
  }
  function makeGlow(rgb) { // pre-rendered soft glow, one drawImage per packet instead of shadowBlur
    const s = document.createElement('canvas');
    s.width = s.height = 64;
    const g = s.getContext('2d');
    const gr = g.createRadialGradient(32, 32, 0, 32, 32, 32);
    gr.addColorStop(0, 'rgba(' + rgb + ',.55)');
    gr.addColorStop(0.28, 'rgba(' + rgb + ',.16)');
    gr.addColorStop(1, 'rgba(' + rgb + ',0)');
    g.fillStyle = gr;
    g.fillRect(0, 0, 64, 64);
    return s;
  }
  function readPalette() {
    const cs = getComputedStyle(document.documentElement);
    const v = (name, def) => cs.getPropertyValue(name).trim() || def;
    col.ground = v('--ground', '#060505');
    col.ink = v('--ink', '#ece6dc');
    col.mute = v('--ink-mute', '#8a847a');
    col.kind[REQ] = v('--accent-amber', '#c9a26b');
    col.kind[EVT] = v('--accent-sage', '#8fa68e');
    col.kind[DATA] = v('--accent-rose', '#b58a8a');
    for (let k = 0; k < 3; k++) glow[k] = makeGlow(rgbOf(col.kind[k]));
  }

  /* ---------- runtime graph, built once ---------- */
  reseed(7);
  function reseed(s) { seed = s >>> 0; }
  const flows = FLOWS.map((f) => {
    const nodes = {}, list = [];
    Object.keys(f.nodes).forEach((id) => {
      const d = f.nodes[id];
      const n = {
        label: d[0], short: d[1], sub0: d[2], sub: '', c: d[3], r: d[4], type: d[5], k: d[6], states: d[7] || null,
        x: 0, y: 0, hw: 6, hh: 5, lab: '', lx: 0, ly: 0, sy: 0, flash: 0, fk: 0, busy: 0,
        flipStep: null, dsub: false, part: 'p' + ((rand() * 3) | 0), offBase: Math.floor(rr(1.2e4, 9.6e4)),
        offRate: rr(2, 5), offVal: -1, offStr: '',
      };
      nodes[id] = n;
      list.push(n);
    });
    const edges = {}, elist = [];
    Object.keys(f.edges).forEach((id) => {
      const d = f.edges[id];
      const e = {
        a: nodes[d[0]], b: nodes[d[1]], txt0: d[2], txt: '', k: d[3], lt: d[4] || 0.5,
        x0: 0, y0: 0, c1x: 0, c1y: 0, c2x: 0, c2y: 0, x3: 0, y3: 0, len: 1, lx: 0, ly: 0, tw: 0, act: 0, el: '', elw: 0,
      };
      edges[id] = e;
      elist.push(e);
    });
    const span = (s) => Object.assign({}, s, {
      path: (s.path || []).map((id) => edges[id]), at: s.at ? nodes[s.at] : null, el: s.el || '',
      txt: '', dur: '', t0: 0, t1: 0, as: 0, ae: 0, bs: 0, be: -1, busy: null, subNode: null, elw: 0,
    });
    const steps = f.steps.map(span);
    steps.forEach((s) => {
      if (s.flip) nodes[s.flip].flipStep = s;
      if (s.sub) { s.subNode = s.path[s.path.length - 1].b; s.subNode.dsub = true; }
    });
    return {
      tab: f.tab, title: f.title, list, elist, steps, root: span(f.root),
      chipDefs: steps.filter((s) => s.chip).map((s) => [nodes[s.chip[0]], s.chip[1], s.chip[2], s])
        .concat(f.root.chip ? [[nodes[f.root.chip[0]], f.root.chip[1], f.root.chip[2], null]] : []),
      chips: [], amb: f.amb.map((a) => [edges[a[0]], a[1], a[2]]), segs: [], T: 8, D: 1, header: '', axis: '',
    };
  });

  /* ---------- clock ---------- */
  let g = 3, tau = 0, cur = 0, cycle = 0, A = 1;
  const SEG = 0.36, DWELL = 0.22, WORK = 0.75, GAP = 0.05, FADE = 0.45, HOLD = 1.5, CHIP_LIFE = 2.2, AMB_TRAVEL = 1.1;
  const batchDone = () => 196 + Math.floor(g * 2.4); // tasks completed so far, simulated

  function prepare(F) { // per-cycle numbers: jittered timings, ids, placeholders, then the animation schedule
    reseed(cycle * 7919 + 17);
    const done = batchDone();
    const task = String(done % 400).padStart(4, '0'), job = String(2611 + Math.floor(done / 400));
    const brand = BRANDS[Math.floor(cycle / 4) % 4];
    const fill = (s) => s.replace('{task}', task).replace('{job}', job).replace('{brand}', brand);
    F.list.forEach((n) => { n.sub = fill(n.sub0); });
    F.elist.forEach((e) => { e.txt = fill(e.txt0); });
    const f = rr(0.86, 1.18);
    let end = 0;
    F.steps.forEach((s) => {
      s.txt = fill(s.name);
      s.t0 = s.ms[0] * f * rr(0.97, 1.03);
      s.t1 = s.t0 + (s.ms[1] - s.ms[0]) * f * rr(0.9, 1.1);
      s.dur = fmtMs(s.t1 - s.t0);
      end = Math.max(end, s.t1);
    });
    const R = F.root;
    R.txt = fill(R.name);
    R.t1 = R.after != null ? F.steps[R.after].t1 + 3 * f : end * 1.03 + 2;
    R.dur = fmtMs(R.t1);
    F.D = Math.max(end, R.t1);
    F.axis = fmtMs(F.D);
    const hex = () => ('0000000' + ((rand() * 4294967296) >>> 0).toString(16)).slice(-8);
    F.header = 'trace ' + hex() + hex().slice(0, 4) + '  ·  ' + (F.steps.length + 1) + ' spans';
    schedule(F);
  }

  function schedule(F) {
    const segs = [];
    let t = 0.3;
    const go = (path, dir) => {
      for (let i = 0; i < path.length; i++) {
        const e = dir > 0 ? path[i] : path[path.length - 1 - i];
        segs.push({ e, dir, ts: t, te: t + SEG });
        t += SEG;
      }
    };
    const R = F.root;
    R.as = t;
    go(R.path, 1);
    F.steps.forEach((s, i) => {
      s.as = t;
      if (!s.path.length) { s.busy = s.at; s.bs = t; t += WORK; s.be = t; }
      else {
        go(s.path, 1);
        if (s.round) { s.busy = s.path[s.path.length - 1].b; s.bs = t; t += DWELL; s.be = t; go(s.path, -1); }
      }
      s.ae = t;
      t += GAP;
      if (R.after === i) { go(R.path, -1); R.ae = t; }
    });
    if (R.after == null) { go(R.path, -1); R.ae = t; }
    F.segs = segs;
    F.chips = F.chipDefs.map((c) => ({ n: c[0], txt: c[1], k: c[2], at: c[3] ? c[3].ae : R.ae, root: !c[3], w: 0 }));
    F.T = t + HOLD + FADE;
  }

  /* ---------- layout ---------- */
  let W = 0, H = 0, dpr = 1, rich = false, trace = false, mX = 40, titleY = 0, tabsY = 0, segY = 0, statusY = 0, simY = 0;
  const rowsY = [0, 0, 0];
  const wf = { top: 0, x0: 0, x1: 0, lw: 0, bx0: 0, bx1: 0 };
  const tabX = [0, 0, 0, 0], tabW = [0, 0, 0, 0];
  const layer = document.createElement('canvas'); // static parts of the current flow, repainted on switch
  const lctx = layer.getContext('2d');
  const P = { x: 0, y: 0 };
  const crisp = (v) => (Math.round(v * dpr) + (dpr === 1 ? 0.5 : 0)) / dpr;
  const rowY = (i) => wf.top + 22 + i * 19;

  function bez(e, t) {
    const u = 1 - t, a = u * u * u, b = 3 * u * u * t, c = 3 * u * t * t, d = t * t * t;
    P.x = a * e.x0 + b * e.c1x + c * e.c2x + d * e.x3;
    P.y = a * e.y0 + b * e.c1y + c * e.c2y + d * e.y3;
  }

  function measure() {
    W = wrap.clientWidth;
    H = wrap.clientHeight;
    if (W < 40 || H < 40) return false;
    dpr = Math.min(2, window.devicePixelRatio || 1);
    canvas.width = Math.round(W * dpr);
    canvas.height = Math.round(H * dpr);
    readPalette();
    rich = W >= 660; // full labels, subs and every edge label; otherwise short labels, active edge label only
    trace = H >= 400; // room for the span waterfall; otherwise a one-line status
    mX = rich ? Math.max(40, W * 0.13) : Math.max(30, W * 0.09);
    if (trace) {
      tabsY = 26; titleY = 47;
      rowsY[0] = H * 0.2; rowsY[1] = H * 0.34; rowsY[2] = H * 0.48;
      wf.top = H * 0.575; wf.x0 = mX - 8; wf.x1 = W - mX + 8;
      wf.lw = (wf.x1 - wf.x0) * 0.4; wf.bx0 = wf.x0 + wf.lw; wf.bx1 = wf.x1 - 58;
    } else {
      titleY = 18; segY = 32;
      rowsY[0] = H * 0.29; rowsY[1] = H * 0.48; rowsY[2] = H * 0.67;
      statusY = H * 0.835; simY = H * 0.92;
    }
    ctx.font = font(10);
    let total = 0;
    for (let i = 0; i < 4; i++) { tabW[i] = ctx.measureText(FLOWS[i].tab).width; total += tabW[i] + (i ? 22 : 0); }
    for (let i = 0, x = (W - total) / 2; i < 4; i++) { tabX[i] = x; x += tabW[i] + 22; }
    return true;
  }

  function layoutFlow(F) {
    const X = (c) => mX + (c * (W - 2 * mX)) / 4;
    F.list.forEach((n) => {
      n.x = X(n.c);
      n.y = Math.round(rowsY[n.r]);
      if (n.type === 'topic') { n.hw = rich ? 43 : 24; n.hh = rich ? 11 : 8; }
      else if (n.type === 'batch') { n.hw = rich ? 42 : 26; n.hh = rich ? 12 : 8; }
      else { n.hw = 6; n.hh = n.type === 'svc' ? 4 : 5; }
      n.lab = rich ? n.label : n.short;
      n.ly = n.y + n.hh + (n.type === 'batch' ? 16 : 11);
      n.sy = n.ly + 12;
    });
    F.elist.forEach((e) => {
      const a = e.a, b = e.b, s = b.x >= a.x ? 1 : -1;
      e.x0 = a.x + s * a.hw; e.y0 = a.y;
      e.x3 = b.x - s * b.hw; e.y3 = b.y;
      const dx = e.x3 - e.x0;
      e.c1x = e.x0 + dx * 0.5; e.c1y = e.y0;
      e.c2x = e.x3 - dx * 0.5; e.c2y = e.y3;
      let len = 0, px = e.x0, py = e.y0;
      for (let i = 1; i <= 16; i++) { bez(e, i / 16); len += Math.hypot(P.x - px, P.y - py); px = P.x; py = P.y; }
      e.len = Math.max(1, len);
      bez(e, e.lt);
      e.lx = P.x; e.ly = P.y - 7;
    });
  }

  function roundRect(g2, x, y, w, h, r) {
    g2.beginPath();
    g2.moveTo(x + r, y);
    g2.arcTo(x + w, y, x + w, y + h, r);
    g2.arcTo(x + w, y + h, x, y + h, r);
    g2.arcTo(x, y + h, x, y, r);
    g2.arcTo(x, y, x + w, y, r);
    g2.closePath();
  }
  function pathEdge(g2, e) {
    g2.beginPath();
    g2.moveTo(e.x0, e.y0);
    g2.bezierCurveTo(e.c1x, e.c1y, e.c2x, e.c2y, e.x3, e.y3);
  }
  function hline(g2, x0, y, x1) { g2.beginPath(); g2.moveTo(x0, crisp(y)); g2.lineTo(x1, crisp(y)); g2.stroke(); }
  function fit(g2, s, max) { // truncate a label to a width, once per repaint
    if (g2.measureText(s).width <= max) return s;
    while (s.length > 4 && g2.measureText(s + '…').width > max) s = s.slice(0, -1);
    return s + '…';
  }

  function body(g2, n) {
    const x = n.x, y = n.y, c = col.kind[n.k];
    g2.fillStyle = col.ground;
    g2.globalAlpha = 1;
    g2.beginPath();
    if (n.type === 'svc') g2.arc(x, y, 4, 0, TAU);
    else if (n.type === 'store') g2.rect(crisp(x - 4.5), crisp(y - 4.5), 9, 9);
    else if (n.type === 'client') roundRect(g2, x - 5, y - 5, 10, 10, 2.5);
    else if (n.type === 'ext') { g2.moveTo(x, y - 6); g2.lineTo(x + 6, y); g2.lineTo(x, y + 6); g2.lineTo(x - 6, y); g2.closePath(); }
    else roundRect(g2, crisp(x - n.hw), crisp(y - n.hh), Math.round(n.hw * 2), Math.round(n.hh * 2), 4);
    g2.fill();
    g2.strokeStyle = n.type === 'store' ? c : n.type === 'topic' ? col.kind[EVT] : col.ink;
    g2.globalAlpha = n.type === 'topic' ? 0.4 : n.type === 'batch' ? 0.3 : 0.6;
    g2.stroke();
    if (n.type === 'topic') { // three partitions
      g2.strokeStyle = col.kind[EVT];
      g2.globalAlpha = 0.2;
      const o = n.hh * 0.5;
      for (let p = -1; p <= 1; p++) hline(g2, x - n.hw + 5, y + p * o, x + n.hw - 5);
    } else if (n.type !== 'batch') {
      g2.fillStyle = c;
      g2.globalAlpha = 0.9;
      g2.beginPath(); g2.arc(x, y, 1.5, 0, TAU); g2.fill();
    }
  }

  function paintLayer(F) {
    layer.width = canvas.width;
    layer.height = canvas.height;
    const g2 = lctx;
    g2.setTransform(dpr, 0, 0, dpr, 0, 0);
    g2.clearRect(0, 0, W, H);
    g2.textBaseline = 'middle';
    g2.lineWidth = 1;

    g2.font = font(11);
    g2.textAlign = 'center';
    g2.fillStyle = col.ink;
    g2.globalAlpha = 0.85;
    g2.fillText(F.title, W / 2, titleY);

    F.elist.forEach((e) => {
      g2.strokeStyle = col.kind[e.k];
      g2.globalAlpha = 0.2;
      pathEdge(g2, e);
      g2.stroke();
    });
    F.list.forEach((n) => body(g2, n));

    F.list.forEach((n) => {
      g2.font = font(10);
      const half = g2.measureText(n.lab).width / 2;
      n.lx = clamp(n.x, half + 3, W - half - 3);
      g2.fillStyle = col.mute;
      g2.globalAlpha = 1;
      g2.fillText(n.lab, n.lx, n.ly);
      const dyn = n.states || n.dsub || n.type === 'topic' || n.type === 'batch';
      if (rich && n.sub && !dyn) {
        g2.font = font(9);
        g2.globalAlpha = 0.6;
        g2.fillText(n.sub, n.lx, n.sy);
      }
    });

    g2.font = font(9);
    F.elist.forEach((e) => { e.tw = e.txt ? g2.measureText(e.txt).width : 0; });
    F.steps.forEach((s) => { s.elw = s.el ? g2.measureText(s.el).width : 0; });
    g2.font = font(8.5);
    F.chips.forEach((c) => { c.w = g2.measureText(c.txt).width + 10; });

    if (trace) paintWaterfall(g2, F);
    else {
      g2.font = font(8.5);
      g2.textAlign = 'center';
      g2.fillStyle = col.mute;
      g2.globalAlpha = 0.7;
      g2.fillText('simulated trace', W / 2, simY);
    }
    g2.globalAlpha = 1;
  }

  function paintWaterfall(g2, F) {
    const rows = F.steps.length + 1, last = rowY(rows - 1);
    g2.font = font(9);
    g2.fillStyle = col.mute;
    g2.textAlign = 'left';
    g2.globalAlpha = 0.85;
    g2.fillText(F.header, wf.x0, wf.top);
    g2.textAlign = 'right';
    g2.globalAlpha = 0.65;
    g2.fillText('simulated trace', wf.x1, wf.top);
    g2.strokeStyle = col.mute;
    g2.globalAlpha = 0.16;
    hline(g2, wf.x0, wf.top + 10, wf.x1);
    g2.globalAlpha = 0.07;
    for (let k = 0; k <= 4; k++) {
      const x = crisp(wf.bx0 + (k * (wf.bx1 - wf.bx0)) / 4);
      g2.beginPath(); g2.moveTo(x, wf.top + 14); g2.lineTo(x, last + 8); g2.stroke();
    }
    g2.textAlign = 'left';
    for (let i = 0; i < rows; i++) {
      const s = i ? F.steps[i - 1] : F.root, ind = (s.d || 0) * 9, y = rowY(i);
      g2.fillStyle = i ? col.mute : col.ink;
      g2.globalAlpha = i ? 0.9 : 0.75;
      g2.fillText(fit(g2, s.txt, wf.lw - ind - 10), wf.x0 + ind, y);
      g2.strokeStyle = col.mute;
      g2.globalAlpha = 0.05;
      hline(g2, wf.bx0, y, wf.bx1);
    }
    g2.fillStyle = col.mute;
    g2.globalAlpha = 0.55;
    g2.fillText('0 ms', wf.bx0, last + 18);
    g2.textAlign = 'right';
    g2.fillText(F.axis, wf.bx1, last + 18);
  }

  /* ---------- per-frame drawing ---------- */
  const ga = (a) => { ctx.globalAlpha = a * A; };

  function activity(F) {
    F.elist.forEach((e) => { e.act = 0; e.el = ''; });
    F.list.forEach((n) => { n.flash = 0; n.busy = 0; });
    for (let i = 0; i < F.segs.length; i++) {
      const s = F.segs[i], e = s.e;
      let a = 0;
      if (tau >= s.ts && tau < s.te) a = 1;
      else if (tau >= s.te && tau < s.te + 0.9) a = 1 - (tau - s.te) / 0.9;
      if (a > e.act) e.act = a;
      if (tau >= s.te && tau < s.te + 0.6) {
        const n = s.dir > 0 ? e.b : e.a, f = 1 - (tau - s.te) / 0.6;
        if (f > n.flash) { n.flash = f; n.fk = e.k; }
      }
    }
    for (let i = 0; i < F.steps.length; i++) {
      const s = F.steps[i];
      if (s.busy && tau >= s.bs && tau < s.be) s.busy.busy = 1;
      if (s.el && tau >= s.as && tau < s.ae + 0.9) for (let j = 0; j < s.path.length; j++) { s.path[j].el = s.el; s.path[j].elw = s.elw; }
    }
  }

  function drawEdges(F) {
    ctx.lineWidth = 1;
    F.elist.forEach((e) => {
      if (e.act < 0.01) return;
      ctx.strokeStyle = col.kind[e.k];
      ga(0.5 * e.act);
      pathEdge(ctx, e);
      ctx.stroke();
    });
    ctx.font = font(9);
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    F.elist.forEach((e) => {
      const txt = e.el || e.txt, w = e.el ? e.elw : e.tw;
      if (!txt || (!rich && e.act < 0.02)) return;
      ctx.fillStyle = col.ground;
      ga(rich ? 0.8 : 0.8 * e.act);
      ctx.fillRect(e.lx - w / 2 - 3, e.ly - 6, w + 6, 12);
      if (rich) { ctx.fillStyle = col.mute; ga(0.7); ctx.fillText(txt, e.lx, e.ly); }
      if (e.act > 0.02) { ctx.fillStyle = col.ink; ga(e.act * 0.95); ctx.fillText(txt, e.lx, e.ly); }
    });
  }

  function drawWidgets(F) {
    ctx.font = font(9);
    ctx.textAlign = 'center';
    F.list.forEach((n) => {
      if (n.type === 'topic') { // messages moving along the partitions, plus a committed offset
        const span = n.hw * 2 - 14, o = n.hh * 0.5;
        ctx.strokeStyle = col.kind[EVT];
        ctx.lineCap = 'round';
        ctx.lineWidth = 2;
        ga(0.75);
        ctx.beginPath();
        for (let p = -1; p <= 1; p++) {
          for (let i = 0; i < 3; i++) {
            const x = n.x - n.hw + 5 + ((g * (12 + p * 4) + (i * span) / 3 + (p + 1) * 9) % span);
            ctx.moveTo(x, n.y + p * o); ctx.lineTo(x + 3, n.y + p * o);
          }
        }
        ctx.stroke();
        ctx.lineCap = 'butt';
        ctx.lineWidth = 1;
        const val = n.offBase + Math.floor(g * n.offRate);
        if (val !== n.offVal) { n.offVal = val; n.offStr = n.part + ' · off ' + val.toLocaleString('en-US'); }
        ctx.fillStyle = col.mute;
        ga(0.75);
        ctx.fillText(n.offStr, n.lx, n.sy);
      } else if (n.type === 'batch') { // task slots, job progress
        const slots = rich ? 12 : 6, size = 3.5, gap = 2.5, total = slots * size + (slots - 1) * gap;
        const done = batchDone(), task = done % 400;
        for (let i = 0; i < slots; i++) {
          const lit = hash(i, Math.floor(g * 1.7 + i * 0.37)) > 0.42;
          ctx.fillStyle = lit ? col.kind[EVT] : col.mute;
          ga(lit ? 0.75 : 0.16);
          ctx.fillRect(n.x - total / 2 + i * (size + gap), n.y - size / 2, size, size);
        }
        const ty = crisp(n.y + n.hh + 5);
        ctx.strokeStyle = col.mute;
        ga(0.18);
        ctx.beginPath(); ctx.moveTo(n.x - n.hw, ty); ctx.lineTo(n.x + n.hw, ty); ctx.stroke();
        ctx.strokeStyle = col.kind[EVT];
        ga(0.75);
        ctx.beginPath(); ctx.moveTo(n.x - n.hw, ty); ctx.lineTo(n.x - n.hw + (2 * n.hw * task) / 400, ty); ctx.stroke();
        if (task !== n.offVal) { n.offVal = task; n.offStr = 'task ' + task + '/400'; }
        ctx.fillStyle = col.mute;
        ga(0.8);
        ctx.fillText(n.offStr, n.lx, n.sy);
      } else if (n.states) { // circuit breaker on the token-store call
        const st = n.states[n.flipStep && tau >= n.flipStep.ae ? 1 : 0];
        ctx.fillStyle = col.kind[st[1]];
        ga(0.9);
        ctx.fillText(st[0], n.lx, n.sy);
      } else if (n.dsub) { // which API resource is being called
        let txt = n.sub;
        for (let i = 0; i < F.steps.length; i++) if (F.steps[i].subNode === n && tau >= F.steps[i].as) txt = F.steps[i].sub;
        ctx.fillStyle = col.mute;
        ga(0.75);
        ctx.fillText(txt, n.lx, n.sy);
      }
    });
  }

  function packet(e, p, dir, alpha) {
    const c = col.kind[e.k], G = rich ? 9 : 7;
    const tail = clamp(p - (dir * (rich ? 24 : 16)) / e.len, 0, 1);
    ctx.strokeStyle = c;
    bez(e, tail);
    let px = P.x, py = P.y;
    for (let s = 1; s <= 6; s++) {
      const f = s / 6;
      bez(e, tail + (p - tail) * f);
      ga(alpha * 0.5 * f * f);
      ctx.beginPath(); ctx.moveTo(px, py); ctx.lineTo(P.x, P.y); ctx.stroke();
      px = P.x; py = P.y;
    }
    ga(alpha * 0.9);
    ctx.drawImage(glow[e.k], px - G, py - G, G * 2, G * 2);
    ga(alpha);
    ctx.fillStyle = c;
    ctx.beginPath(); ctx.arc(px, py, 1.3, 0, TAU); ctx.fill();
  }

  function drawPackets(F) {
    ctx.globalCompositeOperation = 'lighter';
    ctx.lineWidth = 1;
    ctx.lineCap = 'round';
    for (let i = 0; i < F.segs.length; i++) {
      const s = F.segs[i];
      if (tau < s.ts || tau >= s.te) continue;
      const t = (tau - s.ts) / (s.te - s.ts);
      packet(s.e, s.dir > 0 ? t : 1 - t, s.dir, 1);
    }
    for (let i = 0; i < F.amb.length; i++) { // background traffic (change feeds, catalog import)
      const a = F.amb[i], u = ((tau + a[2]) % a[1]) / AMB_TRAVEL;
      if (u < 1) packet(a[0], u, 1, 0.6);
    }
    ctx.globalCompositeOperation = 'source-over';
    ctx.lineCap = 'butt';
  }

  function drawNodeFx(F) {
    ctx.lineWidth = 1;
    ctx.font = font(10);
    ctx.textAlign = 'center';
    F.list.forEach((n) => {
      const box = n.type === 'topic' || n.type === 'batch';
      if (n.flash > 0.02 && !box) {
        ctx.strokeStyle = col.kind[n.fk];
        ga(n.flash * 0.5);
        ctx.beginPath(); ctx.arc(n.x, n.y, 8 + (1 - n.flash) * 7, 0, TAU); ctx.stroke();
      }
      if (n.busy) {
        ctx.strokeStyle = col.kind[n.k];
        ga(0.8);
        if (box) { roundRect(ctx, crisp(n.x - n.hw), crisp(n.y - n.hh), Math.round(n.hw * 2), Math.round(n.hh * 2), 4); ctx.stroke(); }
        else { const a0 = g * 6; ctx.beginPath(); ctx.arc(n.x, n.y, 9, a0, a0 + 1.7); ctx.stroke(); }
      }
      const hot = Math.max(n.flash, n.busy);
      if (hot > 0.02) {
        ctx.fillStyle = col.ink;
        ga(hot * 0.9);
        ctx.fillText(n.lab, n.lx, n.ly);
      }
    });
  }

  function drawChips(F) { // status / cache markers; a node shows only its latest chip
    ctx.font = font(8.5);
    ctx.textAlign = 'left';
    for (let i = 0; i < F.chips.length; i++) {
      const c = F.chips[i], age = tau - c.at;
      if (age < 0 || (!c.root && age > CHIP_LIFE)) continue;
      let newer = false;
      for (let j = 0; j < F.chips.length; j++) if (j !== i && F.chips[j].n === c.n && F.chips[j].at > c.at && tau >= F.chips[j].at) newer = true;
      if (newer) continue;
      const a = Math.min(1, age / 0.15) * (c.root ? 1 : clamp((CHIP_LIFE - age) / 0.4, 0, 1));
      const x = clamp(c.n.x - c.w / 2, 4, W - 4 - c.w), y = c.n.y - c.n.hh - 17; // above the node, clear of edge labels
      roundRect(ctx, x, y - 6, c.w, 12, 6);
      ctx.fillStyle = col.ground;
      ga(0.9 * a);
      ctx.fill();
      ctx.strokeStyle = col.kind[c.k];
      ga(0.7 * a);
      ctx.stroke();
      ctx.fillStyle = col.kind[c.k];
      ga(a);
      ctx.fillText(c.txt, x + 5, y + 0.5);
    }
  }

  function drawWaterfall(F) {
    const bw = wf.bx1 - wf.bx0, rows = F.steps.length + 1;
    ctx.font = font(9);
    ctx.textAlign = 'right';
    for (let i = 0; i < rows; i++) {
      const s = i ? F.steps[i - 1] : F.root, y = rowY(i);
      const f = clamp((tau - s.as) / Math.max(0.01, s.ae - s.as), 0, 1);
      if (f <= 0) continue;
      const x = wf.bx0 + (s.t0 / F.D) * bw, w = Math.max(2, ((s.t1 - s.t0) / F.D) * bw);
      ctx.fillStyle = col.kind[s.k];
      ga(0.85);
      ctx.fillRect(x, y - 2, w * f, 4);
      if (f < 1) {
        ctx.globalCompositeOperation = 'lighter';
        ga(0.9);
        ctx.drawImage(glow[s.k], x + w * f - 8, y - 8, 16, 16);
        ctx.globalCompositeOperation = 'source-over';
      } else {
        ctx.fillStyle = col.ink;
        ga(0.8);
        ctx.fillText(s.dur, wf.x1, y);
      }
    }
  }

  function drawStatus(F) { // narrow/short: the span in progress, as one line
    let s = F.root;
    for (let i = 0; i < F.steps.length; i++) if (tau >= F.steps[i].as) s = F.steps[i];
    if (tau >= F.root.ae) s = F.root;
    const x0 = Math.max(12, mX - 12), x1 = W - x0;
    ctx.font = font(10);
    ctx.textAlign = 'left';
    ctx.fillStyle = col.kind[s.k];
    ga(0.9);
    ctx.fillRect(x0, statusY - 3, 2, 6);
    ctx.fillStyle = col.ink;
    ga(0.85);
    ctx.fillText(s.txt, x0 + 8, statusY);
    if (tau >= s.ae) {
      ctx.textAlign = 'right';
      ctx.fillStyle = col.mute;
      ga(0.9);
      ctx.fillText(s.dur, x1, statusY);
    }
  }

  function drawTabs(F) { // flow selector; not faded with the flow
    const p = clamp(tau / F.T, 0, 1);
    ctx.globalAlpha = 1;
    ctx.lineWidth = 1;
    if (trace) {
      ctx.font = font(10);
      ctx.textAlign = 'left';
      for (let i = 0; i < 4; i++) {
        const on = i === cur;
        ctx.fillStyle = on ? col.ink : col.mute;
        ctx.globalAlpha = on ? 0.95 : 0.55;
        ctx.fillText(FLOWS[i].tab, tabX[i], tabsY);
      }
      const y = crisp(tabsY + 10);
      ctx.strokeStyle = col.mute;
      ctx.globalAlpha = 0.25;
      ctx.beginPath(); ctx.moveTo(tabX[cur], y); ctx.lineTo(tabX[cur] + tabW[cur], y); ctx.stroke();
      ctx.strokeStyle = col.kind[REQ];
      ctx.globalAlpha = 0.85;
      ctx.beginPath(); ctx.moveTo(tabX[cur], y); ctx.lineTo(tabX[cur] + tabW[cur] * p, y); ctx.stroke();
    } else {
      const sw = 18, gap = 6, x0 = W / 2 - (4 * sw + 3 * gap) / 2, y = crisp(segY);
      for (let i = 0; i < 4; i++) {
        const x = x0 + i * (sw + gap);
        ctx.strokeStyle = col.mute;
        ctx.globalAlpha = i < cur ? 0.5 : 0.22;
        ctx.beginPath(); ctx.moveTo(x, y); ctx.lineTo(x + sw, y); ctx.stroke();
        if (i === cur) {
          ctx.strokeStyle = col.kind[REQ];
          ctx.globalAlpha = 0.85;
          ctx.beginPath(); ctx.moveTo(x, y); ctx.lineTo(x + sw * p, y); ctx.stroke();
        }
      }
    }
  }

  function draw() {
    const F = flows[cur];
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.globalCompositeOperation = 'source-over';
    ctx.globalAlpha = 1;
    ctx.clearRect(0, 0, W, H); // transparent: the wrap paints the ground
    ctx.textBaseline = 'middle';
    drawTabs(F);
    A = smooth(tau / FADE) * smooth((F.T - tau) / FADE);
    if (A < 0.002) return;
    ctx.globalAlpha = A;
    ctx.drawImage(layer, 0, 0, W, H);
    activity(F);
    drawEdges(F);
    drawWidgets(F);
    drawPackets(F);
    drawNodeFx(F);
    drawChips(F);
    if (trace) drawWaterfall(F);
    else drawStatus(F);
  }

  /* ---------- lifecycle ---------- */
  function show(i) {
    cur = i;
    const F = flows[cur];
    prepare(F);
    layoutFlow(F);
    paintLayer(F);
  }

  function step(dt) {
    g += dt;
    tau += dt;
    if (tau >= flows[cur].T) { tau = 0; cycle++; show(cycle % 4); }
  }

  let running = false, raf = 0, last = 0, onScreen = true, started = false;
  function frame(now) {
    if (!running) return;
    const dt = last ? Math.min(0.05, (now - last) / 1000) : 1 / 60;
    last = now;
    step(dt);
    draw();
    raf = requestAnimationFrame(frame);
  }
  function sync() {
    const want = !REDUCED && onScreen && !document.hidden && W > 0;
    if (want && !running) { running = true; last = 0; raf = requestAnimationFrame(frame); }
    else if (!want && running) { running = false; cancelAnimationFrame(raf); }
  }

  function onResize() {
    if (!measure()) return;
    if (!started) {
      started = true;
      prepare(flows[cur]);
      if (REDUCED) tau = flows[cur].root.ae + 0.6; // one composed frame: the finished trace, before fade-out
    }
    layoutFlow(flows[cur]);
    paintLayer(flows[cur]);
    draw();
    sync();
  }

  if (window.ResizeObserver) new ResizeObserver(onResize).observe(wrap);
  else { window.addEventListener('resize', onResize); onResize(); }

  if (window.IntersectionObserver) {
    new IntersectionObserver((es) => { onScreen = es[es.length - 1].isIntersecting; sync(); }).observe(canvas);
  }
  document.addEventListener('visibilitychange', sync);

  // Tabs are canvas-drawn, so hit-test them by hand: click a tab to jump to that flow.
  function tabAt(e) {
    if (!tabsY) return -1;
    const r = canvas.getBoundingClientRect();
    const x = e.clientX - r.left, y = e.clientY - r.top;
    if (Math.abs(y - tabsY) > 16) return -1;
    for (let i = 0; i < 4; i++) if (x >= tabX[i] - 10 && x <= tabX[i] + tabW[i] + 10) return i;
    return -1;
  }
  canvas.addEventListener('click', (e) => {
    const i = tabAt(e);
    if (i < 0) return;
    tau = 0; cycle = i; show(i); draw();
  });
  canvas.addEventListener('pointermove', (e) => { canvas.style.cursor = tabAt(e) >= 0 ? 'pointer' : ''; });

  if (document.fonts && document.fonts.ready) { // repaint labels once JetBrains Mono is in
    document.fonts.ready.then(() => { if (W && measure()) { layoutFlow(flows[cur]); paintLayer(flows[cur]); draw(); } });
  }
})();
