/* CryptoSentinel Obsidian interactions: actual replay rows only; no synthetic observations. */
(() => {
  let obsMarketEvents = [];
  let obsSelectedNodeId = null;
  let obsGraphRefreshAt = 0;
  const byId = id => document.getElementById(id);
  const finite = value => Number.isFinite(Number(value));
  const dpr = () => Math.max(1, Math.min(2, window.devicePixelRatio || 1));
  const canvasSize = (canvas, logicalHeight) => {
    const ratio = dpr();
    const width = Math.max(280, canvas.clientWidth || canvas.parentElement?.clientWidth || 640);
    canvas.width = Math.round(width * ratio);
    canvas.height = Math.round(logicalHeight * ratio);
    const ctx = canvas.getContext('2d');
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    return { ctx, W: width, H: logicalHeight };
  };

  function drawMarketTape(rows) {
    const canvas = byId('chart-tl'), empty = byId('chart-tl-empty'), select = byId('ov-symbol');
    if (!canvas || !empty || !select) return;
    const counts = new Map();
    (rows || []).forEach(row => { if (row.symbol) counts.set(row.symbol, (counts.get(row.symbol) || 0) + 1); });
    const symbols = [...counts.keys()].sort((a, b) => counts.get(b) - counts.get(a) || a.localeCompare(b));
    const previous = select.value;
    select.replaceChildren();
    if (!symbols.length) {
      const option = document.createElement('option'); option.value = ''; option.textContent = 'Waiting for replay'; select.append(option);
      canvas.style.display = 'none'; empty.style.display = 'block';
      empty.querySelector('.sw-title').textContent = 'No historical observations yet';
      empty.querySelector('.sw-sub').textContent = 'Start benchmark replay to plot actual source prices and raw model scores.';
      canvas.title = 'No benchmark rows have been scored in this process.';
      return;
    }
    symbols.forEach(symbol => { const option = document.createElement('option'); option.value = symbol; option.textContent = symbol; select.append(option); });
    select.value = symbols.includes(previous) ? previous : symbols[0];
    const symbol = select.value;
    const points = (rows || []).filter(row => row.symbol === symbol && finite(row.price) && Number(row.price) > 0 && row.event_timestamp)
      .map(row => ({
        ts: new Date(row.event_timestamp), price: Number(row.price), volume: Number(row.volume || 0),
        risk: Math.max(0, Math.min(1, Number(row.risk_score || 0))),
        label: row.label === 'PUMP_AND_DUMP' ? 'Model flagged' : row.label === 'NORMAL' ? 'Model normal' : 'Scoring error',
        eventId: row.event_id || '', truth: row.ground_truth ?? row.is_pump
      })).filter(point => Number.isFinite(point.ts.getTime())).sort((a, b) => a.ts - b.ts);
    if (!points.length) {
      canvas.style.display = 'none'; empty.style.display = 'block';
      empty.querySelector('.sw-title').textContent = `No price rows for ${symbol}`;
      empty.querySelector('.sw-sub').textContent = 'Choose another observed asset or replay the benchmark.';
      return;
    }
    canvas.style.display = 'block'; empty.style.display = 'none';
    const { ctx, W, H } = canvasSize(canvas, window.innerWidth < 768 ? 245 : 330);
    ctx.clearRect(0, 0, W, H);
    const left = 65, right = 24, top = 27, split = H * .59, lowerTop = H * .70, bottom = H - 36;
    const plotW = W - left - right;
    const prices = points.map(p => p.price);
    const maxVolume = Math.max(...points.map(p => p.volume), 0);
    let minPrice = Math.min(...prices), maxPrice = Math.max(...prices);
    if (maxPrice === minPrice) { const padding = Math.abs(maxPrice || 1) * .02; minPrice -= padding; maxPrice += padding; }
    else { const padding = (maxPrice - minPrice) * .10; minPrice -= padding; maxPrice += padding; }
    const firstTs = points[0].ts.getTime(), lastTs = points[points.length - 1].ts.getTime();
    const xAt = i => points.length < 2 || lastTs === firstTs ? left + plotW / 2 : left + (points[i].ts.getTime() - firstTs) / (lastTs - firstTs) * plotW;
    const yPrice = price => top + (maxPrice - price) / (maxPrice - minPrice) * (split - top);
    const yRisk = risk => bottom - risk * (bottom - lowerTop);
    ctx.font = '10px ui-monospace,monospace'; ctx.lineWidth = 1;
    for (let tick = 0; tick <= 4; tick++) {
      const y = top + (split - top) * tick / 4;
      ctx.strokeStyle = '#e7eee9'; ctx.beginPath(); ctx.moveTo(left, y); ctx.lineTo(W - right, y); ctx.stroke();
      const value = maxPrice - (maxPrice - minPrice) * tick / 4;
      ctx.fillStyle = '#788a80'; ctx.textAlign = 'right'; ctx.fillText(fPr(value), left - 8, y + 3);
    }
    ctx.strokeStyle = '#d8e5dd'; ctx.setLineDash([3, 5]); ctx.beginPath(); ctx.moveTo(left, lowerTop); ctx.lineTo(W - right, lowerTop); ctx.stroke(); ctx.setLineDash([]);
    for (let tick = 0; tick <= 2; tick++) {
      const risk = tick * .5, y = yRisk(risk);
      ctx.strokeStyle = risk === .5 ? '#e5c88f' : '#e7eee9';
      if (risk === .5) ctx.setLineDash([5, 4]);
      ctx.beginPath(); ctx.moveTo(left, y); ctx.lineTo(W - right, y); ctx.stroke(); ctx.setLineDash([]);
      ctx.fillStyle = risk === .5 ? '#bd7a20' : '#788a80'; ctx.textAlign = 'right'; ctx.fillText(risk.toFixed(1), left - 8, y + 3);
    }
    ctx.fillStyle = '#71857a'; ctx.textAlign = 'left'; ctx.font = '9px ui-monospace,monospace';
    ctx.fillText('PRICE', 8, top + 4); ctx.fillText('SCORE', 8, lowerTop + 4); ctx.fillText('0–1', 8, lowerTop + 16);
    const sampled = points.length > 700 ? points.filter((_, i) => i % Math.ceil(points.length / 700) === 0 || i === points.length - 1) : points;
    ctx.beginPath(); sampled.forEach((point, index) => {
      const originalIndex = points.indexOf(point), x = xAt(originalIndex), y = yPrice(point.price);
      if (index === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = '#13845d'; ctx.lineWidth = 2; ctx.stroke();
    if (points.length === 1) { ctx.beginPath(); ctx.arc(xAt(0), yPrice(points[0].price), 3, 0, Math.PI * 2); ctx.fillStyle = '#13845d'; ctx.fill(); }
    points.forEach((point, index) => {
      if (point.risk >= .5) { ctx.beginPath(); ctx.arc(xAt(index), yPrice(point.price), 3.1, 0, Math.PI * 2); ctx.fillStyle = '#d66a50'; ctx.fill(); }
    });
    // Volume bars are independently normalized to this selected asset's maximum; they are not on the price scale.
    if (maxVolume > 0) {
      const barW = Math.max(1, Math.min(5, plotW / points.length * .7));
      points.forEach((point, index) => {
        const h = (point.volume / maxVolume) * (bottom - lowerTop) * .82;
        ctx.fillStyle = 'rgba(76,145,196,.28)';
        ctx.fillRect(xAt(index) - barW / 2, bottom - h, barW, h);
      });
    }
    ctx.beginPath(); points.forEach((point, index) => { const x = xAt(index), y = yRisk(point.risk); if (index === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y); });
    ctx.strokeStyle = '#d66a50'; ctx.lineWidth = 1.55; ctx.stroke();
    const labelTime = date => date.toLocaleString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', timeZone: 'UTC' }) + ' UTC';
    ctx.fillStyle = '#778980'; ctx.font = '9px ui-monospace,monospace';
    const tickIndexes = [...new Set([0, Math.floor((points.length - 1) / 2), points.length - 1])];
    tickIndexes.forEach((index, position) => {
      const x = xAt(index); ctx.textAlign = position === 0 ? 'left' : position === tickIndexes.length - 1 ? 'right' : 'center';
      ctx.fillText(labelTime(points[index].ts), x, H - 9);
    });
    ctx.fillStyle = '#4e685b'; ctx.font = '10px ui-monospace,monospace'; ctx.textAlign = 'left';
    ctx.fillText(`${symbol} · ${points.length} observed rows`, left, 14);
    canvas.setAttribute('aria-label', `Historical source-price and raw model-score series for ${symbol}, with ${points.length} observed benchmark rows.`);
    let lastTip = '';
    canvas.onmousemove = event => {
      const rect = canvas.getBoundingClientRect(), x = (event.clientX - rect.left) * W / rect.width;
      const index = Math.min(points.length - 1, Math.max(0, Math.round((x - left) / Math.max(plotW, 1) * (points.length - 1))));
      const p = points[index];
      const sourceLabel = p.truth === 1 || p.truth === '1' || p.truth === true ? 'Pump benchmark label (retrospective)' : p.truth === 0 || p.truth === '0' || p.truth === false ? 'Normal benchmark label (retrospective)' : 'Benchmark label unavailable';
      const tip = `${symbol} · ${labelTime(p.ts)}\nPrice: ${fPr(p.price)} · Volume: ${p.volume.toFixed(6)}\nRaw XGBoost score: ${(p.risk * 100).toFixed(1)}% · ${p.label}\n${sourceLabel}${p.eventId ? `\nWindow: ${p.eventId}` : ''}`;
      if (tip !== lastTip) { canvas.title = tip; lastTip = tip; }
    };
    canvas.onmouseleave = () => { canvas.title = `Historical ${symbol} price, normalized volume bars, and raw model score. Hover for source-row details.`; };
    canvas.title = `Historical ${symbol} price, normalized volume bars, and raw model score. Hover for source-row details.`;
  }

  window.refreshOvChart = () => drawMarketTape(obsMarketEvents);
  window.updateTLChart = () => drawMarketTape(obsMarketEvents);
  window.updateRiskChart = events => {
    obsMarketEvents = Array.isArray(events) ? events.slice() : [];
    const canvas = byId('chart-risk'), empty = byId('chart-risk-empty');
    const classes = { flagged: 0, normal: 0 };
    obsMarketEvents.forEach(event => {
      if (event.label === 'PUMP_AND_DUMP') classes.flagged++;
      else if (event.label === 'NORMAL') classes.normal++;
    });
    const total = classes.flagged + classes.normal;
    if (canvas && empty) {
      if (!total) { canvas.style.display = 'none'; empty.style.display = 'block'; }
      else {
        canvas.style.display = 'block'; empty.style.display = 'none';
        const { ctx, W, H } = canvasSize(canvas, window.innerWidth < 768 ? 230 : 220);
        ctx.clearRect(0, 0, W, H);
        const compact = W < 360, cx = compact ? W * .26 : Math.min(88, W * .26), cy = H * .48;
        const radius = Math.min(62, H * .34, W * .19), thickness = Math.max(13, radius * .38);
        const values = [classes.flagged, classes.normal], colors = ['#d66a50', '#17875f'];
        let angle = -Math.PI / 2;
        values.forEach((value, index) => {
          const sweep = value / total * Math.PI * 2;
          ctx.beginPath(); ctx.arc(cx, cy, radius, angle, angle + sweep); ctx.strokeStyle = colors[index]; ctx.lineWidth = thickness; ctx.lineCap = 'butt'; ctx.stroke(); angle += sweep;
        });
        ctx.fillStyle = '#19392c'; ctx.textAlign = 'center'; ctx.font = '700 20px system-ui,sans-serif'; ctx.fillText(fN(total), cx, cy + 2);
        ctx.fillStyle = '#788a80'; ctx.font = '10px system-ui,sans-serif'; ctx.fillText('rows', cx, cy + 18);
        const legendX = Math.min(W * .52, cx + radius + 25), legendY = H * .38;
        [['Model flagged', classes.flagged, colors[0]], ['Model normal', classes.normal, colors[1]]].forEach((item, index) => {
          const y = legendY + index * 38, share = total ? (item[1] / total * 100).toFixed(1) : '0.0';
          ctx.beginPath(); ctx.arc(legendX + 4, y - 3, 4, 0, Math.PI * 2); ctx.fillStyle = item[2]; ctx.fill();
          ctx.textAlign = 'left'; ctx.fillStyle = '#52685d'; ctx.font = '11px system-ui,sans-serif'; ctx.fillText(item[0], legendX + 15, y);
          ctx.fillStyle = '#19392c'; ctx.font = '700 13px system-ui,sans-serif'; ctx.fillText(`${fN(item[1])} · ${share}%`, legendX + 15, y + 17);
        });
        ctx.textAlign = 'center'; ctx.fillStyle = '#87958d'; ctx.font = '9px system-ui,sans-serif';
        ctx.fillText('Classification in the recent in-memory window', W / 2, H - 4);
        canvas.title = `${fN(classes.flagged)} model-flagged rows (${(classes.flagged / total * 100).toFixed(1)}%) and ${fN(classes.normal)} model-normal rows (${(classes.normal / total * 100).toFixed(1)}%) among ${fN(total)} retained classified rows. These are model predictions, not confirmed event types.`;
      }
    }
    drawMarketTape(obsMarketEvents);
    if (Date.now() - obsGraphRefreshAt > 8000) { obsGraphRefreshAt = Date.now(); drawOverviewGraph(); }
  };

  function drawOverviewGraph() {
    const canvas = byId('ov-graph'); if (!canvas) return;
    fetch('/api/graph/nodes?limit=300', { signal: AbortSignal.timeout(4500) }).then(response => {
      if (!response.ok) throw new Error(`HTTP ${response.status}`); return response.json();
    }).then(data => paintOverviewGraph(canvas, data.nodes || [], data.edges || [])).catch(() => paintOverviewGraph(canvas, [], [], 'Graph snapshot unavailable; retry on the Graph page.'));
  }
  function paintOverviewGraph(canvas, nodes, edges, errorText = '') {
    const { ctx, W, H } = canvasSize(canvas, window.innerWidth < 768 ? 210 : 230);
    ctx.clearRect(0, 0, W, H); ctx.fillStyle = '#f6faf7'; ctx.fillRect(0, 0, W, H);
    if (!nodes.length) {
      ctx.fillStyle = '#13845d'; ctx.font = '11px ui-monospace,monospace'; ctx.textAlign = 'center';
      ctx.fillText(errorText || 'NO GRAPH RECORDS IN THIS PROCESS', W / 2, H / 2 - 4);
      ctx.fillStyle = '#8d998d'; ctx.font = '10px ui-monospace,monospace'; ctx.fillText(errorText ? 'Open Graph Analysis to retry' : 'Run historical replay to build actual event relationships', W / 2, H / 2 + 18);
      return;
    }
    const assets = nodes.filter(node => node.type === 'CryptoAsset');
    if (assets.length) {
      const chosenIds = new Set(assets.map(node => node.id));
      nodes.filter(node => node.type === 'PumpGroup').forEach(node => chosenIds.add(node.id));
      assets.forEach(asset => {
        const linkedIds = new Set(edges.filter(edge => edge.source === asset.id || edge.target === asset.id).map(edge => edge.source === asset.id ? edge.target : edge.source));
        nodes.filter(node => node.type === 'MarketEvent' && linkedIds.has(node.id)).slice(0, 12).forEach(node => chosenIds.add(node.id));
      });
      nodes = nodes.filter(node => chosenIds.has(node.id));
      const visibleIds = new Set(nodes.map(node => node.id));
      edges = edges.filter(edge => visibleIds.has(edge.source) && visibleIds.has(edge.target));
    } else {
      nodes = nodes.slice(0, 45);
      const visibleIds = new Set(nodes.map(node => node.id));
      edges = edges.filter(edge => visibleIds.has(edge.source) && visibleIds.has(edge.target));
    }
    const pos = new Map();
    const lanes = { CryptoAsset: .16, MarketEvent: .51, PumpGroup: .84 };
    nodes.forEach(node => {
      const hash = Array.from(String(node.id)).reduce((sum, char) => (sum * 31 + char.charCodeAt(0)) >>> 0, 7);
      const y = H * (.13 + ((hash % 1000) / 1000) * .74);
      pos.set(node.id, { x: W * (lanes[node.type] || .5), y });
    });
    const nodeIds = new Set(nodes.map(node => node.id));
    edges.forEach(edge => {
      if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) return;
      const a = pos.get(edge.source), b = pos.get(edge.target); drawArrow(ctx, a, b, edge.type, .75);
    });
    nodes.forEach(node => {
      const p = pos.get(node.id), props = node.properties || {}, color = node.type === 'CryptoAsset' ? '#13845d' : node.type === 'PumpGroup' ? '#d09435' : props.suspicious ? '#d66a50' : '#5cae81';
      const radius = node.type === 'CryptoAsset' ? 6 : node.type === 'PumpGroup' ? 5 : props.suspicious ? 4.5 : 3;
      ctx.beginPath();
      if (node.type === 'PumpGroup') { ctx.moveTo(p.x, p.y - radius); ctx.lineTo(p.x + radius, p.y); ctx.lineTo(p.x, p.y + radius); ctx.lineTo(p.x - radius, p.y); ctx.closePath(); }
      else ctx.arc(p.x, p.y, radius, 0, Math.PI * 2);
      ctx.fillStyle = color; ctx.fill();
      if (node.type !== 'MarketEvent' || props.suspicious) {
        const label = node.type === 'CryptoAsset' ? (props.symbol || node.id.replace('asset_', '')) : node.type === 'PumpGroup' ? node.id.replace('group_', '') : (props.symbol || '');
        ctx.fillStyle = '#466155'; ctx.font = '9px ui-monospace,monospace'; ctx.textAlign = 'center'; ctx.fillText(String(label).slice(0, 15), p.x, p.y - radius - 5);
      }
    });
    ctx.fillStyle = '#70847a'; ctx.font = '9px ui-monospace,monospace'; ctx.textAlign = 'right'; ctx.fillText(`${nodes.length} nodes · ${edges.length} returned edges`, W - 12, H - 9);
  }

  function drawArrow(ctx, a, b, type, alpha = 1) {
    if (!a || !b) return;
    const color = type === 'ORGANIZED_BY' ? '#d09435' : type === 'NEXT' ? '#7cae89' : '#a7b9ae';
    const dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy) || 1, ux = dx / len, uy = dy / len;
    const startX = a.x + ux * 5, startY = a.y + uy * 5, endX = b.x - ux * 7, endY = b.y - uy * 7;
    ctx.strokeStyle = color; ctx.globalAlpha = alpha; ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(startX, startY); ctx.lineTo(endX, endY); ctx.stroke();
    const angle = Math.atan2(dy, dx), size = 4;
    ctx.beginPath(); ctx.moveTo(endX, endY); ctx.lineTo(endX - size * Math.cos(angle - Math.PI / 6), endY - size * Math.sin(angle - Math.PI / 6)); ctx.lineTo(endX - size * Math.cos(angle + Math.PI / 6), endY - size * Math.sin(angle + Math.PI / 6)); ctx.closePath(); ctx.fillStyle = color; ctx.fill(); ctx.globalAlpha = 1;
  }

  const originalInitGraph = window.initGraph;
  window.initGraph = function () {
    if (originalInitGraph) originalInitGraph();
    if (gCvs) {
      gCvs.width = gCvs.parentElement.offsetWidth || 700;
      gCvs.height = window.innerWidth < 768 ? 390 : window.innerWidth < 980 ? 520 : 640;
      initGPos(); renderG();
    }
  };

  window.initGPos = function () {
    const W = gCvs?.width || 700, H = gCvs?.height || 460, n = gN.length;
    if (!n) { gPos = {}; return; }
    const lanes = { CryptoAsset: .15, MarketEvent: .50, PumpGroup: .85 }, positions = gN.map(node => {
      const hash = Array.from(String(node.id)).reduce((sum, char) => (sum * 33 + char.charCodeAt(0)) >>> 0, 17);
      return { id: node.id, type: node.type, x: W * (lanes[node.type] || .5) + ((hash % 81) - 40), y: H * (.12 + ((hash >>> 7) % 760) / 1000) };
    });
    const index = new Map(positions.map((p, i) => [p.id, i]));
    const links = gE.filter(edge => index.has(edge.source) && index.has(edge.target)).map(edge => ({ a: index.get(edge.source), b: index.get(edge.target), type: edge.type }));
    const iterations = n > 220 ? 32 : 54;
    for (let step = 0; step < iterations; step++) {
      const fx = new Array(n).fill(0), fy = new Array(n).fill(0);
      for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) {
        let dx = positions[i].x - positions[j].x, dy = positions[i].y - positions[j].y, d2 = dx * dx + dy * dy;
        if (d2 < 1) { dx = ((i * 7 + j * 3) % 5) + 1; dy = ((i + j * 2) % 5) + 1; d2 = dx * dx + dy * dy; }
        const force = Math.min(1600, 19000 / d2), d = Math.sqrt(d2), rx = dx / d * force, ry = dy / d * force;
        fx[i] += rx; fy[i] += ry; fx[j] -= rx; fy[j] -= ry;
      }
      links.forEach(link => {
        const a = positions[link.a], b = positions[link.b], dx = b.x - a.x, dy = b.y - a.y, d = Math.hypot(dx, dy) || 1;
        const ideal = link.type === 'NEXT' ? 76 : 126, force = (d - ideal) * .018, sx = dx / d * force, sy = dy / d * force;
        fx[link.a] += sx; fy[link.a] += sy; fx[link.b] -= sx; fy[link.b] -= sy;
      });
      positions.forEach((p, i) => {
        const anchor = lanes[p.type] || .5;
        fx[i] += (W * anchor - p.x) * .004 + (W / 2 - p.x) * .001;
        fy[i] += (H / 2 - p.y) * .006;
        p.x = Math.max(28, Math.min(W - 28, p.x + Math.max(-9, Math.min(9, fx[i] * .16))));
        p.y = Math.max(26, Math.min(H - 26, p.y + Math.max(-9, Math.min(9, fy[i] * .16))));
      });
    }
    gPos = {}; positions.forEach(p => { gPos[p.id] = { x: p.x, y: p.y }; });
  };

  window.renderG = function () {
    if (!gCtx || !gCvs) return;
    const W = gCvs.width, H = gCvs.height;
    gCtx.clearRect(0, 0, W, H); gCtx.fillStyle = '#f6faf7'; gCtx.fillRect(0, 0, W, H);
    gCtx.save(); gCtx.strokeStyle = '#e9f0ec'; gCtx.lineWidth = 1;
    for (let x = 0; x < W; x += 36) { gCtx.beginPath(); gCtx.moveTo(x, 0); gCtx.lineTo(x, H); gCtx.stroke(); }
    for (let y = 0; y < H; y += 36) { gCtx.beginPath(); gCtx.moveTo(0, y); gCtx.lineTo(W, y); gCtx.stroke(); }
    if (!gN.length) {
      gCtx.fillStyle = '#13845d'; gCtx.font = '12px ui-monospace,monospace'; gCtx.textAlign = 'center';
      gCtx.fillText('NO MARKET RELATIONSHIPS IN MEMORY', W / 2, H / 2 - 9);
      gCtx.fillStyle = '#788a80'; gCtx.font = '11px ui-monospace,monospace'; gCtx.fillText('Run historical benchmark replay to populate this canvas', W / 2, H / 2 + 15);
      gCtx.restore(); return;
    }
    gCtx.save(); gCtx.translate(gOffsetX, gOffsetY); gCtx.scale(gScale, gScale);
    const nodeSet = new Set(gN.map(node => node.id));
    gE.forEach(edge => { if (nodeSet.has(edge.source) && nodeSet.has(edge.target)) drawArrow(gCtx, gPos[edge.source], gPos[edge.target], edge.type, edge.type === 'NEXT' ? .65 : .8); });
    gN.forEach(node => {
      const point = gPos[node.id]; if (!point) return;
      const props = node.properties || {}, radius = node.type === 'CryptoAsset' ? 12 : node.type === 'PumpGroup' ? 9 : props.suspicious ? 8 : 5;
      const color = node.type === 'CryptoAsset' ? '#13845d' : node.type === 'PumpGroup' ? '#d09435' : props.suspicious ? '#d66a50' : '#5cae81';
      gCtx.beginPath();
      if (node.type === 'PumpGroup') { gCtx.moveTo(point.x, point.y - radius); gCtx.lineTo(point.x + radius, point.y); gCtx.lineTo(point.x, point.y + radius); gCtx.lineTo(point.x - radius, point.y); gCtx.closePath(); }
      else gCtx.arc(point.x, point.y, radius, 0, Math.PI * 2);
      gCtx.fillStyle = color; gCtx.fill();
      if (node.id === obsSelectedNodeId) { gCtx.beginPath(); gCtx.arc(point.x, point.y, radius + 5, 0, Math.PI * 2); gCtx.strokeStyle = '#ffffff'; gCtx.lineWidth = 1.5; gCtx.stroke(); }
      if (node === gHov) { gCtx.beginPath(); gCtx.arc(point.x, point.y, radius + 3, 0, Math.PI * 2); gCtx.strokeStyle = '#13845d'; gCtx.lineWidth = 1.3; gCtx.stroke(); }
      if (node.type !== 'MarketEvent' || props.suspicious) {
        const label = node.type === 'CryptoAsset' ? (props.symbol || node.id.replace('asset_', '')) : node.type === 'PumpGroup' ? node.id.replace('group_', '') : (props.symbol || '');
        if (label) { gCtx.fillStyle = '#345447'; gCtx.font = `${node.type === 'CryptoAsset' ? '600 ' : ''}11px ui-monospace,monospace`; gCtx.textAlign = 'center'; gCtx.fillText(String(label).slice(0, 20), point.x, point.y - radius - 7); }
      }
    });
    gCtx.restore(); gCtx.restore();
  };

  function showNodeEvidence(node) {
    const panel = byId('g-det'); if (!panel || !node) return;
    const properties = node.properties || {};
    const relations = gE.filter(edge => edge.source === node.id || edge.target === node.id).map(edge => {
      const otherId = edge.source === node.id ? edge.target : edge.source;
      const other = gN.find(item => item.id === otherId), label = other?.properties?.symbol || other?.properties?.group || other?.id || otherId;
      return `<div class="ir"><span class="ik">${esc(edge.type)} →</span><span class="iv">${esc(String(label))}</span></div>`;
    }).join('');
    panel.innerHTML = `<div class="badge b-info">${esc(node.type)}</div><div class="mono" style="margin:9px 0 12px;word-break:break-all">${esc(node.id)}</div>` + Object.entries(properties).map(([key, value]) => `<div class="ir"><span class="ik">${esc(key)}</span><span class="iv">${esc(typeof value === 'number' && Math.abs(value) < 1 && value !== 0 ? value.toFixed(6) : String(value))}</span></div>`).join('') + (relations ? `<div style="margin-top:12px;padding-top:8px;border-top:1px solid var(--border2)"><div class="graph-hint" style="margin-bottom:5px">VISIBLE RELATIONSHIPS</div>${relations}</div>` : '<div class="graph-hint" style="margin-top:10px">No connected edge is present in the returned graph slice.</div>');
  }

  const originalGraphMouseDown = window.onGMD;
  window.onGMD = function (event) {
    const point = graphPoint(event), x = (point.x - gOffsetX) / gScale, y = (point.y - gOffsetY) / gScale;
    let selected = null;
    for (const node of gN) { const p = gPos[node.id]; if (p && (x - p.x) ** 2 + (y - p.y) ** 2 < (nRad(node) + 9) ** 2) { selected = node; break; } }
    if (selected) { obsSelectedNodeId = selected.id; gHov = selected; showNodeEvidence(selected); }
    else { obsSelectedNodeId = null; gHov = null; }
    if (originalGraphMouseDown) originalGraphMouseDown(event);
    renderG();
  };

  window.inspectGraphNode = async nodeId => {
    const nav = document.querySelectorAll('.sb-item')[4]; showPage('graph', nav);
    const status = byId('g-status'); if (status) status.textContent = 'Loading connected evidence for selected event…';
    try {
      const response = await fetch(`/api/graph/neighbors/${encodeURIComponent(nodeId)}?depth=2`, { signal: AbortSignal.timeout(6000) });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (!(data.nodes || []).some(node => node.id === nodeId)) throw new Error('This event is no longer in the process-local graph.');
      gN = data.nodes || []; gE = data.edges || []; initGPos();
      const selected = gN.find(node => node.id === nodeId); obsSelectedNodeId = nodeId; showNodeEvidence(selected);
      if (status) status.textContent = `Event evidence selected · ${gN.length} connected nodes · ${gE.length} actual edges`;
      renderG(); setTimeout(() => byId('gcanvas')?.scrollIntoView({ behavior: 'smooth', block: 'center' }), 60);
    } catch (error) {
      if (status) status.textContent = `Graph evidence unavailable: ${error.message}`;
      toast('This event has no available graph connection in the current process.', 'warn');
    }
  };

  const originalLoadDs = window.loadDs;
  window.loadDs = async function () { if (originalLoadDs) await originalLoadDs(); await window.loadDsSample(); };
  window.loadDsSample = async function () {
    const host = byId('ds-sample'); if (!host) return;
    host.innerHTML = '<div class="sw"><div class="sw-title">Reading source rows…</div></div>';
    try {
      const response = await fetch('/api/dataset/sample?limit=6', { signal: AbortSignal.timeout(8000) });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const result = await response.json();
      if (!result.available || !result.rows?.length) throw new Error(result.error || 'No sample rows are available.');
      const preferred = ['timestamp', 'symbol', 'price', 'volume', 'n_trades', 'event_id', 'is_pump'];
      const selectedColumns = preferred.filter(column => result.columns.includes(column));
      const columns = selectedColumns.concat(result.columns.filter(column => !selectedColumns.includes(column)).slice(0, Math.max(0, 7 - selectedColumns.length)));
      host.innerHTML = `<table class="tbl sample-table"><thead><tr>${columns.map(column => `<th title="${esc(column)}">${esc(column)}</th>`).join('')}</tr></thead><tbody>${result.rows.map(row => `<tr>${columns.map(column => `<td class="${['timestamp', 'event_id'].includes(column) ? 'mono' : ''}" title="${esc(row[column] || '')}">${esc(row[column] || '—')}</td>`).join('')}</tr>`).join('')}</tbody></table><div style="padding:9px 12px;border-top:1px solid var(--border2);font:10px var(--mono);color:var(--text3)">${result.returned} actual source records · ${esc(result.source_file)} · labels are shown for review and are not model inputs</div>`;
    } catch (error) {
      host.innerHTML = `<div class="sw"><div class="sw-title">Source sample unavailable</div><div class="sw-sub">${esc(error.message)}. Check the Flask API and retry.</div><button class="btn btn-out btn-sm" onclick="loadDsSample()">Retry sample</button></div>`;
    }
  };

  const originalLoadEvents = window.loadEvents;
  window.loadEvents = async function () { if (originalLoadEvents) await originalLoadEvents(); };

  window.addEventListener('resize', () => {
    if (obsMarketEvents.length) { drawMarketTape(obsMarketEvents); }
    const graphCanvas = byId('gcanvas'); if (graphCanvas && byId('page-graph')?.classList.contains('active')) { graphCanvas.width = graphCanvas.parentElement.offsetWidth || 700; graphCanvas.height = window.innerWidth < 768 ? 390 : window.innerWidth < 980 ? 520 : 640; initGPos(); renderG(); }
    if (byId('page-overview')?.classList.contains('active')) drawOverviewGraph();
  });
})();
