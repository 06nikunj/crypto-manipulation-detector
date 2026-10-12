/* CryptoSentinel live market client. The public Binance stream and benchmark replay remain separate. */
(() => {
  const $ = id => document.getElementById(id);
  const API = '';
  const META = {
    overview: ['Live market overview', 'Binance Spot / live trades / five-second rule monitoring'],
    live: ['Live Markets', 'Binance Spot trade stream / selected pairs / rolling observations'],
    events: ['Live Events', 'Actual exchange trade messages / current connection session'],
    alerts: ['Live Alerts', 'Transparent market rules / warnings are not manipulation findings'],
    graph: ['Live Market Graph', 'Five-second market bars and asset relationships / no wallets'],
    evaluation: ['Model Evaluation', 'Historical benchmark test results / not live-market validation'],
    dataset: ['Dataset & Benchmark', 'Historical research data / optional replay is separate from live markets'],
    system: ['System Health', 'Measured API, public feed, rule engine and optional service status'],
    stream: ['Historical Benchmark Replay', 'Optional demonstration / saved benchmark rows only'],
    'benchmark-events': ['Benchmark Events', 'Historical model-scored rows / replay session only'],
    'benchmark-alerts': ['Benchmark Alerts', 'Historical benchmark model flags / not live alerts'],
    'benchmark-graph': ['Benchmark Graph', 'Historical benchmark relationship graph / no wallet records']
  };
  const historical = {
    events: window.loadEvents,
    alerts: window.loadAlerts,
    graph: window.loadGraph,
    stats: window.loadGStats,
    sus: window.loadSusList,
    initGraph: window.initGraph
  };
  let activePage = 'overview';
  let feed = null;
  let pollTimer = null;
  let pendingRender = false;
  let current = null;
  let status = null;
  let trades = [];
  let bars = {};
  let alerts = [];
  let graphData = {nodes: [], edges: [], stats: {}};
  let liveGraphSelected = null;
  let graphRefreshAt = 0;
  let detectorNoticeShown = false;

  const num = value => Number(value).toLocaleString('en-US');
  const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
  const utcTime = value => {
    if (!value) return '—';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '—' : date.toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit', second:'2-digit', timeZone:'UTC'}) + ' UTC';
  };
  const utcDateTime = value => {
    if (!value) return '—';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString('en-GB', {year:'numeric', month:'short', day:'2-digit', hour:'2-digit', minute:'2-digit', second:'2-digit', timeZone:'UTC'}) + ' UTC';
  };
  const localTime = value => {
    if (!value) return '—';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '—' : date.toLocaleTimeString();
  };
  const priceText = (symbol, value) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return '—';
    const digits = n >= 1000 ? 2 : n >= 1 ? 4 : 8;
    return n.toLocaleString('en-US', {minimumFractionDigits: Math.min(digits, 2), maximumFractionDigits: digits});
  };
  const compact = value => {
    const n = Number(value);
    return Number.isFinite(n) ? Intl.NumberFormat('en-US', {notation:'compact', maximumFractionDigits:2}).format(n) : '—';
  };
  const pct = value => Number.isFinite(Number(value)) ? `${(Number(value) * 100).toFixed(2)}%` : '—';
  const active = id => $(`page-${id}`)?.classList.contains('active');
  const notify = (message, type='info') => {
    if (typeof window.toast === 'function') window.toast(message, type);
    else console[type === 'error' ? 'error' : 'log'](message);
  };
  const watchlistInput = () => ($('live-symbols')?.value || $('live-symbols-top')?.value || 'BTCUSDT,ETHUSDT,BNBUSDT').split(',').map(x=>x.trim().toUpperCase()).filter(Boolean);

  function applyConnection(next) {
    if (!next) return;
    status = {...(status || {}), ...next};
    const state = status.connection_status || 'disconnected';
    const connected = state === 'connected';
    const activeAttempt = ['connecting', 'reconnecting'].includes(state);
    const dot = $('cdot'), label = $('clabel');
    if (dot) dot.className = connected ? 'cdot on' : 'cdot';
    if (label) label.textContent = connected ? 'Binance Spot · live' : state === 'connecting' ? 'Connecting to Binance…' : state === 'reconnecting' ? 'Reconnecting…' : 'Disconnected';
    const connect = $('btn-hdr-connect'), stop = $('btn-hdr-disconnect');
    if (connect) { connect.disabled = connected || activeAttempt; connect.textContent = activeAttempt ? 'Connecting…' : connected ? 'Connected' : 'Connect'; }
    if (stop) stop.hidden = !(connected || activeAttempt);
    const stamp = $('tb-ts');
    if (stamp) stamp.textContent = status.last_received_at ? utcTime(status.last_received_at) : '—';
    const stateText = connected ? 'Connected to live trades' : state === 'connecting' ? 'Connection attempt in progress' : state === 'reconnecting' ? 'Connection interrupted · retrying' : 'Disconnected';
    const stateClass = `state-pill ${connected ? 'is-live' : activeAttempt ? 'is-wait' : 'is-off'}`;
    for (const id of ['live-connection-state','overview-live-connection-state']) { const stateLabel=$(id); if(stateLabel){stateLabel.textContent=stateText;stateLabel.className=stateClass;} }
    const navState=$('sb-live-nav-state'); if(navState){navState.textContent=connected?'LIVE':activeAttempt?'WAIT':'OFF';navState.className=`sb-live-badge ${connected?'is-live':activeAttempt?'is-wait':''}`;}
    const note = $('live-connection-note');
    if (note) note.textContent = status.last_error || (connected ? `Receiving public trade messages for ${(status.watchlist || []).join(', ')}.` : state === 'connecting' ? 'Waiting for the Binance Spot WebSocket handshake. No prices are shown until real trades arrive.' : 'Use Connect to open the public market-data stream. No API key or trading permission is used.');
    setText('live-last-received', status.last_received_at ? utcDateTime(status.last_received_at) : 'No trade received this session');
    setText('live-last-exchange-event', status.last_exchange_event_at ? utcDateTime(status.last_exchange_event_at) : 'No exchange event yet');
    setText('live-last-exchange-trade', status.last_exchange_trade_at ? utcDateTime(status.last_exchange_trade_at) : 'No exchange trade yet');
    setText('live-watchlist', (status.watchlist || watchlistInput()).join(' · '));
    if ($('tb-title') && META[activePage]) $('tb-title').textContent = META[activePage][0];
  }

  function setText(id, text) { const el = $(id); if (el) el.textContent = text; }

  async function fetchJSON(path, options={}) {
    const response = await fetch(API + path, {signal: AbortSignal.timeout(9000), ...options});
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
    return body;
  }

  async function connectLive() {
    const symbols = watchlistInput();
    if (!symbols.length) { notify('Enter at least one Binance Spot pair.', 'error'); return; }
    try {
      applyConnection({connection_status:'connecting', watchlist:symbols, last_error:null});
      const result = await fetchJSON('/api/live/start', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({symbols})});
      applyConnection(result.status || {connection_status:'connecting', watchlist:symbols});
      notify(`Connection requested for ${symbols.join(', ')}.`, 'success');
    } catch (error) {
      applyConnection({connection_status:'disconnected', last_error:error.message});
      notify(`Could not connect: ${error.message}`, 'error');
    }
  }

  async function disconnectLive() {
    try {
      const result = await fetchJSON('/api/live/stop', {method:'POST'});
      applyConnection(result.status || {connection_status:'disconnected'});
      notify('Live market stream stopped.', 'info');
    } catch (error) { notify(`Could not stop the live stream: ${error.message}`, 'error'); }
  }

  async function refreshStatus() {
    try {
      const next = await fetchJSON('/api/live/status');
      applyConnection(next);
      if (active('system')) renderSystem(next);
      return next;
    } catch (error) {
      const dot = $('cdot'), label = $('clabel');
      if (dot) dot.className = 'cdot';
      if (label) label.textContent = 'API unavailable';
      if (active('system')) renderSystemError(error.message);
      return null;
    }
  }

  function acceptSnapshot(snapshot) {
    if (!snapshot) return;
    current = snapshot;
    trades = Array.isArray(snapshot.trades) ? snapshot.trades.slice(0, 300) : trades;
    bars = snapshot.bars || bars || {};
    alerts = Array.isArray(snapshot.alerts) ? snapshot.alerts.slice(0, 100) : alerts;
    applyConnection(snapshot);
    scheduleRender();
  }

  function startFeed() {
    if (feed) feed.close();
    feed = new EventSource('/api/live/stream');
    feed.addEventListener('snapshot', event => {
      try { acceptSnapshot(JSON.parse(event.data)); } catch (error) { console.warn('Live snapshot parse error', error); }
    });
    feed.addEventListener('status', event => {
      try { applyConnection(JSON.parse(event.data)); } catch (error) { console.warn('Live status parse error', error); }
    });
    feed.addEventListener('trade', event => {
      try {
        const tick = JSON.parse(event.data);
        trades.unshift(tick); if (trades.length > 300) trades.length = 300;
        current ||= {latest_prices:{}, bars:{}, alerts:[], processed_events:0};
        current.latest_prices ||= {};
        current.latest_prices[tick.symbol] = tick;
        current.processed_events = Number(current.processed_events || 0) + 1;
        current.last_received_at = tick.received_at;
        current.last_exchange_event_at = tick.exchange_event_time;
        current.last_exchange_trade_at = tick.exchange_trade_time;
        scheduleRender();
      } catch (error) { console.warn('Trade event parse error', error); }
    });
    feed.addEventListener('bar', event => {
      try {
        const bar = JSON.parse(event.data);
        bars[bar.symbol] ||= [];
        if (!bars[bar.symbol].some(existing => existing.bucket_start_ms === bar.bucket_start_ms)) bars[bar.symbol].push(bar);
        bars[bar.symbol] = bars[bar.symbol].slice(-360);
        scheduleRender();
        if (active('graph')) loadLiveGraph();
      } catch (error) { console.warn('Market bar parse error', error); }
    });
    feed.addEventListener('alert', event => {
      try { alerts.unshift(JSON.parse(event.data)); alerts = alerts.slice(0, 100); scheduleRender(); } catch (error) { console.warn('Live alert parse error', error); }
    });
    feed.onerror = () => {
      const detail = $('live-stream-transport');
      if (detail) detail.textContent = 'Dashboard event channel reconnecting; Binance connection status is reported separately.';
    };
  }

  function scheduleRender() {
    if (pendingRender) return;
    pendingRender = true;
    requestAnimationFrame(() => { pendingRender = false; renderAll(); });
  }

  function renderAll() {
    renderMetrics();
    renderOverviewTrades();
    renderEventTables();
    renderAlerts();
    drawPriceChart('live-chart');
    drawPriceChart('overview-live-chart');
    if (active('graph') && Date.now() - graphRefreshAt > 2500) loadLiveGraph();
    if (active('system') && status) renderSystem(status);
  }

  function setPairChoices(id, symbols, includeAll=false) {
    const select=$(id); if(!select)return;
    const previous=select.value;
    const options=[];
    if(includeAll){const all=document.createElement('option');all.value='';all.textContent='All selected pairs';options.push(all);}
    symbols.forEach(symbol=>{const option=document.createElement('option');option.value=symbol;option.textContent=symbol;options.push(option);});
    select.replaceChildren(...options);
    select.value=symbols.includes(previous)?previous:(includeAll?'':(symbols[0]||''));
  }

  function renderMetrics() {
    const latest = current?.latest_prices || {};
    const btc = latest.BTCUSDT, eth = latest.ETHUSDT;
    const setBoth=(id,value)=>{setText(id,value);setText(`${id}-full`,value);};
    setBoth('live-kpi-btc', btc ? `$${priceText('BTCUSDT', btc.price)}` : '—');
    setBoth('live-kpi-eth', eth ? `$${priceText('ETHUSDT', eth.price)}` : '—');
    setBoth('live-kpi-trades', num(current?.processed_events || 0));
    setBoth('live-kpi-alerts', num(alerts.length));
    setBoth('live-kpi-btc-sub', btc ? `${utcTime(btc.exchange_trade_time)} · exchange trade time` : 'Waiting for an observed trade');
    setBoth('live-kpi-eth-sub', eth ? `${utcTime(eth.exchange_trade_time)} · exchange trade time` : 'Waiting for an observed trade');
    setBoth('live-kpi-trades-sub', 'Valid trade messages received this live session');
    setBoth('live-kpi-alerts-sub', alerts.length ? `${alerts.length} retained rule warnings · session only` : 'No rule threshold crossed this session');
    const alertBadge=$('sb-live-alerts');if(alertBadge){alertBadge.textContent=String(alerts.length);alertBadge.style.display=alerts.length?'inline':'none';}
    const count = $('overview-live-count');
    if (count) count.textContent = `${num(current?.processed_events || 0)} actual trade messages · this process session`;
    setText('overview-live-watchlist', (status?.watchlist || watchlistInput()).join(' · '));
    setText('overview-live-updated', current?.last_received_at ? `Last received locally ${utcTime(current.last_received_at)}` : 'No live messages received');
    const detectorText=current?.rule_detector?.status === 'active' ? 'Rule monitoring active' : 'Rule monitoring warming up';
    setBoth('live-rule-status',detectorText);
    setBoth('live-model-status','XGBoost unavailable for live trades');
    const modelText=current?.xgboost_live?.reason || status?.xgboost_live?.reason || 'The benchmark model is not applied to raw exchange trades.';
    setBoth('live-model-note',modelText);
    const desired=status?.watchlist || current?.watchlist || watchlistInput();
    setPairChoices('market-symbol',desired,false);setPairChoices('overview-market-symbol',desired,false);
    setPairChoices('live-event-symbol-filter',desired,true);setPairChoices('live-alert-symbol-filter',desired,true);setPairChoices('live-graph-filter',desired,true);
    liveGraphSelected=$('live-graph-filter')?.value||'';
  }

  function drawPriceChart(canvasId) {
    const canvas = $(canvasId); if (!canvas) return;
    const parent = canvas.parentElement;
    const width = Math.max(280, parent?.clientWidth || canvas.clientWidth || 640);
    const height = canvasId === 'overview-live-chart' ? (innerWidth < 768 ? 205 : 245) : (innerWidth < 768 ? 250 : 340);
    const dpr = Math.max(1, Math.min(2, devicePixelRatio || 1));
    canvas.width = Math.round(width * dpr); canvas.height = Math.round(height * dpr); canvas.style.height = `${height}px`;
    const ctx = canvas.getContext('2d'); ctx.setTransform(dpr,0,0,dpr,0,0); ctx.clearRect(0,0,width,height);
    const symbol = canvasId === 'overview-live-chart' ? ($('overview-market-symbol')?.value || $('market-symbol')?.value || (status?.watchlist || [])[0]) : ($('market-symbol')?.value || (status?.watchlist || [])[0]);
    const points = (bars[symbol] || []).slice(-360).filter(item => Number.isFinite(Number(item.close)) && item.bucket_start_exchange_time);
    const empty = $(canvasId === 'overview-live-chart' ? 'overview-chart-empty' : 'live-chart-empty');
    if (!points.length) {
      if (empty) empty.style.display = 'block'; canvas.style.display = 'none';
      if (empty) { const title=empty.querySelector('.sw-title'), sub=empty.querySelector('.sw-sub'); if(title)title.textContent='No observed price bars yet'; if(sub)sub.textContent='Connect to Binance Spot and wait for trades. The chart never fills gaps with simulated prices.'; }
      return;
    }
    canvas.style.display = 'block'; if (empty) empty.style.display = 'none';
    const pad={l:70,r:18,t:25,b:34}, plotW=width-pad.l-pad.r, plotH=height-pad.t-pad.b;
    const values=points.map(p=>Number(p.close)); let min=Math.min(...values),max=Math.max(...values);
    if (min===max) {const gap=Math.max(Math.abs(min)*0.001,0.00000001);min-=gap;max+=gap;} else {const gap=(max-min)*0.12;min-=gap;max+=gap;}
    const xAt=i=>pad.l+(points.length<2?plotW/2:i/(points.length-1)*plotW);
    const yAt=v=>pad.t+(max-v)/(max-min)*plotH;
    ctx.font='10px ui-monospace,monospace'; ctx.lineWidth=1;
    for(let i=0;i<=4;i++){const y=pad.t+plotH*i/4;ctx.strokeStyle='#e8efea';ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(width-pad.r,y);ctx.stroke();const value=max-(max-min)*i/4;ctx.fillStyle='#819087';ctx.textAlign='right';ctx.fillText(priceText(symbol,value),pad.l-8,y+3);}
    ctx.beginPath(); points.forEach((point,i)=>{const x=xAt(i),y=yAt(Number(point.close));if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);});
    ctx.strokeStyle='#16855e';ctx.lineWidth=2.1;ctx.stroke();
    const last=points[points.length-1];ctx.beginPath();ctx.arc(xAt(points.length-1),yAt(Number(last.close)),3.5,0,Math.PI*2);ctx.fillStyle='#16855e';ctx.fill();
    ctx.fillStyle='#72837a';ctx.font='9px ui-monospace,monospace';ctx.textAlign='left';ctx.fillText(utcTime(points[0].bucket_start_exchange_time),pad.l,height-8);ctx.textAlign='right';ctx.fillText(utcTime(last.bucket_start_exchange_time),width-pad.r,height-8);
    ctx.fillStyle='#314d40';ctx.textAlign='left';ctx.font='10px ui-monospace,monospace';ctx.fillText(`${symbol} · ${points.length} observed, non-empty five-second bars`,pad.l,14);
    canvas.title=`Observed Binance Spot ${symbol} trade prices aggregated in UTC five-second bars. Empty intervals are not interpolated.`;
    canvas.onmousemove=e=>{const rect=canvas.getBoundingClientRect();const x=(e.clientX-rect.left)*width/rect.width;const idx=Math.min(points.length-1,Math.max(0,Math.round((x-pad.l)/Math.max(1,plotW)*(points.length-1))));const b=points[idx];canvas.title=`${symbol} · bar starts ${utcDateTime(b.bucket_start_exchange_time)}\nOHLC ${priceText(symbol,b.open)} / ${priceText(symbol,b.high)} / ${priceText(symbol,b.low)} / ${priceText(symbol,b.close)}\nBase volume ${Number(b.base_volume).toPrecision(6)} · trades ${b.trade_count}\nReceived/closed locally ${utcDateTime(b.closed_at_received)}${b.price_change_pct==null?'':`\nClose-to-close change ${pct(b.price_change_pct)}`}`;};
  }

  function renderOverviewTrades() {
    const el = $('overview-live-events'); if (!el) return;
    if (!trades.length) { el.innerHTML='<div class="sw"><div class="sw-ico">⌁</div><div class="sw-title">Waiting for real trades</div><div class="sw-sub">Connect the public Binance Spot feed. No sample data is inserted.</div></div>'; return; }
    el.innerHTML = trades.slice(0,7).map(t=>`<div class="live-mini-row"><strong>${esc(t.symbol)}</strong><span>${priceText(t.symbol,t.price)}</span><small>${utcTime(t.exchange_trade_time)}</small></div>`).join('');
  }

  function renderEventTables() {
    const query=($('live-event-q')?.value||'').trim().toUpperCase();
    const symbol=$('live-event-symbol-filter')?.value||'';
    const rows=trades.filter(t=>(!symbol||t.symbol===symbol)&&(!query||t.symbol.includes(query)||String(t.trade_id||'').includes(query))).slice(0,100);
    const body=$('live-events-tbody');
    if (body) body.innerHTML=rows.length?rows.map(t=>`<tr><td class="mono">${utcTime(t.exchange_trade_time)}</td><td><strong>${esc(t.symbol)}</strong></td><td class="mono">${priceText(t.symbol,t.price)}</td><td class="mono">${Number(t.quantity).toPrecision(6)}</td><td class="mono">${Number(t.quote_quantity).toPrecision(6)}</td><td class="mono">${esc(t.trade_id??'—')}</td><td class="mono">${utcTime(t.received_at)}</td></tr>`).join(''):'<tr><td colspan="7"><div class="sw"><div class="sw-title">No trade messages in this session</div><div class="sw-sub">Connect the exchange feed to populate this table with observed trades.</div></div></td></tr>';
    setText('live-event-count', `${num(trades.length)} retained actual trades`);
    setText('live-event-empty-note', trades.length ? 'Trade time is provided by Binance; received time is recorded locally by this app.' : 'This view is live-session only. Historical benchmark rows are in the optional Benchmark area.');
  }

  function renderAlerts() {
    const list=$('live-alert-list');
    if (!list) return;
    const items=$('live-alert-symbol-filter')?.value ? alerts.filter(a=>a.symbol===$('live-alert-symbol-filter').value) : alerts;
    setText('live-alert-count', `${num(alerts.length)} rule warnings this session`);
    setText('live-alert-high', num(alerts.filter(a=>a.severity==='high').length));
    if (!items.length) {
      list.innerHTML=`<div class="sw"><div class="sw-ico">✓</div><div class="sw-title">${alerts.length?'No alerts for this pair':'No rule thresholds crossed'}</div><div class="sw-sub">Alerts appear only when observed closed five-second market bars meet the published rules. An alert is a review cue—not a manipulation finding.</div></div>`;
      return;
    }
    list.innerHTML=items.map(a=>`<article class="live-alert-card ${a.severity==='high'?'alert-high':''}"><div class="live-alert-top"><div><strong>${esc(a.symbol)}</strong><span class="badge ${a.severity==='high'?'b-pump':'b-pred'}">${esc(a.severity)} · rule warning</span></div><span class="mono">${utcDateTime(a.detected_at)}</span></div><div class="live-alert-facts"><span>Bar close <b>${priceText(a.symbol,a.price)}</b></span><span>Move <b>${a.price_change_pct==null?'—':pct(a.price_change_pct)}</b></span><span>Volume <b>${a.volume_ratio_vs_median==null?'Warm-up':Number(a.volume_ratio_vs_median).toFixed(2)+'× median'}</b></span><span>Trades <b>${num(a.trade_count)}</b></span></div><p>${esc(a.explanation)}</p><div class="live-alert-source">Exchange bar: ${utcDateTime(a.exchange_bar_start)} → ${utcTime(a.exchange_bar_end)} · detected locally ${utcTime(a.detected_at)} · method: rule-based five-second aggregate · ML score: unavailable</div></article>`).join('');
  }

  async function loadLiveGraph() {
    const canvas=$('live-graph'); if (!canvas) return;
    try { graphData=await fetchJSON('/api/live/graph?limit=300'); graphRefreshAt=Date.now(); renderLiveGraph(); }
    catch(error) { setText('live-graph-status', `Live graph unavailable: ${error.message}`); }
  }

  function renderLiveGraph() {
    const canvas=$('live-graph'); if(!canvas)return;
    const width=Math.max(280,canvas.parentElement?.clientWidth||700),height=innerWidth<768?380:520,dpr=Math.max(1,Math.min(2,devicePixelRatio||1));
    canvas.width=Math.round(width*dpr);canvas.height=Math.round(height*dpr);canvas.style.height=`${height}px`;
    const ctx=canvas.getContext('2d');ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,width,height);ctx.fillStyle='#fbfdfb';ctx.fillRect(0,0,width,height);
    const all=graphData.nodes||[],stats=graphData.stats||{};
    const assets=all.filter(n=>n.type==='CryptoAsset'),events=all.filter(n=>n.type==='MarketEvent');
    const selected=liveGraphSelected; const filtered=selected?events.filter(n=>n.properties?.symbol===selected):events;
    const displayEvents=filtered.slice(-Math.max(20,Math.floor(width/8)));
    const nodes=[...assets,...displayEvents],byId=new Map(nodes.map(n=>[n.id,n]));
    const pos=new Map();
    assets.forEach((node,i)=>pos.set(node.id,{x:width*.12,y:height*(.18+i*.66/Math.max(assets.length-1,1))}));
    const lane=width*.52;
    displayEvents.forEach((node,i)=>{const y=height*(.1+(i+1)/Math.max(displayEvents.length+1,2)*.8);pos.set(node.id,{x:lane,y});});
    (graphData.edges||[]).forEach(edge=>{const a=byId.get(edge.source),b=byId.get(edge.target);if(!a||!b)return;const p=pos.get(a.id),q=pos.get(b.id);if(!p||!q)return;ctx.beginPath();ctx.moveTo(p.x,p.y);ctx.lineTo(q.x,q.y);ctx.strokeStyle=edge.type==='NEXT'?'#b4c9bc':'#d3e2d8';ctx.lineWidth=edge.type==='NEXT'?1.25:.8;ctx.stroke();});
    nodes.forEach(node=>{const p=pos.get(node.id),props=node.properties||{},flagged=node.type==='MarketEvent'&&props.suspicious;const radius=node.type==='CryptoAsset'?9:flagged?5:3.5;ctx.beginPath();ctx.arc(p.x,p.y,radius,0,Math.PI*2);ctx.fillStyle=node.type==='CryptoAsset'?'#16855e':flagged?'#c94f4a':'#63a98a';ctx.fill();if(node.type==='CryptoAsset'){ctx.fillStyle='#254638';ctx.font='700 11px system-ui';ctx.textAlign='left';ctx.fillText(props.symbol||node.id,p.x+14,p.y+4);}});
    if(!events.length){ctx.fillStyle='#72847a';ctx.font='13px system-ui';ctx.textAlign='center';ctx.fillText('No closed five-second market bars yet',width/2,height/2-8);ctx.font='11px system-ui';ctx.fillText('Receive trades in a later five-second bucket to build actual bar nodes.',width/2,height/2+14);}
    setText('live-graph-status',events.length?`${num(events.length)} observed five-second bars · ${num(stats.total_edges||0)} actual links · current live session`:'Waiting for closed bars');
    setText('live-graph-nodes',num(stats.total_nodes||0));setText('live-graph-edges',num(stats.total_edges||0));setText('live-graph-bars',num(stats.market_bars||0));setText('live-graph-assets',num(stats.assets||0));setText('live-graph-flags',num(stats.rule_flagged_bars||0));
    const tip=$('live-graph-detail'); if(tip)tip.textContent=events.length?`Asset nodes represent subscribed pairs. MarketEvent nodes represent actual closed, non-empty five-second bars; red nodes crossed a displayed rule. NEXT edges connect observed bars for one pair. No wallet, address, group or order-book relationships are inferred.`:'No live relationships yet. Historical graph analysis remains under the Benchmark area.';
  }

  function renderSystem(data) {
    setText('health-api-state', 'Healthy · status endpoint returned successfully');
    setText('health-stream-state', stateName(data.connection_status));
    const ingestionLabel=Number(data.processed_events||0)>0?'Receiving validated trades':data.connection_status==='connected'?'Connected · awaiting first trade':'Idle · no active trade stream';
    setText('health-ingestion-state', ingestionLabel);
    setText('health-detector-state', data.rule_detector?.status==='active'?'Active · transparent rules':'Warming up · no closed bars');
    setText('health-model-state', 'Unavailable for live schema');
    setText('health-kafka-state', 'Not started by dashboard');
    setText('health-spark-state', 'Not started by dashboard');
    setText('health-benchmark-state', 'Separate · historical only');
    setText('health-events-state', num(data.processed_events||0));
    setText('health-last-error', data.last_error || 'None');
    setText('health-updated-at', utcDateTime(new Date().toISOString()));
    setText('health-stream-detail', data.connection_status==='connected'?`Connected since ${utcDateTime(data.connected_at)} · pairs: ${(data.watchlist||[]).join(', ')} · reconnects: ${num(data.reconnect_attempts||0)}`:(data.last_error||'No active exchange socket; API itself remains available.'));
    setText('health-model-detail', data.xgboost_live?.reason || 'The saved model is reserved for its historical benchmark feature schema.');
  }
  function renderSystemError(error) { setText('health-api-state','Unavailable');setText('health-stream-state','Unknown');setText('health-last-error',error); }
  function stateName(value) { return ({connected:'Connected',connecting:'Connecting',reconnecting:'Reconnecting',disconnected:'Disconnected',error:'Error'})[value]||'Unknown'; }

  async function loadSnapshot() {
    try { acceptSnapshot(await fetchJSON('/api/live/snapshot')); }
    catch(error) { if(active('system')) renderSystemError(error.message); }
  }

  async function refreshLiveOverview() { await Promise.all([loadSnapshot(),refreshStatus()]); }
  async function refreshAllLive() {
    await refreshStatus();
    if (active('overview') || active('live') || active('events') || active('alerts')) await loadSnapshot();
    if (active('graph')) await loadLiveGraph();
    if (active('evaluation')) window.loadEval?.();
    if (active('dataset')) window.loadDs?.();
    if (active('stream')) { window.refreshStr?.(); window.pollFeed?.(); }
    if (active('benchmark-events')) historical.events?.();
    if (active('benchmark-alerts')) historical.alerts?.();
    if (active('benchmark-graph')) { historical.stats?.(); historical.graph?.(); historical.sus?.(); }
  }

  function navigate(name) { const button=document.querySelector(`.sb-item[data-page="${name}"]`);window.showPage(name,button); }
  window.navigateLive = navigate;
  window.connectLive = connectLive;
  window.disconnectLive = disconnectLive;
  window.refreshLiveGraph = loadLiveGraph;
  window.filterLiveEvents = renderEventTables;
  window.filterLiveAlerts = renderAlerts;
  window.refreshLiveStatus = refreshStatus;
  window.showPage = function(name, btn) {
    document.querySelector('.sidebar')?.classList.remove('open');
    document.querySelectorAll('.page').forEach(page=>page.classList.remove('active'));
    document.querySelectorAll('.sb-item').forEach(button=>button.classList.remove('active'));
    const page=$(`page-${name}`); if(page)page.classList.add('active');
    if(btn)btn.classList.add('active');
    activePage=name;
    const meta=META[name]||[name,''];setText('tb-title',meta[0]);setText('tb-sub',meta[1]);
    if(name==='overview')refreshLiveOverview();
    else if(name==='live')Promise.all([loadSnapshot(),refreshStatus()]);
    else if(name==='events'){loadSnapshot();renderEventTables();}
    else if(name==='alerts'){loadSnapshot();renderAlerts();}
    else if(name==='graph')loadLiveGraph();
    else if(name==='evaluation')window.loadEval?.();
    else if(name==='dataset')window.loadDs?.();
    else if(name==='system')refreshStatus();
    else if(name==='stream'){window.refreshStr?.();window.pollFeed?.();}
    else if(name==='benchmark-events')historical.events?.();
    else if(name==='benchmark-alerts')historical.alerts?.();
    else if(name==='benchmark-graph'){historical.stats?.();historical.graph?.();historical.sus?.();setTimeout(()=>historical.initGraph?.(),50);}
    if(name==='graph')setTimeout(renderLiveGraph,60);
  };
  window.refreshOv = refreshLiveOverview;
  window.checkHealth = refreshStatus;
  window.refreshAll = refreshAllLive;
  window.startTimer = function() {
    if(pollTimer)clearInterval(pollTimer);
    pollTimer=setInterval(()=>{
      refreshStatus();
      if(active('overview')||active('live')||active('events')||active('alerts'))loadSnapshot();
      if(active('graph')&&Date.now()-graphRefreshAt>5000)loadLiveGraph();
      if(active('stream')){window.refreshStr?.();window.pollFeed?.();}
    },5000);
  };
  // Preserve the old benchmark handlers for clearly separated secondary pages.
  window.loadEvents = function(){return active('benchmark-events')?historical.events?.():loadSnapshot();};
  window.loadAlerts = function(){return active('benchmark-alerts')?historical.alerts?.():loadSnapshot();};
  window.loadGraph = function(){return active('benchmark-graph')?historical.graph?.():loadLiveGraph();};
  window.loadGStats = function(){return active('benchmark-graph')?historical.stats?.():loadLiveGraph();};
  window.loadSusList = function(){return active('benchmark-graph')?historical.sus?.():loadLiveGraph();};

  document.addEventListener('DOMContentLoaded',()=>{
    startFeed();
    refreshLiveOverview();
    const symbols=$('live-symbols');
    if(symbols && !symbols.value) symbols.value=(status?.watchlist||['BTCUSDT','ETHUSDT','BNBUSDT']).join(', ');
    const symbolsTop=$('live-symbols-top');if(symbolsTop && symbols)symbolsTop.value=symbols.value;
    $('live-symbols')?.addEventListener('change',()=>{if($('live-symbols-top'))$('live-symbols-top').value=$('live-symbols').value;});
    $('live-symbols-top')?.addEventListener('change',()=>{if($('live-symbols'))$('live-symbols').value=$('live-symbols-top').value;});
    $('live-event-q')?.addEventListener('input',renderEventTables);
    $('live-event-symbol-filter')?.addEventListener('change',renderEventTables);
    $('live-alert-symbol-filter')?.addEventListener('change',renderAlerts);
    $('live-graph-filter')?.addEventListener('change',event=>{liveGraphSelected=event.target.value;loadLiveGraph();});
    $('market-symbol')?.addEventListener('change',()=>{drawPriceChart('live-chart');if($('overview-market-symbol'))$('overview-market-symbol').value=$('market-symbol').value;});
    $('overview-market-symbol')?.addEventListener('change',()=>drawPriceChart('overview-live-chart'));
    window.addEventListener('resize',()=>{drawPriceChart('live-chart');drawPriceChart('overview-live-chart');if(active('graph'))renderLiveGraph();});
    window.startTimer();
  });
})();
