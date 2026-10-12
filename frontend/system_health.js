/* Measured integration status and replay controls; never substitutes demo metrics. */
(() => {
  let lastStatusAt = 0;
  let lastMetricsAt = 0;
  let busy = false;
  const byId = id => document.getElementById(id);
  const pretty = value => value == null ? '—' : Number(value).toLocaleString();
  const label = status => ({
    connected: 'Connected', healthy: 'Healthy', processing: 'Processing', running: 'Running',
    unavailable: 'Unavailable', not_configured: 'Not configured', starting: 'Starting',
    stopped: 'Stopped', completed: 'Completed', idle: 'Idle', unknown: 'Unknown'
  }[String(status || 'unknown').toLowerCase()] || String(status));
  const color = status => ['connected', 'healthy', 'processing', 'running'].includes(String(status || '').toLowerCase())
    ? 'var(--green)' : ['unavailable', 'failed', 'error'].includes(String(status || '').toLowerCase())
      ? 'var(--red)' : ['starting', 'paused'].includes(String(status || '').toLowerCase())
        ? 'var(--orange)' : 'var(--text3)';
  function setState(id, status) {
    const el = byId(id); if (!el) return;
    el.textContent = label(status);
    el.style.color = color(status);
  }
  function setDetail(id, value) {
    const el = byId(id); if (el) el.textContent = value || 'No additional detail reported.';
  }
  function serviceDetail(service) {
    if (!service) return 'No status response received.';
    if (service.detail) return service.detail;
    if (service.error) return service.error;
    const parts = [];
    if (service.topic) parts.push(`topic ${service.topic}`);
    if (service.bucket) parts.push(`bucket ${service.bucket}`);
    if (service.organization) parts.push(`organization ${service.organization}`);
    if (service.brokers) parts.push(`brokers ${service.brokers.join(', ')}`);
    if (service.model && service.model.loaded != null) parts.push(`benchmark XGBoost ${service.model.loaded ? 'loaded' : 'not loaded'}`);
    return parts.join(' · ') || 'Status verified by the configured service health check.';
  }
  function updateReplay(status) {
    const stateEl = byId('health-kafka-replay-state');
    const detailEl = byId('health-kafka-replay-detail');
    if (!status) return;
    if (stateEl) {
      stateEl.textContent = label(status.status);
      stateEl.className = `badge ${status.status === 'running' ? 'b-run' : status.status === 'paused' ? 'b-pause' : status.status === 'completed' ? 'b-info' : 'b-stop'}`;
    }
    if (detailEl) {
      const published = Number(status.published || 0), total = Number(status.total || 0);
      detailEl.textContent = `${published.toLocaleString()} / ${total.toLocaleString()} source rows published to Kafka · ${Number(status.errors || 0).toLocaleString()} publish errors${status.replay_id ? ` · replay ${status.replay_id}` : ''}${status.last_error ? ` · ${status.last_error}` : ''}`;
    }
  }
  async function refreshSystem(force = false) {
    const now = Date.now();
    if (busy || (!force && now - lastStatusAt < 8000)) return;
    busy = true; lastStatusAt = now;
    try {
      const response = await fetch('/api/pipeline/status', { signal: AbortSignal.timeout(20000) });
      if (!response.ok) throw new Error(`Pipeline status HTTP ${response.status}`);
      const data = await response.json();
      const services = data.services || {};
      setState('health-api-state', 'healthy');
      setState('health-kafka-state', services.kafka?.status || 'unknown');
      setState('health-spark-state', services.spark?.status || 'unknown');
      setState('health-neo4j-state', services.neo4j?.status || 'unknown');
      setState('health-influx-state', services.influxdb?.status || 'unknown');
      setState('health-grafana-state', services.grafana?.status || 'unknown');
      setDetail('health-kafka-detail', serviceDetail(services.kafka));
      const sparkDetail = serviceDetail(services.spark);
      const modelInfo = services.spark?.model;
      setDetail('health-spark-detail', modelInfo?.reason ? `${sparkDetail} · ${modelInfo.reason}` : sparkDetail);
      setDetail('health-neo4j-detail', serviceDetail(services.neo4j));
      setDetail('health-influx-detail', serviceDetail(services.influxdb));
      setDetail('health-grafana-detail', serviceDetail(services.grafana));
      const spark = services.spark || {};
      const counts = spark.counts || {};
      const hasCounts = Object.keys(counts).length > 0;
      byId('health-spark-processed').textContent = hasCounts ? pretty(counts.processed_events) : '—';
      byId('health-spark-flagged').textContent = hasCounts ? pretty(counts.flagged_predictions) : '—';
      byId('health-api-consumed').textContent = pretty(data.pipeline?.consumed_records ?? 0);
      const pipelineError = data.pipeline?.last_consumer_error || data.pipeline?.last_error;
      setDetail('health-pipeline-error', pipelineError ? `Latest consumer/persistence error: ${pipelineError}` : `API consumer status: ${label(data.pipeline?.status)} · recent output records retained in process: ${pretty(data.pipeline?.recent_processed_events)}`);
      const model = data.model || {};
      setState('health-model-state', model.available ? (model.f1 == null ? 'Loaded · benchmark only' : `Loaded · benchmark F1 ${(model.f1 * 100).toFixed(1)}%`) : 'Unavailable for live schema');
      setDetail('health-model-detail', model.available ? 'Saved classifier is used only for the historical benchmark feature schema.' : 'Saved benchmark XGBoost artifact was not found by the API.');
      setState('health-stream-state', data.live_market?.connection_status || 'disconnected');
      setDetail('health-stream-detail', data.live_market?.last_error || `Binance Spot public feed · ${data.live_market?.watchlist?.join(', ') || 'no pairs selected'}`);
      byId('health-events-state').textContent = pretty(data.live_market?.processed_events ?? 0);
      byId('health-updated-at').textContent = new Date(data.checked_at || Date.now()).toLocaleTimeString([], { timeZone: 'UTC' }) + ' UTC';
      byId('health-last-error').textContent = data.live_market?.last_error || data.pipeline?.last_consumer_error || 'None';
      updateReplay(data.pipeline?.replay);
      if (force || Date.now() - lastMetricsAt > 30000) refreshMetrics();
    } catch (error) {
      setState('health-api-state', 'unavailable');
      setDetail('health-pipeline-error', error.message || 'Could not query pipeline status.');
      setDetail('health-metrics', 'InfluxDB metrics unavailable because the pipeline status request failed.');
    } finally { busy = false; }
  }
  async function refreshMetrics() {
    lastMetricsAt = Date.now();
    const target = byId('health-metrics');
    if (!target) return;
    target.textContent = 'Querying InfluxDB…';
    const range = byId('health-metrics-range')?.value || '1h';
    try {
      const response = await fetch(`/api/metrics?range=${encodeURIComponent(range)}&limit=100`, { signal: AbortSignal.timeout(6000) });
      const data = await response.json();
      if (!response.ok || !data.available) {
        target.textContent = data.error || `InfluxDB query failed (HTTP ${response.status}). No metrics were substituted.`;
        return;
      }
      const records = data.records || [];
      if (!records.length) {
        target.textContent = `InfluxDB is connected, but no records were returned for the last ${range}. Run a Kafka/Spark replay or enable live ingestion.`;
        return;
      }
      const table = document.createElement('table'); table.className = 'tbl';
      const head = document.createElement('thead');
      head.innerHTML = '<tr><th>Time (UTC)</th><th>Measurement</th><th>Field</th><th>Observed value</th><th>Source / service</th><th>Status</th></tr>';
      table.append(head);
      const body = document.createElement('tbody');
      records.slice(0, 30).forEach(record => {
        const row = document.createElement('tr');
        const values = [record.time, record.measurement, record.field, record.value,
          record.source || record.service || '—', record.status || '—'];
        values.forEach(value => { const cell = document.createElement('td'); cell.textContent = value == null ? '—' : String(value); row.append(cell); });
        body.append(row);
      });
      table.append(body); target.replaceChildren(table);
    } catch (error) {
      target.textContent = `InfluxDB query failed: ${error.message || 'network error'}. No metrics were substituted.`;
    }
  }
  async function startKafkaReplay() {
    const maxRows = Number(byId('health-replay-rows')?.value || 1000);
    const speed = Number(byId('health-replay-speed')?.value || 100);
    if (!Number.isInteger(maxRows) || maxRows < 1 || maxRows > 100000 || !Number.isFinite(speed) || speed <= 0 || speed > 100000) {
      setDetail('health-kafka-replay-detail', 'Choose 1–100000 rows and a speed greater than 0 and at most 100000×.'); return;
    }
    await replayRequest('start', { max_rows: maxRows, speed });
  }
  async function controlKafkaReplay(action) { await replayRequest(action); }
  async function replayRequest(action, payload = {}) {
    try {
      const response = await fetch(`/api/pipeline/replay/${action}`, {
        method: action === 'status' ? 'GET' : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: action === 'status' ? undefined : JSON.stringify(payload),
        signal: AbortSignal.timeout(6000)
      });
      const data = await response.json();
      if (!response.ok || data.ok === false) throw new Error(data.error || `HTTP ${response.status}`);
      updateReplay(data.status || data);
      if (action === 'start') window.toast?.('Historical rows are being published to Kafka.', 'success');
    } catch (error) {
      setDetail('health-kafka-replay-detail', error.message || 'Kafka replay request failed.');
      window.toast?.(error.message || 'Kafka replay request failed.', 'error');
    }
  }
  window.refreshSystem = () => refreshSystem(true);
  window.refreshSystemAuto = () => refreshSystem(false);
  window.refreshMetrics = refreshMetrics;
  window.startKafkaReplay = startKafkaReplay;
  window.controlKafkaReplay = controlKafkaReplay;
  window.refreshIntegrationStatus = refreshSystem;
})();
