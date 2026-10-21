const elements = {
  connection: document.querySelector(".live-pill"),
  connectionLabel: document.querySelector("#connection-label"),
  lastRefresh: document.querySelector("#last-refresh"),
  ingestionRate: document.querySelector("#ingestion-rate"),
  eventsRate: document.querySelector("#events-rate"),
  activeViewers: document.querySelector("#active-viewers"),
  bufferTime: document.querySelector("#buffer-time"),
  errorRate: document.querySelector("#error-rate"),
  queryLatency: document.querySelector("#query-latency"),
  storageLatency: document.querySelector("#storage-latency"),
  rebufferRate: document.querySelector("#rebuffer-rate"),
  engagement: document.querySelector("#engagement"),
  engagementRing: document.querySelector("#engagement-ring"),
  pipelineState: document.querySelector("#pipeline-state"),
  contentRows: document.querySelector("#content-rows"),
  chart: document.querySelector("#events-chart"),
};

const number = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1, notation: "compact" });
const integer = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
let timeseries = [];

async function getJson(path) {
  const response = await fetch(path, { headers: { Accept: "application/json" } });
  if (!response.ok) throw new Error(`${path} returned ${response.status}`);
  return response.json();
}

async function refresh() {
  try {
    const [overview, content, series] = await Promise.all([
      getJson("/api/v1/overview"),
      getJson("/api/v1/content"),
      getJson("/api/v1/timeseries"),
    ]);
    renderOverview(overview, content);
    renderContent(content);
    timeseries = series;
    drawChart();
    elements.connection.classList.remove("disconnected");
    elements.connectionLabel.textContent = "Live";
    elements.pipelineState.textContent = "Healthy";
    elements.lastRefresh.textContent = new Date().toLocaleTimeString();
  } catch (error) {
    console.error(error);
    elements.connection.classList.add("disconnected");
    elements.connectionLabel.textContent = "Disconnected";
    elements.pipelineState.textContent = "Waiting for data";
  }
}

function renderOverview(data, content) {
  const rate = Number(data.eventsPerSecond || 0);
  const queryLatency = Number(data.queryLatencyMs || 0);
  const engagement = content.length
    ? content.reduce((sum, row) => sum + Number(row.engagementRatio || 0), 0) / content.length
    : 0;
  elements.eventsRate.textContent = number.format(rate);
  elements.ingestionRate.textContent = `${number.format(rate)} events/s`;
  elements.activeViewers.textContent = number.format(data.activeViewers || 0);
  elements.bufferTime.textContent = `${integer.format(data.averageBufferMs || 0)} ms`;
  elements.errorRate.textContent = percent(data.errorRate);
  elements.rebufferRate.textContent = percent(data.rebufferRatio);
  elements.queryLatency.textContent = `${queryLatency.toFixed(1)} ms query`;
  elements.storageLatency.textContent = `${queryLatency.toFixed(1)} ms`;
  elements.engagement.textContent = percent(engagement, 1);
  elements.engagementRing.style.setProperty("--percentage", `${Math.min(100, engagement * 100)}%`);
}

function renderContent(rows) {
  if (!rows.length) {
    elements.contentRows.innerHTML = '<tr><td colspan="5" class="empty">No events yet. The generator may still be starting.</td></tr>';
    return;
  }
  elements.contentRows.innerHTML = rows.map((row) => {
    const engagement = Math.min(1, Number(row.engagementRatio || 0));
    return `<tr>
      <td>${escapeHtml(row.contentId)}</td>
      <td>${integer.format(row.totalEvents)}</td>
      <td>${integer.format(row.viewerWindows)}</td>
      <td><span class="metric-bar"><i style="--value:${engagement * 100}%"></i>${percent(engagement, 1)}</span></td>
      <td>${percent(row.errorRate)}</td>
    </tr>`;
  }).join("");
}

function drawChart() {
  const canvas = elements.chart;
  const rect = canvas.getBoundingClientRect();
  const ratio = Math.min(window.devicePixelRatio || 1, 2);
  canvas.width = Math.max(1, rect.width * ratio);
  canvas.height = Math.max(1, rect.height * ratio);
  const context = canvas.getContext("2d");
  context.scale(ratio, ratio);
  const width = rect.width;
  const height = rect.height;
  const padding = { top: 18, right: 12, bottom: 28, left: 38 };
  context.clearRect(0, 0, width, height);

  context.strokeStyle = "rgba(123, 164, 200, .12)";
  context.fillStyle = "#637c94";
  context.font = "10px ui-monospace, monospace";
  for (let line = 0; line <= 4; line += 1) {
    const y = padding.top + ((height - padding.top - padding.bottom) * line) / 4;
    context.beginPath(); context.moveTo(padding.left, y); context.lineTo(width - padding.right, y); context.stroke();
  }
  if (!timeseries.length) {
    context.fillText("Waiting for telemetry", padding.left, height / 2);
    return;
  }

  const maxEvents = Math.max(...timeseries.map((point) => Number(point.events)), 1);
  const maxViewers = Math.max(...timeseries.map((point) => Number(point.viewers)), 1);
  line(timeseries.map((point) => Number(point.events) / maxEvents), "#24dae2", true);
  line(timeseries.map((point) => Number(point.viewers) / maxViewers), "#9a82ff", false);

  const labels = [0, Math.floor((timeseries.length - 1) / 2), timeseries.length - 1];
  labels.forEach((index) => {
    const x = xFor(index);
    const label = new Date(timeseries[index].timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    context.fillText(label, Math.max(padding.left, x - 18), height - 8);
  });

  function xFor(index) {
    return padding.left + ((width - padding.left - padding.right) * index) / Math.max(timeseries.length - 1, 1);
  }

  function line(values, color, fill) {
    const chartHeight = height - padding.top - padding.bottom;
    context.beginPath();
    values.forEach((value, index) => {
      const x = xFor(index);
      const y = padding.top + chartHeight * (1 - value);
      if (index === 0) context.moveTo(x, y); else context.lineTo(x, y);
    });
    context.lineWidth = 2;
    context.strokeStyle = color;
    context.stroke();
    if (fill) {
      context.lineTo(xFor(values.length - 1), height - padding.bottom);
      context.lineTo(xFor(0), height - padding.bottom);
      const gradient = context.createLinearGradient(0, padding.top, 0, height - padding.bottom);
      gradient.addColorStop(0, "rgba(36, 218, 226, .22)");
      gradient.addColorStop(1, "rgba(36, 218, 226, 0)");
      context.fillStyle = gradient;
      context.fill();
    }
  }
}

function percent(value, digits = 2) {
  return `${(Number(value || 0) * 100).toFixed(digits)}%`;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>'"]/g, (character) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;",
  })[character]);
}

window.addEventListener("resize", drawChart);
refresh();
setInterval(refresh, 5_000);
