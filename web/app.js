const $ = (selector) => document.querySelector(selector);
const examples = {
  genuine: {text: "The headphones were comfortable for a three-hour train ride. Noise cancellation worked well, though the case is bulkier than expected.", rating: 4, verified: true, product: "quietform-studio"},
  promotional: {text: "AMAZING AMAZING AMAZING!!! Best headphones ever, everyone must buy this perfect product right now!!!", rating: 5, verified: false, product: "quietform-studio"},
  ai: {text: "These headphones present a compelling blend of refined acoustic performance and thoughtful ergonomic design, making every listening session consistently enjoyable.", rating: 5, verified: false, product: "quietform-studio"},
  "positive-campaign": {text: "Outstanding battery life and premium quality, highly recommended for everyone!", rating: 5, verified: false, product: "demo-smartwatch"},
  "negative-campaign": {text: "Battery failed immediately and support was completely useless, avoid this product!", rating: 1, verified: false, product: "demo-competitor-watch"},
  "legitimate-burst": {text: "Bought during the launch sale. Delivery took two days and the advertised battery life matches my first week of use.", rating: 4, verified: true, product: "launch-day-speaker"}
};
let feed = [];

function setExample(key) {
  const item = examples[key];
  $("#review-text").value = item.text;
  $("#rating").value = String(item.rating);
  $("#verified").checked = item.verified;
  $("#product-id").value = item.product;
}

function payload() {
  const timestamp = $("#timestamp").value ? new Date($("#timestamp").value).toISOString() : new Date().toISOString();
  return {
    review_id: `demo-${Date.now()}`, user_id: $("#user-id").value || "demo-user",
    product_id: $("#product-id").value || "demo-product", text: $("#review-text").value,
    rating: Number($("#rating").value), timestamp, verified_purchase: $("#verified").checked,
    helpful_votes: Number($("#helpful").value || 0), language: $("#language").value || "en"
  };
}

function resetSteps() {
  document.querySelectorAll("#scan-steps li").forEach((item) => item.className = "");
}

async function animateSteps() {
  resetSteps();
  for (const item of document.querySelectorAll("#scan-steps li")) {
    item.className = "active";
    await new Promise((resolve) => setTimeout(resolve, 105));
    item.className = "done";
  }
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const data = await response.json();
  if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : "Request failed");
  return data;
}

function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function renderResult(data) {
  const root = $("#result");
  root.replaceChildren();
  root.append(element("span", `status ${data.needs_review ? "warn" : "safe"}`, data.needs_review ? "needs review" : "normal"));
  root.append(element("div", "risk-number", `${Math.round(data.fake_probability * 100)}%`));
  root.append(element("p", "meta", `${Math.round(data.calibrated_confidence * 100)}% calibrated confidence · ${data.processing_ms} ms`));
  const track = element("div", "risk-track"); const fill = element("div", "risk-fill");
  fill.style.width = `${Math.round(data.fake_probability * 100)}%`; track.append(fill); root.append(track);
  const dl = element("dl");
  [["Campaign", data.campaign_id || "None detected"], ["Similar reviews", String(data.similar_review_count)],
   ["Model", data.model_version], ["Data", data.dataset_version],
   ["Features", data.feature_version], ["Schema", data.api_schema_version]].forEach(([term, value]) => {
    dl.append(element("dt", "", term), element("dd", "", value));
  });
  root.append(dl, element("h3", "", "Evidence"));
  const list = element("ul");
  data.evidence.forEach((item) => list.append(element("li", "", item)));
  root.append(list, element("p", "notice", "Risk signals are evidence for human review—not proof that a person or account is a bot."));
}

async function scanReview(event) {
  event.preventDefault();
  const button = $("#review-form button[type=submit]");
  button.disabled = true; $("#scan-time").textContent = "SCANNING";
  const animation = animateSteps();
  try {
    const data = await fetchJson("/v1/reviews/score", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(payload())});
    await animation; renderResult(data); $("#scan-time").textContent = `${data.processing_ms} MS`;
    await refreshAll();
  } catch (error) {
    document.querySelectorAll("#scan-steps li.active").forEach((item) => item.className = "failed");
    $("#result").replaceChildren(element("span", "status warn", "unavailable"), element("h3", "", "Scan could not complete"), element("p", "", error.message));
    $("#scan-time").textContent = "FAILED";
  } finally { button.disabled = false; }
}

async function replayCampaign() {
  const button = $("#replay"); button.disabled = true; button.textContent = "Replaying…";
  const scenario = $("#example").value.includes("negative") ? "coordinated-negative" : "coordinated-positive";
  try {
    await fetchJson("/v1/demo/replay", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({scenario})});
    await refreshAll(); location.hash = "campaigns";
  } catch (error) { window.alert(`Replay failed: ${error.message}`); }
  finally { button.disabled = false; button.textContent = "Replay campaign"; }
}

function includeRecord(item, filter) {
  if (filter === "needs") return item.needs_review;
  if (filter === "campaign") return Boolean(item.campaign_id);
  if (filter === "verified") return item.verified_purchase;
  if (filter === "unverified") return !item.verified_purchase;
  if (filter === "positive") return item.rating >= 4;
  if (filter === "negative") return item.rating <= 2;
  return true;
}

function renderFeed() {
  const root = $("#review-list"); root.replaceChildren();
  const shown = feed.filter((item) => includeRecord(item, $("#feed-filter").value));
  if (!shown.length) { root.append(element("p", "empty", "No reviews match this filter.")); return; }
  shown.forEach((item) => {
    const card = element("article", "review-card");
    const top = element("div", "card-top");
    top.append(element("span", `status ${item.needs_review ? "warn" : "safe"}`, item.needs_review ? "needs review" : "normal"),
      element("span", "meta", `${Math.round(item.fake_probability * 100)}% risk`));
    card.append(top, element("h3", "", `${"★".repeat(Math.round(item.rating))} ${item.product_id}`),
      element("p", "", item.text), element("p", "meta", `${item.verified_purchase ? "Verified" : "Unverified"} · ${item.processing_ms} ms · ${item.model_version}`));
    if (item.campaign_id) card.append(element("p", "notice", `Campaign: ${item.campaign_id}`));
    const details = element("details"); details.append(element("summary", "", "Evidence and lineage"));
    const list = element("ul"); item.evidence.forEach((value) => list.append(element("li", "", value)));
    details.append(list, element("p", "meta", `${item.lineage.git_commit} · ${item.lineage.data} · ${item.lineage.features}`)); card.append(details); root.append(card);
  });
}

async function loadFeed() {
  const data = await fetchJson("/v1/reviews/recent?limit=50"); feed = data.items; renderFeed();
}

async function moderate(id, decision) {
    await fetchJson(`/v1/campaigns/${encodeURIComponent(id)}/decision`, {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({decision, moderator: "demo-moderator", reason: "UI demonstration"})});
  await loadCampaigns();
}

async function loadCampaigns() {
  const data = await fetchJson("/v1/campaigns"); const root = $("#campaign-list"); root.replaceChildren();
  if (!data.items.length) { root.append(element("p", "empty", "Replay a campaign to populate this console.")); return; }
  data.items.forEach((item) => {
    const card = element("article", "campaign-card"), info = element("div"), evidence = element("div");
    info.append(element("span", "status warning", item.status), element("h3", "", item.campaign_id),
      element("p", "", `${Math.round(item.risk_score * 100)}% coordination risk · ${item.product_id}`),
      element("p", "meta", `${item.review_ids.length} reviews · ${item.user_ids.length} accounts · soft limit ${item.soft_limit ? "active" : "off"}`));
    const list = element("ul");
    Object.entries(item.evidence).forEach(([key, value]) => list.append(element("li", "", `${key.replaceAll("_", " ")}: ${value}`)));
    const actions = element("div", "campaign-actions");
    [["confirm", "Confirm + limit"], ["dismiss", "Dismiss"], ["restore", "Restore"]].forEach(([decision, label]) => {
      const button = element("button", "", label); button.type = "button"; button.addEventListener("click", () => moderate(item.campaign_id, decision)); actions.append(button);
    });
    evidence.append(list, actions); card.append(info, evidence); root.append(card);
  });
}

async function loadOps() {
  const data = await fetchJson("/v1/operations"), root = $("#service-grid"); root.replaceChildren();
  data.services.forEach((item) => {
    const card = element("article", "service-card"), head = element("div", "service-head");
    const state = item.status === "healthy" ? "safe" : item.status === "unavailable" ? "warn" : "neutral";
    head.append(element("strong", "", item.name), element("span", `status ${state}`, item.status));
    card.append(head, element("p", "meta", item.detail)); root.append(card);
  });
  const links = $("#ops-links"); links.replaceChildren();
  Object.entries(data.links).forEach(([name, url]) => { const link = element("a", "", `Open ${name}`); link.href = url; link.target = "_blank"; link.rel = "noopener"; links.append(link); });
}

function renderBars(root, rows) {
  root.replaceChildren();
  rows.forEach(([label, value, max, suffix]) => {
    const row = element("div", "bar-row"), track = element("div", "bar-track"), fill = element("div", "bar-fill");
    fill.style.width = `${Math.min(100, (value / max) * 100)}%`; track.append(fill);
    row.append(element("span", "", label), track, element("strong", "", `${value}${suffix}`)); root.append(row);
  });
}

async function loadMonitoring() {
  const data = await fetchJson("/v1/monitoring");
  $("#hero-scans").textContent = data.reviews_processed; $("#hero-campaigns").textContent = data.campaign_alerts; $("#hero-latency").textContent = `${data.latency_ms.p95} ms`;
  $("#monitor-updated").textContent = `Updated ${new Date(data.updated_at).toLocaleTimeString()}`;
  const metrics = [["Throughput", data.reviews_per_second, "/ sec"], ["API errors", data.api_error_rate, "%"], ["Kafka lag", data.kafka_consumer_lag, "events"], ["Campaigns", data.campaign_alerts, "alerts"], ["Soft limits", data.soft_limited_campaigns, "active"], ["Mean risk", Math.round(data.mean_review_risk * 100), "%"]];
  const root = $("#metric-grid"); root.replaceChildren();
  metrics.forEach(([name, value, suffix]) => { const card = element("article", "metric-card"); card.append(element("strong", "", `${value} ${suffix}`), element("span", "", name)); root.append(card); });
  renderBars($("#latency-bars"), [["p50", data.latency_ms.p50, 150, " ms"], ["p95", data.latency_ms.p95, 150, " ms"], ["p99", data.latency_ms.p99, 150, " ms"]]);
  renderBars($("#drift-bars"), [["Feature", Math.round(data.feature_drift * 100), 100, "%"], ["Embedding", Math.round(data.embedding_drift * 100), 100, "%"]]);
}

async function loadLineage() {
  const data = await fetchJson("/v1/lineage"), root = $("#lineage-table"); root.replaceChildren();
  Object.entries(data).forEach(([key, value]) => { const row = element("tr"); row.append(element("th", "", key.replaceAll("_", " ")), element("td", "", value)); root.append(row); });
}

async function health() {
  try { await fetchJson("/health/live"); $("#api-status").className = "health-pill healthy"; $("#api-status").textContent = "API healthy"; }
  catch { $("#api-status").className = "health-pill warning"; $("#api-status").textContent = "API unavailable"; }
}

async function refreshAll() {
  await Promise.allSettled([loadFeed(), loadCampaigns(), loadMonitoring(), loadOps(), loadLineage()]);
}

$("#review-form").addEventListener("submit", scanReview);
$("#example").addEventListener("change", (event) => setExample(event.target.value));
$("#feed-filter").addEventListener("change", renderFeed);
$("#replay").addEventListener("click", replayCampaign);
$("#timestamp").value = new Date(Date.now() - new Date().getTimezoneOffset() * 60000).toISOString().slice(0, 16);
setExample("genuine"); health(); refreshAll(); setInterval(loadMonitoring, 10000);
