const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];
const dialog = $("#user-dialog");
let latestOverview = null;
let latestEvents = [];
let activeJourney = null;
let toastTimer = null;
let renderedEventSignature = null;

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, ch => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[ch]));
}

async function api(url, options = {}) {
  const response = await fetch(url, {headers:{"Content-Type":"application/json"}, ...options});
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}

function showToast(message) {
  const toast = $("#toast");
  toast.textContent = message;
  toast.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.remove("show"), 3000);
}

function normalizedUser(user) {
  return {id:Number(user.id),name:user.name,email:user.email,age:user.age == null ? null : Number(user.age)};
}

function sameUser(a, b) {
  if (!a || !b) return false;
  const x = normalizedUser(a), y = normalizedUser(b);
  return x.id === y.id && x.name === y.name && x.email === y.email && x.age === y.age;
}

function renderUsers(rows, documents) {
  const esById = new Map(documents.map(user => [String(user.id), user]));
  const pgIds = new Set(rows.map(user => String(user.id)));
  $("#postgres-count").textContent = rows.length;
  $("#db-foot-count").textContent = `${rows.length} records`;
  $("#es-count").textContent = documents.length;
  $("#es-foot-count").textContent = `${documents.length} documents`;

  $("#postgres-table").innerHTML = rows.length ? rows.map(user => `
    <tr><td>${user.id}</td><td><strong>${escapeHtml(user.name)}</strong></td><td class="email-cell">${escapeHtml(user.email)}</td><td>${user.age ?? "—"}</td>
    <td><div class="row-actions"><button class="row-action" data-edit="${user.id}">Edit</button><button class="row-action delete" data-delete="${user.id}">Delete</button></div></td></tr>`).join("") : `<tr><td colspan="5" class="empty-cell">No rows yet. Add one to start the CDC flow.</td></tr>`;

  const ids = new Set([...rows.map(user => String(user.id)), ...documents.map(user => String(user.id))]);
  const docById = new Map(documents.map(user => [String(user.id), user]));
  const joined = [...ids].sort((a,b) => Number(a)-Number(b));
  $("#elasticsearch-table").innerHTML = joined.length ? joined.map(id => {
    const pg = rows.find(user => String(user.id) === id), es = docById.get(id);
    const state = pg && es ? (sameUser(pg,es) ? ["Synced","synced"] : ["Catching up","pending"]) : pg ? ["Waiting","pending"] : ["Delete pending","pending"];
    const row = es || pg;
    return `<tr><td>${escapeHtml(id)}</td><td><strong>${escapeHtml(row.name)}</strong></td><td class="email-cell">${escapeHtml(row.email)}</td><td>${row.age ?? "—"}</td><td><span class="sync-pill ${state[1]}">${state[0]}</span></td></tr>`;
  }).join("") : `<tr><td colspan="5" class="empty-cell">No Elasticsearch documents yet.</td></tr>`;

  $$('[data-edit]').forEach(button => button.addEventListener("click", () => openEdit(rows.find(user => user.id === Number(button.dataset.edit)))));
  $$('[data-delete]').forEach(button => button.addEventListener("click", () => removeUser(rows.find(user => user.id === Number(button.dataset.delete)))));
  updateJourney(rows, documents);
}

function statusLabel(state) {
  return String(state || "UNKNOWN").toUpperCase().replaceAll("_", " ");
}

function setStatus(elementId, data) {
  const element = $(elementId);
  const state = data?.state || "UNKNOWN";
  element.textContent = statusLabel(state);
  element.className = `service-state ${state.toLowerCase()}`;
}

function updateStatuses(services) {
  setStatus("#status-postgres", services.postgres);
  setStatus("#status-source", services.source_connector);
  setStatus("#status-kafka", services.kafka);
  setStatus("#status-sink", services.sink_connector);
  setStatus("#status-es", services.elasticsearch);
  $$('[data-connector]').forEach(button => {
    const status = button.dataset.connector === "postgres-cdc" ? services.source_connector : services.sink_connector;
    const paused = String(status?.state).toUpperCase() === "PAUSED";
    button.textContent = paused ? "Resume" : "Pause";
    button.classList.toggle("resume", paused);
    button.disabled = !["RUNNING", "PAUSED"].includes(String(status?.state).toUpperCase());
    button.title = button.disabled ? `Connector state: ${statusLabel(status?.state)}` : `${paused ? "Resume" : "Pause"} ${button.dataset.connector}`;
  });
}

function eventOperation(value) {
  if (value === null) return "tombstone";
  return value?.payload?.op || value?.op || (value?.payload?.after ? "c" : "event");
}

function eventPayload(value) {
  return value?.payload ?? value;
}

function eventKey(event) {
  return event.key?.payload ?? event.key;
}

function eventKeyId(event) {
  const key = eventKey(event);
  if (key && typeof key === "object") return key.id ?? key.ID ?? "?";
  return key ?? "?";
}

function eventName(op) {
  return ({c:"INSERT",r:"SNAPSHOT",u:"UPDATE",d:"DELETE",tombstone:"TOMBSTONE"})[op] || String(op).toUpperCase();
}

function eventSummary(event, op) {
  const payload = eventPayload(event.value);
  const id = eventKeyId(event);
  if (op === "tombstone") return `Null-value marker for row ${id}; the sink uses this to remove its document.`;
  if (op === "d") {
    const oldRow = payload?.before;
    return oldRow?.name ? `Deleted ${oldRow.name} · ${oldRow.email || `row ${id}`}` : `Delete event for row ${id}.`;
  }
  const row = payload?.after || payload?.before;
  if (!row) return `CDC event for row ${id}.`;
  if (op === "u") return `Updated ${row.name || `row ${id}`} · ${row.email || ""}`;
  if (op === "c" || op === "r") return `${op === "r" ? "Snapshot" : "Inserted"} ${row.name || `row ${id}`} · ${row.email || ""}`;
  return `CDC event for row ${id}.`;
}

function eventRowFields(event, op) {
  const payload = eventPayload(event.value);
  const before = payload?.before;
  const after = payload?.after;
  if (op === "u" && before && after) {
    const changed = ["name", "email", "age"].filter(field => before[field] !== after[field]);
    if (changed.length) return `<div class="event-change-list">${changed.map(field => `<div class="event-change"><span>${escapeHtml(field)}</span><strong>${escapeHtml(before[field] ?? "—")}</strong><i>→</i><strong>${escapeHtml(after[field] ?? "—")}</strong></div>`).join("")}</div>`;
  }
  const row = op === "d" ? before : after;
  if (!row) return `<p class="event-no-row">${op === "tombstone" ? "Null-value tombstone. See the preceding DELETE event for the row details." : "Kafka carries the row key; this event has no row image."}</p>`;
  const fields = ["id", "name", "email", "age"];
  const label = op === "d" ? "BEFORE DELETE" : op === "u" ? "AFTER UPDATE" : "ROW DATA";
  return `<div class="event-detail-label">${label}</div><div class="event-fields">${fields.filter(field => row[field] !== undefined && row[field] !== null && row[field] !== "").map(field => `<div><span>${escapeHtml(field)}</span><strong>${escapeHtml(row[field])}</strong></div>`).join("")}</div>`;
}

function renderEvents(events, error) {
  const root = $("#events-list");
  if (!events.length) {
    renderedEventSignature = null;
    root.innerHTML = `<div class="empty-events">${escapeHtml(error || "No topic messages yet. Add or change a row to create the first CDC event.")}</div>`;
    return;
  }
  const signature = events.map(event => `${event.partition}:${event.offset}`).join("|");
  if (signature === renderedEventSignature) return;
  const expanded = new Set($$('[data-detail]').filter(detail => !detail.hidden).map(detail => detail.dataset.detail));
  renderedEventSignature = signature;
  root.innerHTML = events.map(event => {
    const op = eventOperation(event.value);
    const eventId = `${event.partition}-${event.offset}`;
    const time = event.timestamp ? new Date(event.timestamp).toLocaleTimeString() : "";
    const cleanEvent = {key:eventKey(event), value:eventPayload(event.value)};
    return `<article class="event-card"><button type="button" class="event-summary" data-event="${eventId}" aria-expanded="${expanded.has(eventId)}"><span class="op-chip op-${escapeHtml(op)}">${eventName(op)}</span><span class="event-key">row ${escapeHtml(eventKeyId(event))}</span><span class="event-preview">${escapeHtml(eventSummary(event, op))}</span><span class="event-location">p${event.partition} · ${event.offset}</span><span class="event-time">${escapeHtml(time)}</span><span class="event-chevron" aria-hidden="true">⌄</span></button><div class="event-detail" data-detail="${eventId}"${expanded.has(eventId) ? "" : " hidden"}>${eventRowFields(event, op)}<details class="raw-event"><summary>Show Debezium event payload</summary><pre>${escapeHtml(JSON.stringify(cleanEvent,null,2))}</pre></details></div></article>`;
  }).join("");
  $$('[data-event]').forEach(button => button.addEventListener("click", () => {
    const detail = $(`[data-detail="${button.dataset.event}"]`);
    detail.hidden = !detail.hidden;
    button.setAttribute("aria-expanded", String(!detail.hidden));
  }));
}

function eventMatchesJourney(event) {
  if (!activeJourney) return false;
  if (String(eventKeyId(event)) !== String(activeJourney.id)) return false;
  if (event.offset <= (activeJourney.offsets[event.partition] ?? -1)) return false;
  const op = eventOperation(event.value);
  return activeJourney.op === "delete" ? ["d", "tombstone"].includes(op) : activeJourney.op === "create" ? ["c", "r"].includes(op) : op === "u";
}

function updateJourney(rows, documents) {
  if (!activeJourney) return;
  const journey = $("#journey-card");
  journey.classList.remove("hidden");
  const pgExists = rows.some(user => String(user.id) === String(activeJourney.id));
  const esUser = documents.find(user => String(user.id) === String(activeJourney.id));
  const esMatches = activeJourney.op === "delete" ? !esUser : esUser && sameUser(esUser, activeJourney.expected);
  const kafkaSeen = activeJourney.kafkaSeen;
  const steps = $$('.journey-step', journey);
  steps[0].className = `journey-step ${activeJourney.saved ? "complete" : "active"}`;
  steps[1].className = `journey-step ${kafkaSeen ? "complete" : activeJourney.saved ? "active" : ""}`;
  steps[2].className = `journey-step ${esMatches ? "complete" : kafkaSeen ? "active" : ""}`;
  const completed = activeJourney.saved && kafkaSeen && esMatches;
  $("#journey-title").textContent = `${activeJourney.label} · row ${activeJourney.id}`;
  $("#journey-badge").textContent = completed ? "PROPAGATED" : "IN PROGRESS";
  $("#journey-badge").classList.toggle("done", completed);
  $("#journey-detail").textContent = completed
    ? "The Kafka event is visible and Elasticsearch now matches the PostgreSQL change."
    : !pgExists && activeJourney.op !== "delete"
      ? "Waiting for the PostgreSQL row to appear…"
      : !kafkaSeen
        ? "PostgreSQL committed the row. Waiting for Debezium to publish the change to Kafka…"
        : "Kafka has the event. Waiting for the Elasticsearch sink to apply it…";
  if (completed && !activeJourney.completedAt) activeJourney.completedAt = Date.now();
}

async function refresh() {
  try {
    const [overview, eventData] = await Promise.all([
      api("/api/overview"),
      fetch("/api/events").then(response => response.json()).catch(error => ({events:[],error:error.message}))
    ]);
    latestOverview = overview;
    latestEvents = eventData.events || [];
    updateStatuses(overview.services || {});
    renderUsers(overview.postgres_users || [], overview.elasticsearch_users || []);
    renderEvents(latestEvents, eventData.error);
    if (activeJourney && latestEvents.some(eventMatchesJourney)) activeJourney.kafkaSeen = true;
    updateJourney(overview.postgres_users || [], overview.elasticsearch_users || []);
    $("#last-updated").textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (error) {
    $("#last-updated").textContent = "Dashboard API unavailable";
    showToast(error.message);
  }
}

function resetDialog() {
  $("#user-form").reset();
  $("#user-id").value = "";
  $("#form-error").textContent = "";
  $("#dialog-title").textContent = "Add a row";
  $("#save-user").textContent = "Save to PostgreSQL";
}

function openCreate() {
  resetDialog();
  dialog.showModal();
  $("#user-name").focus();
}

function openEdit(user) {
  if (!user) return;
  resetDialog();
  $("#user-id").value = user.id;
  $("#user-name").value = user.name;
  $("#user-email").value = user.email;
  $("#user-age").value = user.age ?? "";
  $("#dialog-title").textContent = `Edit row ${user.id}`;
  $("#save-user").textContent = "Save changes";
  dialog.showModal();
  $("#user-name").focus();
}

function beginJourney(op, user) {
  const offsets = {};
  for (const event of latestEvents) offsets[event.partition] = Math.max(offsets[event.partition] ?? -1, event.offset);
  activeJourney = {op, id:user.id, expected:normalizedUser(user), saved:true, kafkaSeen:false, offsets, label:({create:"INSERT",update:"UPDATE",delete:"DELETE"})[op]};
  $("#journey-card").classList.remove("hidden");
  activeJourney.completedAt = null;
}

async function removeUser(user) {
  if (!user || !confirm(`Delete ${user.name} (row ${user.id}) from PostgreSQL? The connector should remove its Elasticsearch document too.`)) return;
  try {
    await api(`/api/users/${user.id}`, {method:"DELETE"});
    beginJourney("delete", user);
    showToast(`Row ${user.id} deleted in PostgreSQL. Following its tombstone…`);
    await refresh();
  } catch (error) { showToast(error.message); }
}

$("#add-user-button").addEventListener("click", openCreate);
$("#close-dialog").addEventListener("click", () => dialog.close());
$("#cancel-dialog").addEventListener("click", () => dialog.close());
$("#refresh-button").addEventListener("click", refresh);
$("#user-form").addEventListener("submit", async event => {
  event.preventDefault();
  const id = $("#user-id").value;
  const payload = {name:$("#user-name").value,email:$("#user-email").value,age:$("#user-age").value || null};
  const saveButton = $("#save-user");
  saveButton.disabled = true;
  $("#form-error").textContent = "";
  try {
    const result = await api(id ? `/api/users/${id}` : "/api/users", {method:id ? "PUT" : "POST", body:JSON.stringify(payload)});
    dialog.close();
    beginJourney(id ? "update" : "create", result.user);
    showToast(`${id ? "Updated" : "Inserted"} row ${result.user.id} in PostgreSQL. Following it downstream…`);
    await refresh();
  } catch (error) { $("#form-error").textContent = error.message; }
  finally { saveButton.disabled = false; }
});

$$('[data-connector]').forEach(button => button.addEventListener("click", async () => {
  const name = button.dataset.connector;
  const status = name === "postgres-cdc" ? latestOverview?.services?.source_connector : latestOverview?.services?.sink_connector;
  const action = String(status?.state).toUpperCase() === "PAUSED" ? "resume" : "pause";
  button.disabled = true;
  try {
    await api(`/api/connectors/${name}/${action}`, {method:"POST",body:"{}"});
    showToast(`${name} ${action} requested.`);
    setTimeout(refresh, 800);
  } catch (error) { showToast(error.message); }
  finally { setTimeout(() => { button.disabled = false; }, 1000); }
}));

refresh();
setInterval(refresh, 3500);
