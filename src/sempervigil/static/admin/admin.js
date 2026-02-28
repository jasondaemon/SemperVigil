async function apiFetch(url, options = {}) {
  const headers = Object.assign({}, options.headers || {});
  if (options.body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  const response = await fetch(
    url,
    Object.assign({}, options, { headers, credentials: "same-origin" })
  );
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || response.statusText);
  }
  return response.json();
}
function showToast(message) {
  const toast = document.createElement("div");
  toast.className = "toast";
  toast.textContent = message;
  document.body.appendChild(toast);
  setTimeout(() => toast.remove(), 2500);
}

function toast(message, _type) {
  showToast(message);
}
function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
function formatAbsolute(value) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  const tz = document.body?.dataset?.timezone || "";
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: tz || undefined,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const parts = Object.fromEntries(
    formatter.formatToParts(date).map((part) => [part.type, part.value])
  );
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
}
function formatRelative(value) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMin = Math.round(diffMs / 60000);
  if (diffMin >= 0 && diffMin < 60) {
    return `${diffMin}m ago`;
  }
  const diffHr = Math.round(diffMin / 60);
  if (diffHr >= 0 && diffHr < 24) {
    return `${diffHr}h ago`;
  }
  const tz = document.body?.dataset?.timezone || "";
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: tz || undefined,
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = Object.fromEntries(
    formatter.formatToParts(date).map((part) => [part.type, part.value])
  );
  return `${parts.month} ${parts.day} ${parts.hour}:${parts.minute}`;
}

function formatDateOnly(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "2-digit" });
}
function formatTimestamp(value) {
  return formatAbsolute(value);
}
function applyTimestampFormatting(root = document) {
  root.querySelectorAll("[data-ts]").forEach((el) => {
    const raw = el.getAttribute("data-ts");
    if (!raw) {
      return;
    }
    el.textContent = formatRelative(raw);
    if (!el.title) {
      el.title = formatAbsolute(raw);
    }
  });
}
function shortId(value) {
  const id = String(value || "");
  if (id.length <= 12) {
    return id;
  }
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function renderShortId(value, href) {
  const full = String(value || "");
  const short = shortId(full);
  const link = href ? `<a class="id-short" href="${href}" title="${esc(full)}">${short}</a>` : `<span class="id-short" title="${esc(full)}">${short}</span>`;
  return `
    <span class="id-wrap" data-full="${esc(full)}">
      ${link}
      <button class="id-copy" type="button" title="Copy ID" data-copy="${esc(full)}">⧉</button>
    </span>
  `;
}
function statusBadge(status) {
  const value = String(status || "unknown");
  let cls = "badge muted";
  if (value === "succeeded" || value === "ok" || value === "active") cls = "badge success";
  if (value === "failed" || value === "error") cls = "badge error";
  if (value === "running" || value === "queued") cls = "badge warn";
  return `<span class="${cls}">${esc(value)}</span>`;
}
function applyShortIds(root = document) {
  root.querySelectorAll(".id-short").forEach((el) => {
    const full = el.getAttribute("title") || el.textContent || "";
    if (!full) return;
    el.textContent = shortId(full);
  });
}
function formatWhen(job) {
  const when = job.finished_at || job.started_at || job.requested_at || "";
  const titleParts = [
    job.requested_at ? `requested: ${formatAbsolute(job.requested_at)}` : null,
    job.started_at ? `started: ${formatAbsolute(job.started_at)}` : null,
    job.finished_at ? `finished: ${formatAbsolute(job.finished_at)}` : null,
  ].filter(Boolean);
  const title = titleParts.join(" | ");
  return `<span data-ts="${esc(when)}" title="${esc(title)}">${esc(formatRelative(when))}</span>`;
}
function wireCopyButtons(root = document) {
  root.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const value = target.dataset.copy;
    if (!value) {
      return;
    }
    try {
      await navigator.clipboard.writeText(value);
      showToast("Copied");
    } catch (err) {
      console.error(err);
    }
  });
}
function wireActionMenus(root = document) {
  const closeAll = () => {
    root.querySelectorAll(".action-menu.open").forEach((menu) => menu.classList.remove("open"));
  };
  root.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const toggle = target.closest(".action-menu-button");
    if (toggle) {
      const menu = toggle.closest(".action-menu");
      if (!menu) return;
      const isOpen = menu.classList.contains("open");
      closeAll();
      if (!isOpen) {
        menu.classList.add("open");
      }
      event.preventDefault();
      return;
    }
    if (!target.closest(".action-menu")) {
      closeAll();
    }
  });
  root.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeAll();
    }
  });
}
function wireDashboard() {
  const jobCountsContainer = document.getElementById("dashboard-job-counts-table");
  const jobsPanel = document.getElementById("dashboard-job-counts");
  const queueBanner = document.getElementById("queue-stale-banner");
  if (!jobCountsContainer || !jobsPanel) {
    return;
  }
  const staleMinutes = parseInt(document.body.dataset.queueStaleMinutes || "0", 10);
  const autoCatchupToggle = document.getElementById("dashboard-auto-catchup");
  async function loadAutoCatchupToggle() {
    if (!autoCatchupToggle) {
      return;
    }
    try {
      const data = await apiFetch("/admin/config/runtime");
      const enabled = !!(
        data &&
        data.config &&
        data.config.jobs &&
        data.config.jobs.auto_catchup_enabled
      );
      autoCatchupToggle.checked = enabled;
    } catch (_err) {
      autoCatchupToggle.checked = false;
    }
  }
  function renderJobCounts(counts, jobTypes, jobGroups, countsSince, queueable) {
    const allTypes = jobTypes && jobTypes.length ? jobTypes : Object.keys(counts).sort();
    const fallbackGroups = [
      { id: "all", title: "Job Queue", job_types: allTypes },
    ];
    const groups = Array.isArray(jobGroups) && jobGroups.length ? jobGroups : fallbackGroups;
    const sinceEl = document.getElementById("dashboard-job-counts-since");
    if (sinceEl) {
      if (countsSince) {
        sinceEl.textContent = `since ${formatTimestamp(countsSince)}`;
      } else {
        sinceEl.textContent = "";
      }
    }
    const headerHtml = `
      <thead>
        <tr>
          <th>Job Type</th>
          <th>Need</th>
          <th>Queue</th>
          <th>Que</th>
          <th>Run</th>
          <th>Fail</th>
          <th>Complete</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
    const needsConfig = {
      fetch_article_content: {
        link: "/ui/content?type=article&content_state=missing",
        action: "missing_content",
        limit: true,
      },
      summarize_article_llm: {
        link: "/ui/content?type=article&missing=summary",
        action: "missing_summary",
      },
      summarize_article_context_llm: {
        link: "/ui/content?type=article&missing=context",
        action: "missing_context",
        limit: true,
      },
      article_enrich_products: {
        link: "/ui/content?type=article&missing=products",
        action: "article_products",
        limit: true,
      },
      article_enrich_threat_actors: {
        link: "/ui/content?type=article&missing=threat_actors",
        action: "article_threats",
        limit: true,
      },
      derive_events_from_articles: {
        link: "/ui/events",
        action: "article_events",
        limit: true,
      },
      cve_enrich_llm: {
        link: "/ui/cves",
        action: "cve_products",
        limit: true,
      },
      cve_enrich_kev: {
        link: "/ui/cves",
        action: "cve_kev",
        limit: true,
      },
      cve_enrich_threat_actors: {
        link: "/ui/cves",
        action: "cve_threats",
        limit: true,
      },
      build_daily_brief: {
        link: "/ui/jobs",
        action: "daily_brief",
        date: true,
      },
    };
    const wrapper = document.createElement("div");
    wrapper.className = "job-counts-grid";
    groups.forEach((group) => {
      const groupTypes = Array.isArray(group.job_types)
        ? group.job_types.filter((type) => allTypes.includes(type))
        : [];
      if (!groupTypes.length) {
        return;
      }
      const section = document.createElement("section");
      section.className = "job-counts-section";
      const heading = document.createElement("h4");
      heading.className = "job-counts-heading";
      heading.textContent = group.title || group.id || "Jobs";
      section.appendChild(heading);
      const table = document.createElement("table");
      table.className = "table compact job-counts-table";
      table.innerHTML = headerHtml;
      const body = table.querySelector("tbody");
      groupTypes.forEach((jobType) => {
        const statusMap = counts[jobType] || {};
        const needsValue =
          typeof queueable?.[jobType] === "number" ? queueable[jobType] : null;
        const needsMeta = needsConfig[jobType];
        const needsLink = needsMeta?.link;
        const needsText = needsValue === null ? "—" : String(needsValue);
        const needsHtml = needsLink ? `<a href="${needsLink}">${needsText}</a>` : needsText;
        let controlHtml = "";
        if (needsMeta?.action === "daily_brief") {
          controlHtml = `
            <span class="job-needs-controls">
              <input type="date" class="dashboard-brief-date" />
              <button class="btn tiny secondary dashboard-brief-queue">Queue</button>
            </span>
          `;
        } else if (needsMeta?.action) {
          const limitSelect = needsMeta.limit
            ? `<select class="dashboard-limit" data-kind="${needsMeta.action}">
                 <option value="50">50</option>
                 <option value="200" selected>200</option>
                 <option value="500">500</option>
               </select>`
            : "";
          controlHtml = `
            <span class="job-needs-controls">
              <button class="btn tiny secondary dashboard-queue" data-kind="${needsMeta.action}">Queue</button>
              ${limitSelect}
            </span>
          `;
        }
        const queueHtml = controlHtml || "—";
        const row = document.createElement("tr");
        row.innerHTML = `
          <td>${jobType}</td>
          <td class="job-needs">
            <span class="job-needs-value">${needsHtml}</span>
          </td>
          <td class="job-queue-cell">${queueHtml}</td>
          <td>${statusMap.queued || 0}</td>
          <td>${statusMap.running || 0}</td>
          <td>${statusMap.failed || 0}</td>
          <td>${statusMap.succeeded || 0}</td>
        `;
        body.appendChild(row);
      });
      section.appendChild(table);
      wrapper.appendChild(section);
    });
    jobCountsContainer.innerHTML = "";
    jobCountsContainer.appendChild(wrapper);
    jobCountsContainer.querySelectorAll(".dashboard-brief-date").forEach((input) => {
      if (input.value) {
        return;
      }
      const today = new Date();
      today.setDate(today.getDate() - 1);
      const yyyy = String(today.getFullYear());
      const mm = String(today.getMonth() + 1).padStart(2, "0");
      const dd = String(today.getDate()).padStart(2, "0");
      input.value = `${yyyy}-${mm}-${dd}`;
    });
  }
  async function loadMetrics() {
    const data = await apiFetch("/admin/api/dashboard/metrics");
    renderJobCounts(
      data.job_counts_by_type_status || {},
      data.job_types || [],
      data.job_groups || [],
      data.job_counts_since || null,
      data.queueable_by_job_type || {}
    );
  }
  async function loadQueueDiagnostics() {
    if (!queueBanner || !staleMinutes) {
      return;
    }
    const data = await apiFetch("/admin/api/diagnostics/queue");
    const items = Array.isArray(data.queue) ? data.queue : [];
    const stale = items.filter((item) => {
      if (typeof item.oldest_age_minutes !== "number") {
        return false;
      }
      return item.oldest_age_minutes >= staleMinutes;
    });
    if (!stale.length) {
      queueBanner.style.display = "none";
      queueBanner.textContent = "";
      return;
    }
    const detail = stale
      .map((item) => `${item.job_type} (${item.oldest_age_minutes}m, ${item.queued} queued)`)
      .join("; ");
    queueBanner.textContent = `Queued jobs older than ${staleMinutes}m: ${detail}. Hint: No worker claims this job type; check SV_WORKER_ONLY_TYPES.`;
    queueBanner.style.display = "block";
  }
  jobCountsContainer.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    if (target.classList.contains("dashboard-queue")) {
      const kind = target.dataset.kind || "";
      const limitSelect = jobCountsContainer.querySelector(`.dashboard-limit[data-kind="${kind}"]`);
      const limit = limitSelect ? parseInt(limitSelect.value, 10) : undefined;
      try {
        const payload = await apiFetch("/admin/api/dashboard/queue_missing", {
          method: "POST",
          body: JSON.stringify({ kind, limit }),
        });
        if (payload.status === "disabled") {
          showToast(payload.message || "Queue disabled");
        } else if (payload.job_id) {
          showToast(`Queued: ${payload.job_id}`);
        } else {
          showToast(`Queued ${payload.queued || 0} (skipped ${payload.skipped || 0})`);
        }
        await loadMetrics();
      } catch (err) {
        showToast(err.message || String(err));
      }
    }
    if (target.classList.contains("dashboard-brief-queue")) {
      const row = target.closest("tr");
      const dateInput = row ? row.querySelector(".dashboard-brief-date") : null;
      const dateValue = dateInput ? dateInput.value : "";
      const payload = dateValue ? { date: dateValue } : {};
      try {
        const result = await apiFetch("/admin/api/daily_brief/build", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        showToast(`Daily brief queued: ${result.job_id}`);
        await loadMetrics();
      } catch (err) {
        showToast(err.message || String(err));
      }
    }
  });
  const resetFailuresBtn = document.getElementById("reset-failures");
  if (resetFailuresBtn) {
    resetFailuresBtn.addEventListener("click", async () => {
      try {
        const payload = await apiFetch("/admin/api/dashboard/reset_failures", { method: "POST" });
        const since = payload && payload.counts_since ? formatTimestamp(payload.counts_since) : null;
        showToast(since ? `Counts reset (${since})` : "Failure counts reset", "success");
        loadMetrics().catch((err) => showToast(err.message || String(err)));
      } catch (err) {
        showToast(err.message || String(err), "error");
      }
    });
  }
  const rebuildVendorBtn = document.getElementById("dashboard-rebuild-vendor-products");
  if (rebuildVendorBtn) {
    rebuildVendorBtn.addEventListener("click", async () => {
      try {
        const payload = await apiFetch("/admin/api/dashboard/rebuild_vendor_products", { method: "POST" });
        if (payload && payload.status === "queued") {
          showToast(`Vendor/product rebuild queued (${payload.job_id})`, "success");
        } else {
          showToast("Vendor/product rebuild queued", "success");
        }
      } catch (err) {
        showToast(err.message || String(err), "error");
      }
    });
  }
  if (autoCatchupToggle) {
    autoCatchupToggle.addEventListener("change", async () => {
      autoCatchupToggle.disabled = true;
      try {
        await apiFetch("/admin/api/config/patch", {
          method: "PUT",
          body: JSON.stringify({
            config: { jobs: { auto_catchup_enabled: !!autoCatchupToggle.checked } },
          }),
        });
        showToast(
          autoCatchupToggle.checked ? "Auto catchup enabled" : "Auto catchup disabled",
          "success"
        );
      } catch (err) {
        autoCatchupToggle.checked = !autoCatchupToggle.checked;
        showToast(err.message || String(err), "error");
      } finally {
        autoCatchupToggle.disabled = false;
      }
    });
  }
  loadAutoCatchupToggle().catch(() => undefined);
  loadMetrics().catch((err) => showToast(err.message || String(err)));
  loadQueueDiagnostics().catch(() => undefined);
  setInterval(() => {
    loadMetrics().catch(() => undefined);
    loadQueueDiagnostics().catch(() => undefined);
  }, 10000);
}
function wireLogs() {
  const output = document.getElementById("logs-output");
  if (!output) {
    return;
  }
  const serviceSelect = document.getElementById("logs-service");
  const linesSelect = document.getElementById("logs-lines");
  const autoToggle = document.getElementById("logs-auto");
  const pinToggle = document.getElementById("logs-pin");
  const refreshBtn = document.getElementById("logs-refresh");
  const openBtn = document.getElementById("logs-open");
  const buildControls = document.getElementById("logs-build-controls");
  const buildStdoutBtn = document.getElementById("logs-build-stdout");
  const buildStderrBtn = document.getElementById("logs-build-stderr");
  const eventList = document.getElementById("logs-event-list");
  const jobList = document.getElementById("logs-job-list");
  const eventAllBtn = document.getElementById("logs-event-all");
  const eventNoneBtn = document.getElementById("logs-event-none");
  const jobAllBtn = document.getElementById("logs-job-all");
  const jobNoneBtn = document.getElementById("logs-job-none");
  let rawLines = [];
  let selectedEvents = null;
  let selectedJobs = null;
  let buildMode = "";
  const workerLlmJobTypes = new Set([
    "summarize_article_llm",
    "summarize_article_context_llm",
    "cve_enrich_llm",
    "article_enrich_products",
    "article_enrich_threat_actors",
    "cve_enrich_threat_actors",
    "derive_events_from_articles",
    "enrich_event_summary_llm",
    "build_daily_brief",
    "article_threat_actors_backfill",
    "cve_threat_actors_backfill",
  ]);
  const workerFetchJobTypes = new Set([
    "ingest_due_sources",
    "ingest_source",
    "fetch_article_content",
    "cve_sync",
    "build_daily_brief",
    "source_acquire",
    "rebuild_vendor_products",
    "article_products_backfill",
    "derive_events_from_articles",
    "enrich_event_from_web",
    "validate_event_web_source",
    "promote_event_web_source_to_article",
  ]);
  const params = new URLSearchParams(window.location.search);
  if (params.get("logs") === "1") {
    document.body.classList.add("logs-standalone");
    document.querySelectorAll("section.panel").forEach((panel) => {
      if (panel.id !== "dashboard-logs") {
        panel.style.display = "none";
      }
    });
  }
  function parseLine(line) {
    const eventMatch = line.match(/\bevent=([^\s]+)/);
    const jobMatch = line.match(/\bjob_type=([^\s]+)/);
    return {
      event: eventMatch ? eventMatch[1] : "",
      jobType: jobMatch ? jobMatch[1] : "",
    };
  }
  function normalizeTimezone(tz) {
    if (!tz) return "America/New_York";
    if (tz.toUpperCase() === "EST") return "America/New_York";
    if (tz.toUpperCase() === "EDT") return "America/New_York";
    return tz;
  }
  function formatLogLineLocal(line) {
    if (!line || line.startsWith("# ")) return line;
    const match = line.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})([.,](\d{1,6}))?\s(.*)$/);
    if (!match) return line;
    const [, datePart, timePart, , fracRaw, rest] = match;
    const ms = fracRaw ? fracRaw.slice(0, 3).padEnd(3, "0") : "000";
    const iso = `${datePart}T${timePart}.${ms}Z`;
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return line;
    const tz = normalizeTimezone(document.body.dataset.timezone || "");
    const fmt = new Intl.DateTimeFormat("sv-SE", {
      timeZone: tz,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
    const localStamp = fmt.format(date);
    return `${localStamp}.${ms} ${rest}`;
  }
  function buildFilterList(container, items, prefix, selected) {
    container.innerHTML = "";
    items.forEach((item) => {
      const id = `${prefix}-${item.value}`.replace(/[^a-zA-Z0-9_-]/g, "_");
      const label = document.createElement("label");
      label.className = "checkbox compact";
      const checked = !selected || selected.has(item.value);
      label.innerHTML = `
        <input type="checkbox" data-value="${item.value}" ${checked ? "checked" : ""} id="${id}">
        <span>${item.value || "—"} <span class="muted">(${item.count})</span></span>
      `;
      const suppressBtn = document.getElementById("article-suppress-toggle");
      if (suppressBtn) {
        suppressBtn.addEventListener("click", async () => {
          try {
            const result = await apiFetch(`/admin/api/articles/${articleId}/suppress`, {
              method: "POST",
              body: JSON.stringify({ suppressed: !item.suppressed }),
            });
            showToast(result.suppressed ? "Article suppressed" : "Article unsuppressed");
            wireContentArticle();
          } catch (err) {
            showToast(err.message || String(err));
          }
        });
      }
      container.appendChild(label);
    });
  }
  function filterLinesByService(lines, service) {
    if (service === "build_hugo") {
      return lines;
    }
    if (service === "openai_prompts") {
      return lines;
    }
    if (service === "worker_llm" || service === "worker_openai") {
      return lines.filter((line) => {
        const parsed = parseLine(line);
        if (parsed.jobType) {
          return workerLlmJobTypes.has(parsed.jobType);
        }
        return line.includes("stage=");
      });
    }
    if (service === "worker_fetch") {
      return lines.filter((line) => {
        const parsed = parseLine(line);
        if (parsed.jobType) {
          return workerFetchJobTypes.has(parsed.jobType);
        }
        return (
          line.includes("source_id=") ||
          line.includes("source_name=") ||
          line.includes("article_id=") ||
          line.includes("fetch_article") ||
          line.includes("content_fetch") ||
          line.includes("content_fetched")
        );
      });
    }
    return lines;
  }
  function collectValues(lines) {
    const eventCounts = new Map();
    const jobCounts = new Map();
    lines.forEach((line) => {
      const parsed = parseLine(line);
      if (parsed.event) {
        eventCounts.set(parsed.event, (eventCounts.get(parsed.event) || 0) + 1);
      }
      if (parsed.jobType) {
        jobCounts.set(parsed.jobType, (jobCounts.get(parsed.jobType) || 0) + 1);
      }
    });
    const events = [...eventCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([value, count]) => ({ value, count }));
    const jobs = [...jobCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([value, count]) => ({ value, count }));
    return { events, jobs };
  }
  function getCheckedValues(container) {
    if (!container) return null;
    const checked = new Set();
    container.querySelectorAll("input[type='checkbox']").forEach((input) => {
      if (input.checked) {
        checked.add(input.dataset.value || "");
      }
    });
    return checked;
  }
  function setAll(container, checked) {
    if (!container) return;
    container.querySelectorAll("input[type='checkbox']").forEach((input) => {
      input.checked = checked;
    });
  }
  function renderFiltered() {
    selectedEvents = getCheckedValues(eventList);
    selectedJobs = getCheckedValues(jobList);
    const allowedEvents = selectedEvents;
    const allowedJobs = selectedJobs;
    const baseLines = filterLinesByService(rawLines, serviceSelect.value);
    const filtered = baseLines.filter((line) => {
      const parsed = parseLine(line);
      const eventOk = !allowedEvents || allowedEvents.size === 0 || !parsed.event || allowedEvents.has(parsed.event);
      const jobOk = !allowedJobs || allowedJobs.size === 0 || !parsed.jobType || allowedJobs.has(parsed.jobType);
      return eventOk && jobOk;
    });
    const displayLines = filtered.map((line) => formatLogLineLocal(line));
    const shouldPin = pinToggle ? pinToggle.checked : true;
    const wasPinned =
      output.scrollHeight <= output.clientHeight ||
      output.scrollTop + output.clientHeight >= output.scrollHeight - 4;
    output.textContent = displayLines.join("\n");
    if (shouldPin || wasPinned) {
      output.scrollTop = output.scrollHeight;
    }
  }
  async function loadLogs() {
    const service = serviceSelect.value;
    const lines = linesSelect.value;
    if (service === "build_hugo") {
      const data = await apiFetch(`/admin/api/logs/tail?service=build_hugo&lines=${lines}`);
      const header = data.log_path ? `# ${data.log_path}\n` : "";
      rawLines = [];
      if (header) {
        rawLines.push(header.trimEnd());
      }
      rawLines = rawLines.concat(
        (data.text || "").split("\n").filter((line) => line.trim() !== "")
      );
    } else if (service === "builder" && buildMode) {
      const data = await apiFetch(
        `/admin/api/logs/builds/latest?stream=${buildMode}&lines=${lines}`
      );
      const header = data.log_path ? `# ${data.log_path}\n` : "";
      rawLines = [];
      if (header) {
        rawLines.push(header.trimEnd());
      }
      rawLines = rawLines.concat(
        (data.text || "").split("\n").filter((line) => line.trim() !== "")
      );
    } else {
      const data = await apiFetch(`/admin/api/logs/tail?service=${service}&lines=${lines}`);
      rawLines = (data.text || "").split("\n").filter((line) => line.trim() !== "");
    }
    const filteredLines = filterLinesByService(rawLines, service);
    const { events, jobs } = collectValues(filteredLines);
    const jobListItems = jobs;
    if (eventList && events.length) {
      buildFilterList(eventList, events, "logs-event", selectedEvents);
    }
    if (jobList && jobListItems.length) {
      buildFilterList(jobList, jobListItems, "logs-job", selectedJobs);
    }
    renderFiltered();
  }
  if (openBtn) {
    openBtn.addEventListener("click", () => {
      const url = `${window.location.origin}/ui/?logs=1`;
      window.open(url, "_blank", "noopener");
    });
  }
  if (refreshBtn) {
    refreshBtn.addEventListener("click", () => {
      loadLogs().catch((err) => showToast(err.message || String(err)));
    });
  }
  if (buildStdoutBtn) {
    buildStdoutBtn.addEventListener("click", () => {
      buildMode = "stdout";
      loadLogs().catch((err) => showToast(err.message || String(err)));
    });
  }
  if (buildStderrBtn) {
    buildStderrBtn.addEventListener("click", () => {
      buildMode = "stderr";
      loadLogs().catch((err) => showToast(err.message || String(err)));
    });
  }
  [serviceSelect, linesSelect].forEach((el) => {
    if (el) {
      el.addEventListener("change", () => {
        selectedEvents = null;
        selectedJobs = null;
        buildMode = "";
        if (buildControls) {
          buildControls.style.display = serviceSelect.value === "builder" ? "flex" : "none";
        }
        loadLogs().catch((err) => showToast(err.message || String(err)));
      });
    }
  });
  if (eventList) {
    eventList.addEventListener("change", () => renderFiltered());
  }
  if (jobList) {
    jobList.addEventListener("change", () => renderFiltered());
  }
  if (eventAllBtn) {
    eventAllBtn.addEventListener("click", () => {
      setAll(eventList, true);
      renderFiltered();
    });
  }
  if (eventNoneBtn) {
    eventNoneBtn.addEventListener("click", () => {
      setAll(eventList, false);
      renderFiltered();
    });
  }
  if (jobAllBtn) {
    jobAllBtn.addEventListener("click", () => {
      setAll(jobList, true);
      renderFiltered();
    });
  }
  if (jobNoneBtn) {
    jobNoneBtn.addEventListener("click", () => {
      setAll(jobList, false);
      renderFiltered();
    });
  }
  setInterval(() => {
    if (autoToggle && autoToggle.checked) {
      loadLogs().catch(() => undefined);
    }
  }, 4000);
  if (buildControls && serviceSelect && serviceSelect.value === "builder") {
    buildControls.style.display = "flex";
  }
  loadLogs().catch((err) => showToast(err.message || String(err)));
}
function wireWatchlist() {
  const vendorTable = document.getElementById("watch-vendors-table");
  const productTable = document.getElementById("watch-products-table");
  if (!vendorTable || !productTable) {
    return;
  }
  const vendorForm = document.getElementById("watch-vendor-form");
  const vendorInput = document.getElementById("watch-vendor-name");
  const productForm = document.getElementById("watch-product-form");
  const productInput = document.getElementById("watch-product-name");
  const productVendor = document.getElementById("watch-product-vendor");
  const productMode = document.getElementById("watch-product-mode");
  const suggestVendors = document.getElementById("watch-suggest-vendors");
  const suggestProducts = document.getElementById("watch-suggest-products");
  const recomputeBtn = document.getElementById("watch-recompute");
  function renderVendors(items) {
    const tbody = vendorTable.querySelector("tbody");
    tbody.innerHTML = "";
    items.forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td><input type="checkbox" class="watch-vendor-toggle" data-id="${item.id}" ${item.enabled ? "checked" : ""}></td>
        <td>${item.display_name}</td>
        <td><button class="btn small danger watch-vendor-delete" data-id="${item.id}">Delete</button></td>
      `;
      tbody.appendChild(row);
    });
  }
  function renderProducts(items) {
    const tbody = productTable.querySelector("tbody");
    tbody.innerHTML = "";
    items.forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td><input type="checkbox" class="watch-product-toggle" data-id="${item.id}" ${item.enabled ? "checked" : ""}></td>
        <td>${item.vendor_norm || ""}</td>
        <td>${item.display_name}</td>
        <td>
          <select class="watch-product-mode" data-id="${item.id}">
            <option value="exact" ${item.match_mode === "exact" ? "selected" : ""}>exact</option>
            <option value="contains" ${item.match_mode === "contains" ? "selected" : ""}>contains</option>
          </select>
        </td>
        <td><button class="btn small danger watch-product-delete" data-id="${item.id}">Delete</button></td>
      `;
      tbody.appendChild(row);
    });
  }
  function renderSuggestions(data) {
    if (suggestVendors) {
      suggestVendors.innerHTML = "";
      (data.vendors || []).forEach((item) => {
        const li = document.createElement("li");
        li.innerHTML = `<button class="btn small secondary watch-suggest-vendor" data-name="${item.display_name}">Add</button> ${item.display_name} (${item.count})`;
        suggestVendors.appendChild(li);
      });
    }
    if (suggestProducts) {
      suggestProducts.innerHTML = "";
      (data.products || []).forEach((item) => {
        const li = document.createElement("li");
        li.innerHTML = `<button class="btn small secondary watch-suggest-product" data-name="${item.display_name}" data-vendor="${item.vendor_norm}">Add</button> ${item.display_name} (${item.count})`;
        suggestProducts.appendChild(li);
      });
    }
  }
  async function refreshAll() {
    const vendors = await apiFetch("/admin/api/watchlist/vendors");
    renderVendors(vendors.items || []);
    const products = await apiFetch("/admin/api/watchlist/products");
    renderProducts(products.items || []);
    const suggestions = await apiFetch("/admin/api/watchlist/suggestions");
    renderSuggestions(suggestions);
  }
  vendorForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const name = vendorInput.value.trim();
    if (!name) {
      return;
    }
    await apiFetch("/admin/api/watchlist/vendors", {
      method: "POST",
      body: JSON.stringify({ display_name: name, enabled: true }),
    });
    vendorInput.value = "";
    showToast("Vendor added");
    refreshAll().catch((err) => showToast(err.message || String(err)));
  });
  productForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const name = productInput.value.trim();
    if (!name) {
      return;
    }
    const vendor = productVendor.value.trim() || null;
    const mode = productMode.value;
    await apiFetch("/admin/api/watchlist/products", {
      method: "POST",
      body: JSON.stringify({
        display_name: name,
        vendor_norm: vendor,
        match_mode: mode,
        enabled: true,
      }),
    });
    productInput.value = "";
    productVendor.value = "";
    showToast("Product added");
    refreshAll().catch((err) => showToast(err.message || String(err)));
  });
  vendorTable.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    if (target.classList.contains("watch-vendor-delete")) {
      await apiFetch(`/admin/api/watchlist/vendors/${target.dataset.id}`, { method: "DELETE" });
      showToast("Vendor removed");
      refreshAll().catch((err) => showToast(err.message || String(err)));
    }
  });
  vendorTable.addEventListener("change", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) {
      return;
    }
    if (!target.classList.contains("watch-vendor-toggle")) {
      return;
    }
    await apiFetch(`/admin/api/watchlist/vendors/${target.dataset.id}`, {
      method: "PATCH",
      body: JSON.stringify({ enabled: target.checked }),
    });
    showToast("Vendor updated");
  });
  productTable.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    if (target.classList.contains("watch-product-delete")) {
      await apiFetch(`/admin/api/watchlist/products/${target.dataset.id}`, { method: "DELETE" });
      showToast("Product removed");
      refreshAll().catch((err) => showToast(err.message || String(err)));
    }
  });
  productTable.addEventListener("change", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    if (target.classList.contains("watch-product-toggle")) {
      await apiFetch(`/admin/api/watchlist/products/${target.dataset.id}`, {
        method: "PATCH",
        body: JSON.stringify({ enabled: target.checked }),
      });
      showToast("Product updated");
      return;
    }
    if (target.classList.contains("watch-product-mode")) {
      await apiFetch(`/admin/api/watchlist/products/${target.dataset.id}`, {
        method: "PATCH",
        body: JSON.stringify({ enabled: true, match_mode: target.value }),
      });
      showToast("Mode updated");
    }
  });
  if (suggestVendors) {
    suggestVendors.addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      if (!target.classList.contains("watch-suggest-vendor")) {
        return;
      }
      const name = target.dataset.name;
      await apiFetch("/admin/api/watchlist/vendors", {
        method: "POST",
        body: JSON.stringify({ display_name: name, enabled: true }),
      });
      showToast("Vendor added");
      refreshAll().catch((err) => showToast(err.message || String(err)));
    });
  }
  if (suggestProducts) {
    suggestProducts.addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      if (!target.classList.contains("watch-suggest-product")) {
        return;
      }
      const name = target.dataset.name;
      const vendor = target.dataset.vendor || null;
      await apiFetch("/admin/api/watchlist/products", {
        method: "POST",
        body: JSON.stringify({
          display_name: name,
          vendor_norm: vendor,
          match_mode: "exact",
          enabled: true,
        }),
      });
      showToast("Product added");
      refreshAll().catch((err) => showToast(err.message || String(err)));
    });
  }
  if (recomputeBtn) {
    recomputeBtn.addEventListener("click", async () => {
      await apiFetch("/admin/api/watchlist/recompute", { method: "POST" });
      showToast("Scope recomputed");
    });
  }
  refreshAll().catch((err) => showToast(err.message || String(err)));
}
function buildPageList(current, total) {
  const pages = new Set([1, total, current - 2, current - 1, current, current + 1, current + 2]);
  return Array.from(pages)
    .filter((p) => p >= 1 && p <= total)
    .sort((a, b) => a - b);
}
function renderPager(container, total, page, size, onPage) {
  if (!container) {
    return;
  }
  container.innerHTML = "";
  const totalPages = Math.max(1, Math.ceil(total / size));
  const controls = document.createElement("div");
  controls.className = "pager-controls";
  const prev = document.createElement("button");
  prev.type = "button";
  prev.className = "btn secondary";
  prev.textContent = "Prev";
  prev.disabled = page <= 1;
  prev.addEventListener("click", () => onPage(page - 1));
  controls.appendChild(prev);
  const pages = buildPageList(page, totalPages);
  let last = 0;
  pages.forEach((p) => {
    if (p - last > 1) {
      const ellipsis = document.createElement("span");
      ellipsis.className = "pager-ellipsis";
      ellipsis.textContent = "…";
      controls.appendChild(ellipsis);
    }
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "pager-page" + (p === page ? " active" : "");
    btn.textContent = String(p);
    btn.addEventListener("click", () => onPage(p));
    controls.appendChild(btn);
    last = p;
  });
  const next = document.createElement("button");
  next.type = "button";
  next.className = "btn secondary";
  next.textContent = "Next";
  next.disabled = page >= totalPages;
  next.addEventListener("click", () => onPage(page + 1));
  controls.appendChild(next);
  const info = document.createElement("div");
  info.className = "pager-info";
  info.textContent = `Page ${page} of ${totalPages}`;
  container.appendChild(controls);
  container.appendChild(info);
}
function wireNavDropdowns() {
  const dropdowns = Array.from(document.querySelectorAll(".nav-dropdown"));
  if (!dropdowns.length) {
    return;
  }
  const closeAll = () => {
    dropdowns.forEach((dropdown) => {
      dropdown.classList.remove("open");
      const toggle = dropdown.querySelector(".dropdown-toggle");
      if (toggle) {
        toggle.setAttribute("aria-expanded", "false");
      }
    });
  };
  dropdowns.forEach((dropdown) => {
    const toggle = dropdown.querySelector(".dropdown-toggle");
    if (!toggle) {
      return;
    }
    toggle.addEventListener("click", (event) => {
      event.preventDefault();
      const isOpen = dropdown.classList.contains("open");
      closeAll();
      if (!isOpen) {
        dropdown.classList.add("open");
        toggle.setAttribute("aria-expanded", "true");
      }
    });
  });
  document.addEventListener("click", (event) => {
    if (!event.target.closest(".nav-dropdown")) {
      closeAll();
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeAll();
    }
  });
}
function wireEnqueueButtons() {
  document.querySelectorAll("[data-enqueue]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const jobType = btn.getAttribute("data-enqueue");
      try {
        await apiFetch("/jobs/enqueue", {
          method: "POST",
          body: JSON.stringify({ job_type: jobType }),
        });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
  const briefBtn = document.getElementById("daily-brief-build");
  if (briefBtn) {
    const dateInput = document.getElementById("daily-brief-date");
    if (dateInput && !dateInput.value) {
      const today = new Date();
      const yyyy = String(today.getFullYear());
      const mm = String(today.getMonth() + 1).padStart(2, "0");
      const dd = String(today.getDate()).padStart(2, "0");
      dateInput.value = `${yyyy}-${mm}-${dd}`;
    }
    briefBtn.addEventListener("click", async () => {
      const dateValue = dateInput ? dateInput.value : "";
      const payload = dateValue ? { date: dateValue } : {};
      try {
        const result = await apiFetch("/admin/api/daily_brief/build", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        showToast(`Daily brief queued: ${result.job_id}`);
      } catch (err) {
        alert(err);
      }
    });
  }
}
function wireSources() {
  const form = document.getElementById("source-form");
  if (!form) {
    return;
  }
  const table = document.getElementById("sources-table");
  const tbody = table ? table.querySelector("tbody") : null;
  const nameField = document.getElementById("source-name");
  const kindField = document.getElementById("source-kind");
  const urlField = document.getElementById("source-url");
  const intervalField = document.getElementById("source-interval");
  const tagsField = document.getElementById("source-tags");
  const addOpen = document.getElementById("source-add-open");
  const addModal = document.getElementById("source-add-modal");
  const addClose = document.getElementById("source-add-close");
  const resetBtn = document.getElementById("source-reset");
  function normalizeOverrides(raw) {
    const base = {
      discovery: { mode: "default", allowlist_regex: "", blocklist_regex: "" },
      content: {
        mode: "default",
        min_chars: 800,
        include_selectors: [],
        exclude_selectors: [],
        strip_patterns: [],
        allow_fallback_to_default: true,
      },
      fetch: { use_vpn: true },
    };
    if (!raw || typeof raw !== "object") {
      return base;
    }
    const discovery = raw.discovery && typeof raw.discovery === "object" ? raw.discovery : {};
    const content = raw.content && typeof raw.content === "object" ? raw.content : {};
    const fetch = raw.fetch && typeof raw.fetch === "object" ? raw.fetch : {};
    return {
      discovery: {
        mode: discovery.mode || base.discovery.mode,
        allowlist_regex: discovery.allowlist_regex || "",
        blocklist_regex: discovery.blocklist_regex || "",
      },
      content: {
        mode: content.mode || base.content.mode,
        min_chars: Number.isFinite(parseInt(content.min_chars, 10))
          ? parseInt(content.min_chars, 10)
          : base.content.min_chars,
        include_selectors: Array.isArray(content.include_selectors)
          ? content.include_selectors
          : [],
        exclude_selectors: Array.isArray(content.exclude_selectors)
          ? content.exclude_selectors
          : [],
        strip_patterns: Array.isArray(content.strip_patterns)
          ? content.strip_patterns
          : [],
        allow_fallback_to_default:
          content.allow_fallback_to_default === undefined
            ? base.content.allow_fallback_to_default
            : !!content.allow_fallback_to_default,
      },
      fetch: {
        use_vpn:
          fetch.use_vpn === undefined ? base.fetch.use_vpn : !!fetch.use_vpn,
      },
    };
  }
  function listToText(value) {
    if (!Array.isArray(value)) {
      return "";
    }
    return value.join("\n");
  }
  function parseList(value) {
    if (!value) {
      return [];
    }
    return value
      .split(/\n|,/)
      .map((item) => item.trim())
      .filter(Boolean);
  }
  function overridesIsEmpty(overrides) {
    if (!overrides) {
      return true;
    }
    const d = overrides.discovery || {};
    const c = overrides.content || {};
    const f = overrides.fetch || {};
    const discoveryDefault =
      (d.mode || "default") === "default" && !d.allowlist_regex && !d.blocklist_regex;
    const contentDefault =
      (c.mode || "default") === "default" &&
      (parseInt(c.min_chars || 800, 10) === 800) &&
      (!c.include_selectors || c.include_selectors.length === 0) &&
      (!c.exclude_selectors || c.exclude_selectors.length === 0) &&
      (!c.strip_patterns || c.strip_patterns.length === 0) &&
      (c.allow_fallback_to_default === undefined || c.allow_fallback_to_default === true);
    const fetchDefault = f.use_vpn === undefined || f.use_vpn === true;
    return discoveryDefault && contentDefault && fetchDefault;
  }
  function resetForm() {
    nameField.value = "";
    kindField.value = "rss";
    urlField.value = "";
    intervalField.value = "60";
    tagsField.value = "";
  }
  if (addOpen && addModal) {
    addOpen.addEventListener("click", () => {
      addModal.style.display = "block";
      resetForm();
      nameField.focus();
    });
  }
  if (addClose && addModal) {
    addClose.addEventListener("click", () => {
      addModal.style.display = "none";
    });
  }
  resetBtn.addEventListener("click", resetForm);
  function renderSourcesTable(sources) {
    if (!tbody) {
      return;
    }
    tbody.innerHTML = "";
    sources.forEach((source) => {
      const row = document.createElement("tr");
      row.dataset.sourceId = source.id;
      row.dataset.sourceUrl = source.url || "";
      row.dataset.sourceName = source.name || "";
      row.dataset.sourceKind = source.kind || "";
      row.dataset.sourceInterval = source.interval_minutes || "";
      row.dataset.sourceTags = (source.tags || []).join(", ");
      const overrides = normalizeOverrides(source.overrides);
      const discovery = overrides.discovery;
      const content = overrides.content;
      const fetchCfg = overrides.fetch;
      const acquiring = source.acquire_status === "queued" || source.acquire_status === "running";
      const acquireLabel = acquiring ? `Acquire (${source.acquire_status})` : "Acquire";
      const newCount = source.new_count || 0;
      const gatheredCount = source.gathered_count || 0;
      const summarizedCount = source.summarized_count || 0;
      let statusHtml = '<span class="status-pill">Unknown</span>';
      if (source.last_error) {
        statusHtml = '<span class="status-pill status-error">Error</span>';
      } else if (source.pause_until || source.enabled === false) {
        statusHtml = '<button class="status-pill status-warn source-resume" type="button" title="Click to resume">Paused</button>';
      } else {
        statusHtml = '<button class="status-pill status-ok source-pause" type="button" title="Click to pause">Enabled</button>';
      }
      row.innerHTML = `
        <td>${statusHtml}</td>
        <td class="source-cell">
          <div class="source-line">
            <span class="source-name">${esc(source.name)}</span>
          </div>
        </td>
        <td class="source-cell source-stack">
          ${source.pause_until || source.enabled === false ? `
          <div class="source-line">
            <span class="source-meta">Pause until:</span>
            <span>${esc(formatTimestamp(source.pause_until))}</span>
          </div>
          <div class="source-subline">
            <span class="source-meta">Reason:</span>
            <span class="truncate" title="${esc(source.paused_reason || "")}">${esc(source.paused_reason || "")}</span>
          </div>` : ""}
          <div class="source-subline">
            ${source.last_successful_poll_at ? `<span class="source-meta">Last Successful Poll:</span><span>${esc(formatTimestamp(source.last_successful_poll_at))}</span>` : ""}
            ${source.last_article_at ? `<span class="source-meta">Last Article Acquired:</span><span>${esc(formatTimestamp(source.last_article_at))}</span>` : ""}
            ${source.last_error ? `<span class="source-meta">Last Error:</span><span class="truncate" title="${esc(source.last_error)}">${esc(source.last_error)}</span>` : ""}
          </div>
        </td>
        <td class="source-cell">
          <div class="counts-line">
            <span class="count-pill ${newCount ? "count-attn" : ""}">New ${newCount}</span>
            <span class="count-pill ${gatheredCount ? "count-attn" : ""}">Gathered ${gatheredCount}</span>
          </div>
          <div class="counts-line">
            <span class="count-pill">24h ${source.articles_24h || 0}</span>
            <span class="count-pill">Total ${source.total_articles || 0}</span>
          </div>
        </td>
        <td class="table-actions">
          <div class="actions-grid">
            <button class="btn small secondary acquire-source" type="button" ${acquiring ? "disabled" : ""}>${acquireLabel}</button>
            <button class="btn small fetch-missing" type="button" ${newCount > 0 ? "" : "disabled"}>Fetch</button>
            <button class="btn small summarize-missing" type="button" ${gatheredCount > 0 ? "" : "disabled"}>Summarize</button>
            <button class="btn small test-source" type="button">Test</button>
            <button class="btn small secondary history-source" type="button">History</button>
            <button class="btn small secondary edit-source" type="button">Edit</button>
            <button class="btn small danger delete-source" type="button">Delete</button>
          </div>
        </td>
      `;
      const editRow = document.createElement("tr");
      editRow.className = "edit-result";
      editRow.dataset.sourceId = source.id;
      editRow.style.display = "none";
      editRow.innerHTML = `
        <td colspan="5">
          <form class="source-edit-form">
            <div class="grid">
              <label>Source ID<input type="text" value="${esc(source.id)}" readonly></label>
              <label>Name<input type="text" name="name" value="${esc(source.name)}"></label>
              <label>Kind
                <select name="kind">
                  <option value="rss" ${source.kind === "rss" ? "selected" : ""}>rss</option>
                  <option value="html" ${source.kind === "html" ? "selected" : ""}>html</option>
                </select>
              </label>
              <label>URL<input type="url" name="url" value="${esc(source.url || "")}"></label>
              <label>Interval (min)<input type="number" name="interval" value="${source.interval_minutes || 60}"></label>
              <label>Tags (comma)<input type="text" name="tags" value="${esc((source.tags || []).join(", "))}"></label>
            </div>
            <details class="source-overrides">
              <summary>Overrides</summary>
              <div class="muted help-text">Only set this for problematic sources; blank = default behavior.</div>
              <div class="grid">
                <label>Discovery mode
                  <select name="override-discovery-mode">
                    <option value="default" ${discovery.mode === "default" ? "selected" : ""}>default</option>
                  </select>
                </label>
                <div class="muted help-text">RSS sources ignore discovery mode; allow/blocklist only.</div>
                <label>Discovery allowlist regex
                  <input type="text" name="override-discovery-allowlist" value="${esc(
                    discovery.allowlist_regex || ""
                  )}">
                </label>
                <label>Discovery blocklist regex
                  <input type="text" name="override-discovery-blocklist" value="${esc(
                    discovery.blocklist_regex || ""
                  )}">
                </label>
                <label>Content mode
                  <select name="override-content-mode">
                    <option value="default" ${content.mode === "default" ? "selected" : ""}>default</option>
                    <option value="jsonld_articlebody" ${
                      content.mode === "jsonld_articlebody" ? "selected" : ""
                    }>jsonld_articlebody</option>
                    <option value="readability" ${content.mode === "readability" ? "selected" : ""}>readability</option>
                    <option value="trafilatura" ${content.mode === "trafilatura" ? "selected" : ""}>trafilatura</option>
                    <option value="css_selectors" ${content.mode === "css_selectors" ? "selected" : ""}>css_selectors</option>
                  </select>
                </label>
                <label>Content min chars
                  <input type="number" name="override-content-min-chars" value="${content.min_chars || 800}">
                </label>
                <label>Include selectors (one per line)
                  <textarea name="override-content-include" rows="3">${esc(listToText(content.include_selectors))}</textarea>
                </label>
                <label>Exclude selectors (one per line)
                  <textarea name="override-content-exclude" rows="3">${esc(listToText(content.exclude_selectors))}</textarea>
                </label>
                <label>Strip patterns (regex, one per line)
                  <textarea name="override-content-strip" rows="3">${esc(listToText(content.strip_patterns))}</textarea>
                </label>
                <label class="checkbox">
                  <input type="checkbox" name="override-content-fallback" ${
                    content.allow_fallback_to_default ? "checked" : ""
                  }> Allow fallback to default
                </label>
                <label class="checkbox">
                  <input type="checkbox" name="override-fetch-use-vpn" ${
                    fetchCfg.use_vpn ? "checked" : ""
                  }> Use VPN proxy for content fetch
                </label>
              </div>
              <div class="override-test">
                <label>Test extraction URL
                  <input type="url" name="override-test-url" placeholder="https://example.com/story/...">
                </label>
                <button class="btn small secondary test-override" type="button">Test override</button>
                <div class="override-test-result" hidden>
                  <div class="mono override-test-meta"></div>
                  <pre class="mono override-test-preview"></pre>
                </div>
              </div>
            </details>
            <div class="actions">
              <button class="btn small edit-save" type="button">Save</button>
              <button class="btn small secondary edit-cancel" type="button">Cancel</button>
            </div>
          </form>
        </td>
      `;
      const testRow = document.createElement("tr");
      testRow.className = "test-result";
      testRow.dataset.sourceId = source.id;
      testRow.style.display = "none";
      testRow.innerHTML = `
        <td colspan="5">
          <div class="test-summary"></div>
          <button class="btn small secondary test-close" type="button">Hide</button>
          <details class="test-details">
            <summary>Details</summary>
            <pre class="mono test-raw"></pre>
          </details>
        </td>
      `;
      const historyRow = document.createElement("tr");
      historyRow.className = "history-result";
      historyRow.dataset.sourceId = source.id;
      historyRow.style.display = "none";
      historyRow.innerHTML = `<td colspan="5"><div class="history-table"></div></td>`;
      tbody.appendChild(row);
      tbody.appendChild(editRow);
      tbody.appendChild(testRow);
      tbody.appendChild(historyRow);
    });
  }
  async function refreshSources() {
    try {
      const sources = await apiFetch("/sources");
      renderSourcesTable(sources);
    } catch (err) {
      alert(err);
    }
  }
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      name: nameField.value.trim(),
      kind: kindField.value,
      url: urlField.value.trim(),
      interval_minutes: parseInt(intervalField.value, 10),
      tags: tagsField.value.trim(),
      enabled: true,
    };
    try {
      await apiFetch("/sources", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      resetForm();
      showToast("Source added");
      if (addModal) {
        addModal.style.display = "none";
      }
      await refreshSources();
    } catch (err) {
      alert(err);
    }
  });
  if (tbody) {
    tbody.addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const row = target.closest("tr");
      if (!row || !row.dataset.sourceId) {
        return;
      }
      const sourceId = row.dataset.sourceId;
      if (target.classList.contains("edit-source")) {
        const editRow = document.querySelector(
          `tr.edit-result[data-source-id="${sourceId}"]`
        );
        editRow.style.display = editRow.style.display === "table-row" ? "none" : "table-row";
        return;
      }
      if (target.classList.contains("edit-cancel")) {
        const editRow = target.closest("tr.edit-result");
        if (editRow) {
          editRow.style.display = "none";
        }
        return;
      }
      if (target.classList.contains("edit-save")) {
        const editRow = target.closest("tr.edit-result");
        const formEl = editRow.querySelector(".source-edit-form");
        const overrides = {
          discovery: {
            mode: formEl.querySelector('select[name="override-discovery-mode"]').value,
            allowlist_regex: formEl
              .querySelector('input[name="override-discovery-allowlist"]')
              .value.trim(),
            blocklist_regex: formEl
              .querySelector('input[name="override-discovery-blocklist"]')
              .value.trim(),
          },
          content: {
            mode: formEl.querySelector('select[name="override-content-mode"]').value,
            min_chars: parseInt(
              formEl.querySelector('input[name="override-content-min-chars"]').value,
              10
            ),
            include_selectors: parseList(
              formEl.querySelector('textarea[name="override-content-include"]').value
            ),
            exclude_selectors: parseList(
              formEl.querySelector('textarea[name="override-content-exclude"]').value
            ),
            strip_patterns: parseList(
              formEl.querySelector('textarea[name="override-content-strip"]').value
            ),
            allow_fallback_to_default: !!formEl.querySelector(
              'input[name="override-content-fallback"]'
            ).checked,
          },
          fetch: {
            use_vpn: !!formEl.querySelector('input[name="override-fetch-use-vpn"]').checked,
          },
        };
        const payload = {
          name: formEl.querySelector('input[name="name"]').value.trim(),
          kind: formEl.querySelector('select[name="kind"]').value,
          url: formEl.querySelector('input[name="url"]').value.trim(),
          interval_minutes: parseInt(formEl.querySelector('input[name="interval"]').value, 10),
          tags: formEl.querySelector('input[name="tags"]').value.trim(),
          overrides: overridesIsEmpty(overrides) ? null : overrides,
        };
        try {
          await apiFetch(`/sources/${sourceId}`, {
            method: "PUT",
            body: JSON.stringify(payload),
          });
          showToast("Source saved");
          editRow.style.display = "none";
          await refreshSources();
        } catch (err) {
          alert(err);
        }
        return;
      }
      if (target.classList.contains("source-resume")) {
        if (!confirm("Resume this source?")) {
          return;
        }
        try {
          await apiFetch(`/admin/api/sources/${sourceId}/resume`, { method: "POST" });
          showToast("Source resumed");
          await refreshSources();
        } catch (err) {
          alert(err);
        }
        return;
      }
      if (target.classList.contains("source-pause")) {
        if (!confirm("Pause this source?")) {
          return;
        }
        try {
          await apiFetch(`/sources/${sourceId}`, {
            method: "PATCH",
            body: JSON.stringify({ enabled: false }),
          });
          showToast("Source paused");
          await refreshSources();
        } catch (err) {
          alert(err);
        }
        return;
      }
      if (target.classList.contains("delete-source")) {
        if (!confirm(`Delete source ${sourceId}?`)) {
          return;
        }
        try {
          await apiFetch(`/sources/${sourceId}`, { method: "DELETE" });
          showToast("Source deleted");
          await refreshSources();
        } catch (err) {
          alert(err);
        }
        return;
      }
      if (target.classList.contains("acquire-source")) {
        try {
          const result = await apiFetch(`/admin/api/sources/${sourceId}/acquire`, {
            method: "POST",
            body: JSON.stringify({}),
          });
          showToast(`Acquire enqueued: ${result.job_id}`);
          await refreshSources();
        } catch (err) {
          alert(err);
        }
        return;
      }
      if (target.classList.contains("fetch-missing")) {
        try {
          const result = await apiFetch(`/admin/api/sources/${sourceId}/fetch_missing`, {
            method: "POST",
          });
          showToast(`Fetch queued: ${result.queued || 0}`);
        } catch (err) {
          alert(err);
        }
        return;
      }
      if (target.classList.contains("summarize-missing")) {
        try {
          const result = await apiFetch(`/admin/api/sources/${sourceId}/summarize_missing`, {
            method: "POST",
          });
          if (result.status === "disabled") {
            showToast(result.message || "Summarization disabled");
            return;
          }
          showToast(`Summarize queued: ${result.queued || 0}`);
        } catch (err) {
          alert(err);
        }
        return;
      }
      if (target.classList.contains("test-close")) {
        const outputRow = target.closest("tr.test-result");
        if (outputRow) {
          outputRow.style.display = "none";
        }
        return;
      }

      if (target.classList.contains("test-source")) {
        const outputRow = document.querySelector(
          `tr.test-result[data-source-id="${sourceId}"]`
        );
        try {
          const result = await apiFetch(`/sources/${sourceId}/test`, { method: "POST" });
          const tactic = result.discovery?.used_tactic || "unknown";
          const mode = result.discovery?.mode || "default";
          const warning = result.discovery?.warning ? ` warning=${result.discovery.warning}` : "";
          const summary = `status=${result.status} http=${result.http_status || ""} found=${result.found_count} accepted=${result.accepted_count} tactic=${tactic} mode=${mode}${warning}`;
          const items = (result.items || [])
            .map((item) => `- ${item.title || ""} ${item.url || ""}`)
            .join("\n");
          const reasons = (result.reject_reasons || [])
            .map((item) => `- ${item.reason}: ${item.count}`)
            .join("\n");
          const samples = (result.extraction_samples || [])
            .map((item) => {
              const status = item.error ? `error=${item.error}` : `chars=${item.char_count} min=${item.min_chars} pass=${item.passed_min_chars} method=${item.method}`;
              const title = item.title ? ` title="${item.title}"` : "";
              return `- ${item.url}${title} ${status}`;
            })
            .join("\n");
          const detailParts = [];
          if (result.discovery?.feed_url) {
            detailParts.push(`feed_url: ${result.discovery.feed_url}`);
          }
          if (result.discovery?.rss_probe) {
            const probe = result.discovery.rss_probe;
            detailParts.push(`rss_probe: content_type=${probe.content_type || ""} looks_like_rss=${probe.looks_like_rss} looks_like_html=${probe.looks_like_html}`);
          }
          if (reasons) {
            detailParts.push("rejected_reasons:\n" + reasons);
          }
          if (samples) {
            detailParts.push("extraction_samples:\n" + samples);
          }
          if (items) {
            detailParts.push("preview_items:\n" + items);
          }
          outputRow.querySelector(".test-summary").textContent = summary;
          outputRow.querySelector(".test-raw").textContent = detailParts.join("\n\n") || "No details";
          outputRow.style.display = "table-row";
        } catch (err) {
          alert(err);
        }
        return;
      }
      if (target.classList.contains("test-override")) {
        const editRow = target.closest("tr.edit-result");
        const formEl = editRow.querySelector(".source-edit-form");
        const urlValue = formEl.querySelector('input[name="override-test-url"]').value.trim();
        if (!urlValue) {
          alert("Enter a URL to test.");
          return;
        }
        const resultBox = formEl.querySelector(".override-test-result");
        const meta = formEl.querySelector(".override-test-meta");
        const preview = formEl.querySelector(".override-test-preview");
        try {
          const result = await apiFetch(`/admin/api/sources/${sourceId}/test_override`, {
            method: "POST",
            body: JSON.stringify({ url: urlValue }),
          });
          meta.textContent = `method=${result.method} chars=${result.char_count}`;
          preview.textContent = result.preview_first_400 || "";
          resultBox.hidden = false;
        } catch (err) {
          alert(err);
        }
        return;
      }
      if (target.classList.contains("history-source")) {
        const outputRow = document.querySelector(
          `tr.history-result[data-source-id="${sourceId}"]`
        );
        if (outputRow.style.display === "table-row") {
          outputRow.style.display = "none";
          return;
        }
        try {
          const result = await apiFetch(`/sources/${sourceId}/health?limit=20`);
          const rows = result
            .map(
              (item) =>
                `<tr>
                  <td>${esc(formatTimestamp(item.ts))}</td>
                  <td>${item.ok ? "ok" : "err"}</td>
                  <td>${item.found_count}</td>
                  <td>${item.accepted_count}</td>
                  <td>${item.seen_count}</td>
                  <td>${item.filtered_count}</td>
                  <td>${item.duration_ms || ""}</td>
                  <td class="truncate" title="${item.last_error || ""}">${item.last_error || ""}</td>
                </tr>`
            )
            .join("");
          outputRow.querySelector(".history-table").innerHTML = `
            <table>
              <thead>
                <tr>
                  <th>ts</th>
                  <th>ok</th>
                  <th>found</th>
                  <th>accepted</th>
                  <th>seen</th>
                  <th>filtered</th>
                  <th>ms</th>
                  <th>error</th>
                </tr>
              </thead>
              <tbody>${rows || "<tr><td colspan=\"8\">No history</td></tr>"}</tbody>
            </table>`;
          outputRow.style.display = "table-row";
        } catch (err) {
          alert(err);
        }
        return;
      }
    });
  }
}
function wireJobs() {
  const refresh = document.getElementById("jobs-refresh");
  const cancelAll = document.getElementById("jobs-cancel-all");
  const table = document.getElementById("jobs-table");
  const tbody = document.getElementById("jobs-table-body");
  const statusFilter = document.getElementById("jobs-filter-status");
  const typeFilter = document.getElementById("jobs-filter-type");
  const sizeSelect = document.getElementById("jobs-page-size");
  const applyBtn = document.getElementById("jobs-apply");
  const clearBtn = document.getElementById("jobs-clear");
  const pager = document.getElementById("jobs-pager");
  if (!refresh || !table || !tbody) {
    return;
  }
  const knownJobTypes = [
    "ingest_due_sources",
    "ingest_source",
    "fetch_article_content",
    "summarize_article_llm",
    "write_article_markdown",
    "build_site",
    "cve_sync",
    "events_rebuild",
    "derive_events_from_articles",
    "enrich_event_from_web",
    "promote_event_web_source_to_article",
    "article_enrich_products",
    "article_enrich_threat_actors",
    "article_products_backfill",
    "article_threat_actors_backfill",
    "enrich_event_summary_llm",
    "build_daily_brief",
    "source_acquire",
    "smoke_test",
    "cve_enrich_threat_actors",
    "cve_threat_actors_backfill",
  ];
  let page = 1;
  let pageSize = sizeSelect ? parseInt(sizeSelect.value, 10) || 20 : 20;
  if (typeFilter && typeFilter.tagName === "SELECT" && typeFilter.options.length <= 1) {
    knownJobTypes.forEach((jobType) => {
      const opt = document.createElement("option");
      opt.value = jobType;
      opt.textContent = jobType;
      typeFilter.appendChild(opt);
    });
  }
  function formatResult(job) {
    if (job.job_type === "build_site" && job.result) {
      const exitCode = job.result.exit_code ?? "";
      const stdout = job.result.stdout_tail || "";
      const stderr = job.result.stderr_tail || "";
      const tail = stderr || stdout;
      return `exit=${exitCode} ${tail}`.trim();
    }
    return job.error || (job.result ? JSON.stringify(job.result) : "");
  }
  function renderRows(jobs) {
    tbody.innerHTML = "";
    jobs.forEach((job) => {
      const canCancel = job.status === "queued" || job.status === "running";
      const pendingBadge = canCancel ? ' <span class="badge warn">pending</span>' : "";
      const runningBadge = job.status === "running" ? ' <span class="badge warn">running</span>' : "";
      let resultHtml = "";
      if (job.job_type === "build_site" && job.result) {
        const exitCode = job.result.exit_code ?? "";
        const stdout = job.result.stdout_tail || "";
        const stderr = job.result.stderr_tail || "";
        const errorNote = job.error
          ? `<div class="error-indicator" title="${esc(job.error)}">⚠</div>`
          : "";
        resultHtml = `
          ${errorNote}
          <div class="mono">exit=${exitCode}</div>
          <details class="job-logs">
            <summary>View logs</summary>
            ${stdout ? `<div class="mono">stdout:</div><pre class="mono">${stdout}</pre>` : ""}
            ${stderr ? `<div class="mono">stderr:</div><pre class="mono">${stderr}</pre>` : ""}
          </details>
        `;
      } else if (job.error || job.result) {
        const errorText = job.error ? `error: ${job.error}` : "";
        const resultText = job.result
          ? (typeof job.result === "string" ? job.result : JSON.stringify(job.result, null, 2))
          : "";
        const summary = job.error ? "View error" : "View output";
        resultHtml = `
          ${job.error ? `<span class="error-indicator" title="${esc(job.error)}">⚠</span>` : ""}
          <details class="job-logs">
            <summary>${summary}</summary>
            ${errorText ? `<div class="mono">${esc(errorText)}</div>` : ""}
            ${resultText ? `<pre class="mono">${esc(resultText)}</pre>` : ""}
          </details>
        `;
      } else {
        const text = formatResult(job);
        resultHtml = text
          ? `<div class="truncate" title="${esc(text)}">${esc(text)}</div>`
          : `<span class="muted">—</span>`;
      }
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${renderShortId(job.id, `/ui/jobs/${job.id}`)}</td>
        <td>${esc(job.job_type)}</td>
        <td>${statusBadge(job.status)}${pendingBadge}${runningBadge}</td>
        <td>${formatWhen(job)}</td>
        <td>${resultHtml}</td>
        <td>
          ${
            canCancel
              ? `<button class="btn small danger job-cancel" type="button" data-job-id="${job.id}">Cancel</button>`
              : job.status === "failed" || job.status === "canceled"
                ? `<button class="btn small secondary job-rerun" type="button" data-job-id="${job.id}">Rerun</button>`
                : `<span class="muted">—</span>`
          }
        </td>
      `;
      tbody.appendChild(row);
    });
    applyTimestampFormatting(tbody);
    requestAnimationFrame(() => {
      tbody.querySelectorAll(".job-logs pre").forEach((node) => {
        node.scrollTop = node.scrollHeight;
      });
    });
  }
  function buildParams() {
    const params = new URLSearchParams();
    params.set("page", String(page));
    params.set("page_size", String(pageSize));
    if (statusFilter && statusFilter.value) {
      params.set("status", statusFilter.value);
    }
    if (typeFilter && typeFilter.value.trim()) {
      params.set("job_type", typeFilter.value.trim());
    }
    return params;
  }
  async function refreshJobs() {
    const params = buildParams();
    const payload = await apiFetch(`/jobs?${params.toString()}`);
    const jobs = Array.isArray(payload) ? payload : payload.items || [];
    renderRows(jobs);
    if (!Array.isArray(payload)) {
      renderPager(pager, payload.total || 0, payload.page || page, payload.page_size || pageSize, (nextPage) => {
        page = nextPage;
        refreshJobs().catch((err) => alert(err));
      });
    }
    return jobs;
  }
  refresh.addEventListener("click", () => {
    refreshJobs().catch((err) => alert(err));
  });
  if (applyBtn) {
    applyBtn.addEventListener("click", () => {
      page = 1;
      pageSize = sizeSelect ? parseInt(sizeSelect.value, 10) || pageSize : pageSize;
      refreshJobs().catch((err) => alert(err));
    });
  }
  if (clearBtn) {
    clearBtn.addEventListener("click", () => {
      if (statusFilter) statusFilter.value = "";
      if (typeFilter) typeFilter.value = "";
      page = 1;
      pageSize = sizeSelect ? parseInt(sizeSelect.value, 10) || pageSize : pageSize;
      refreshJobs().catch((err) => alert(err));
    });
  }
  if (sizeSelect) {
    sizeSelect.addEventListener("change", () => {
      pageSize = parseInt(sizeSelect.value, 10) || pageSize;
      page = 1;
      refreshJobs().catch((err) => alert(err));
    });
  }
  if (cancelAll) {
    cancelAll.addEventListener("click", async () => {
      if (!confirm("Cancel all queued and running jobs?")) {
        return;
      }
      try {
        const data = await apiFetch("/jobs/cancel-all", { method: "POST" });
        showToast(`Canceled ${data.canceled} jobs`);
        refreshJobs().catch((err) => alert(err));
      } catch (err) {
        alert(err.message || String(err));
      }
    });
  }
  tbody.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    if (target.classList.contains("job-rerun")) {
      const jobId = target.dataset.jobId;
      if (!jobId) {
        return;
      }
      try {
        const payload = await apiFetch(`/jobs/${jobId}/rerun`, { method: "POST" });
        if (payload.status === "already_running") {
          showToast(`Already running: ${payload.job_id}`);
        } else {
          showToast(`Queued: ${payload.job_id}`);
        }
        refreshJobs().catch((err) => alert(err));
      } catch (err) {
        alert(err.message || String(err));
      }
      return;
    }
    if (!target.classList.contains("job-cancel")) {
      return;
    }
    const jobId = target.dataset.jobId;
    if (!jobId) {
      return;
    }
    if (!confirm("Cancel this job?")) {
      return;
    }
    try {
      await apiFetch(`/jobs/${jobId}/cancel`, { method: "POST" });
      showToast("Job canceled");
      refreshJobs().catch((err) => alert(err));
    } catch (err) {
      alert(err.message || String(err));
    }
  });
  let polling = false;
  async function poll() {
    if (polling) {
      return;
    }
    polling = true;
    try {
      const jobs = await refreshJobs();
      const running = jobs.some((job) => job.status === "queued" || job.status === "running");
      if (running) {
        setTimeout(() => {
          polling = false;
          poll();
        }, 4000);
      } else {
        polling = false;
      }
    } catch (err) {
      polling = false;
    }
  }
  poll();
}
function wireLogin() {
  const form = document.getElementById("login-form");
  if (!form) {
    return;
  }
  const tokenInput = document.getElementById("login-token");
  const error = document.getElementById("login-error");
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await apiFetch("/ui/login", {
        method: "POST",
        body: JSON.stringify({ token: tokenInput.value.trim() }),
      });
      window.location.href = "/ui";
    } catch (err) {
      error.style.display = "block";
      error.textContent = "Invalid token";
    }
  });
}
function wireRuntimeConfig() {
  const form = document.getElementById("runtime-config-form");
  if (!form) {
    return;
  }
  const error = document.getElementById("runtime-config-error");
  const dataEl = document.getElementById("runtime-config-data");
  let baseConfig = {};
  if (dataEl && dataEl.textContent) {
    try {
      baseConfig = JSON.parse(dataEl.textContent);
    } catch (err) {
      console.error(err);
    }
  }
  function parseList(value) {
    return value
      .split(/\n|,/)
      .map((item) => item.trim())
      .filter(Boolean);
  }
  function intOr(value, fallback) {
    const parsed = parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  function floatOr(value, fallback) {
    const parsed = parseFloat(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  function setValue(id, value) {
    const el = document.getElementById(id);
    if (!el) {
      return;
    }
    if (el.type === "checkbox") {
      el.checked = Boolean(value);
      return;
    }
    el.value = value ?? "";
  }
  function loadConfig() {
    const cfg = baseConfig || {};
    setValue("app-name", cfg.app?.name);
    setValue("app-timezone", cfg.app?.timezone);
    setValue("paths-data-dir", cfg.paths?.data_dir);
    setValue("paths-output-dir", cfg.paths?.output_dir);
    setValue("paths-run-reports-dir", cfg.paths?.run_reports_dir);
    setValue("publishing-format", cfg.publishing?.format);
    setValue("publishing-hugo-section", cfg.publishing?.hugo_section);
    setValue("publishing-write-json-index", cfg.publishing?.write_json_index);
    setValue("publishing-json-index-path", cfg.publishing?.json_index_path);
    setValue("publishing-public-base-url", cfg.publishing?.public_base_url);
    setValue("ingest-timeout", cfg.ingest?.http?.timeout_seconds);
    setValue("ingest-user-agent", cfg.ingest?.http?.user_agent);
    setValue("ingest-max-retries", cfg.ingest?.http?.max_retries);
    setValue("ingest-backoff", cfg.ingest?.http?.backoff_seconds);
    setValue("dedupe-enabled", cfg.ingest?.dedupe?.enabled);
    setValue("dedupe-strategy", cfg.ingest?.dedupe?.strategy);
    setValue("filters-allow", (cfg.ingest?.filters?.allow_keywords || []).join("\n"));
    setValue("filters-deny", (cfg.ingest?.filters?.deny_keywords || []).join("\n"));
    setValue("jobs-lock-timeout", cfg.jobs?.lock_timeout_seconds);
    setValue("llm-openai-background-enabled", cfg.llm?.openai_background_enabled);
    setValue("llm-openai-background-poll-seconds", cfg.llm?.openai_background_poll_seconds);
    setValue("llm-openai-background-max-seconds", cfg.llm?.openai_background_max_seconds);
    setValue("cve-enabled", cfg.cve?.enabled);
    setValue("cve-sync-interval", cfg.cve?.sync_interval_minutes);
    setValue("cve-results-per-page", cfg.cve?.results_per_page);
    setValue("cve-rate-limit", cfg.cve?.rate_limit_seconds);
    setValue("cve-backoff", cfg.cve?.backoff_seconds);
    setValue("cve-max-retries", cfg.cve?.max_retries);
    setValue("cve-prefer-v4", cfg.cve?.prefer_v4);
    setValue("runtime-llm-json", JSON.stringify(cfg.llm || {}, null, 2));
    setValue("runtime-per-source-json", JSON.stringify(cfg.per_source_tweaks || {}, null, 2));
  }
  loadConfig();
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    error.style.display = "none";
    const nextConfig = JSON.parse(JSON.stringify(baseConfig || {}));
    nextConfig.app = nextConfig.app || {};
    nextConfig.paths = nextConfig.paths || {};
    nextConfig.publishing = nextConfig.publishing || {};
    nextConfig.ingest = nextConfig.ingest || {};
    nextConfig.ingest.http = nextConfig.ingest.http || {};
    nextConfig.ingest.dedupe = nextConfig.ingest.dedupe || {};
    nextConfig.ingest.filters = nextConfig.ingest.filters || {};
    nextConfig.jobs = nextConfig.jobs || {};
    nextConfig.cve = nextConfig.cve || {};
    nextConfig.app.name = document.getElementById("app-name").value.trim();
    nextConfig.app.timezone = document.getElementById("app-timezone").value.trim();
    nextConfig.paths.data_dir = document.getElementById("paths-data-dir").value.trim();
    nextConfig.paths.output_dir = document.getElementById("paths-output-dir").value.trim();
    nextConfig.paths.run_reports_dir = document.getElementById("paths-run-reports-dir").value.trim();
    nextConfig.publishing.format = document.getElementById("publishing-format").value.trim();
    nextConfig.publishing.hugo_section = document
      .getElementById("publishing-hugo-section")
      .value.trim();
    nextConfig.publishing.write_json_index = document.getElementById(
      "publishing-write-json-index"
    ).checked;
    nextConfig.publishing.json_index_path = document
      .getElementById("publishing-json-index-path")
      .value.trim();
    nextConfig.publishing.public_base_url = document
      .getElementById("publishing-public-base-url")
      .value.trim();
    nextConfig.ingest.http.timeout_seconds = intOr(
      document.getElementById("ingest-timeout").value,
      nextConfig.ingest.http.timeout_seconds
    );
    nextConfig.ingest.http.user_agent = document.getElementById("ingest-user-agent").value.trim();
    nextConfig.ingest.http.max_retries = intOr(
      document.getElementById("ingest-max-retries").value,
      nextConfig.ingest.http.max_retries
    );
    nextConfig.ingest.http.backoff_seconds = floatOr(
      document.getElementById("ingest-backoff").value,
      nextConfig.ingest.http.backoff_seconds
    );
    nextConfig.ingest.dedupe.enabled = document.getElementById("dedupe-enabled").checked;
    nextConfig.ingest.dedupe.strategy = document.getElementById("dedupe-strategy").value.trim();
    nextConfig.ingest.filters.allow_keywords = parseList(
      document.getElementById("filters-allow").value
    );
    nextConfig.ingest.filters.deny_keywords = parseList(
      document.getElementById("filters-deny").value
    );
    nextConfig.jobs.lock_timeout_seconds = intOr(
      document.getElementById("jobs-lock-timeout").value,
      nextConfig.jobs.lock_timeout_seconds
    );
    nextConfig.llm = nextConfig.llm || {};
    nextConfig.llm.openai_background_enabled = document.getElementById(
      "llm-openai-background-enabled"
    ).checked;
    nextConfig.llm.openai_background_poll_seconds = intOr(
      document.getElementById("llm-openai-background-poll-seconds").value,
      nextConfig.llm.openai_background_poll_seconds
    );
    nextConfig.llm.openai_background_max_seconds = intOr(
      document.getElementById("llm-openai-background-max-seconds").value,
      nextConfig.llm.openai_background_max_seconds
    );
    nextConfig.cve.enabled = document.getElementById("cve-enabled").checked;
    nextConfig.cve.sync_interval_minutes = intOr(
      document.getElementById("cve-sync-interval").value,
      nextConfig.cve.sync_interval_minutes
    );
    nextConfig.cve.results_per_page = intOr(
      document.getElementById("cve-results-per-page").value,
      nextConfig.cve.results_per_page
    );
    nextConfig.cve.rate_limit_seconds = floatOr(
      document.getElementById("cve-rate-limit").value,
      nextConfig.cve.rate_limit_seconds
    );
    nextConfig.cve.backoff_seconds = floatOr(
      document.getElementById("cve-backoff").value,
      nextConfig.cve.backoff_seconds
    );
    nextConfig.cve.max_retries = intOr(
      document.getElementById("cve-max-retries").value,
      nextConfig.cve.max_retries
    );
    nextConfig.cve.prefer_v4 = document.getElementById("cve-prefer-v4").checked;
    try {
      nextConfig.llm = parseJsonField(
        document.getElementById("runtime-llm-json").value,
        {}
      );
      nextConfig.llm.openai_background_enabled = document.getElementById(
        "llm-openai-background-enabled"
      ).checked;
      nextConfig.llm.openai_background_poll_seconds = intOr(
        document.getElementById("llm-openai-background-poll-seconds").value,
        nextConfig.llm.openai_background_poll_seconds
      );
      nextConfig.llm.openai_background_max_seconds = intOr(
        document.getElementById("llm-openai-background-max-seconds").value,
        nextConfig.llm.openai_background_max_seconds
      );
      nextConfig.per_source_tweaks = parseJsonField(
        document.getElementById("runtime-per-source-json").value,
        {}
      );
    } catch (err) {
      error.textContent = "Invalid JSON in advanced fields";
      error.style.display = "block";
      return;
    }
    try {
      await apiFetch("/admin/config/runtime", {
        method: "PUT",
        body: JSON.stringify({ config: nextConfig }),
      });
      showToast("Config saved");
    } catch (err) {
      error.textContent = err.message || "Save failed";
      error.style.display = "block";
    }
  });
}
function wirePersonalization() {
  const form = document.getElementById("personalization-form");
  if (!form) {
    return;
  }
  const error = document.getElementById("personalization-error");
  const note = document.getElementById("personalization-note");
  const watchlistEnabled = document.getElementById("watchlist-enabled");
  const exposureMode = document.getElementById("watchlist-exposure");
  const rssEnabled = document.getElementById("watchlist-rss-enabled");
  const rssToken = document.getElementById("watchlist-rss-token");
  function setError(message) {
    if (!error) return;
    if (message) {
      error.textContent = message;
      error.style.display = "block";
    } else {
      error.textContent = "";
      error.style.display = "none";
    }
  }
  function setNote(message) {
    if (!note) return;
    if (message) {
      note.textContent = message;
      note.style.display = "block";
    } else {
      note.textContent = "";
      note.style.display = "none";
    }
  }
  async function load() {
    const data = await apiFetch("/admin/config/runtime");
    const cfg = data.config || {};
    const personalization = cfg.personalization || {};
    if (watchlistEnabled) {
      watchlistEnabled.checked = Boolean(personalization.watchlist_enabled);
    }
    if (exposureMode) {
      exposureMode.value = personalization.watchlist_exposure_mode || "private_only";
    }
    if (rssEnabled) {
      rssEnabled.checked = Boolean(personalization.rss_enabled);
    }
    if (rssToken) {
      rssToken.value = personalization.rss_private_token || "";
    }
  }
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    setError("");
    setNote("");
    const patch = {
      personalization: {
        watchlist_enabled: Boolean(watchlistEnabled?.checked),
        watchlist_exposure_mode: exposureMode?.value || "private_only",
        rss_enabled: Boolean(rssEnabled?.checked),
        rss_private_token: rssToken?.value || null,
      },
    };
    try {
      await apiFetch("/admin/api/config/patch", {
        method: "PUT",
        body: JSON.stringify({ config: patch }),
      });
      setNote("Personalization settings saved.");
    } catch (err) {
      setError(err.message || String(err));
    }
  });
  load().catch((err) => setError(err.message || String(err)));
}
function parseJsonField(value, fallback) {
  if (!value) {
    return fallback;
  }
  try {
    return JSON.parse(value);
  } catch (err) {
    alert("Invalid JSON: " + err);
    throw err;
  }
}
function wireAiProviders() {
  const form = document.getElementById("provider-form");
  if (!form) {
    return;
  }
  const idField = document.getElementById("provider-id");
  const nameField = document.getElementById("provider-name");
  const typeField = document.getElementById("provider-type");
  const baseField = document.getElementById("provider-base-url");
  const timeoutField = document.getElementById("provider-timeout");
  const retriesField = document.getElementById("provider-retries");
  const enabledField = document.getElementById("provider-enabled");
  const resetBtn = document.getElementById("provider-reset");
  function resetForm() {
    idField.value = "";
    nameField.value = "";
    typeField.value = "openai_compatible";
    baseField.value = "";
    timeoutField.value = "30";
    retriesField.value = "2";
    enabledField.checked = true;
  }
  resetBtn.addEventListener("click", resetForm);
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      id: idField.value.trim() || undefined,
      name: nameField.value.trim(),
      type: typeField.value,
      base_url: baseField.value.trim() || undefined,
      timeout_s: parseInt(timeoutField.value, 10),
      retries: parseInt(retriesField.value, 10),
      is_enabled: enabledField.checked,
    };
    const target = idField.value ? `/admin/ai/providers/${idField.value}` : "/admin/ai/providers";
    const method = idField.value ? "PATCH" : "POST";
    try {
      await apiFetch(target, { method, body: JSON.stringify(payload) });
      window.location.reload();
    } catch (err) {
      alert(err);
    }
  });
  document.querySelectorAll(".edit-provider").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      idField.value = row.dataset.providerId;
      nameField.value = row.querySelector(".provider-name").textContent.trim();
      typeField.value = row.querySelector(".provider-type").textContent.trim();
      baseField.value = row.querySelector(".provider-base-url").textContent.trim();
      timeoutField.value = row.dataset.timeout || row.querySelector(".provider-timeout")?.textContent.trim() || "30";
      retriesField.value = row.dataset.retries || row.querySelector(".provider-retries")?.textContent.trim() || "2";
    });
  });
  document.querySelectorAll(".toggle-provider").forEach((checkbox) => {
    checkbox.addEventListener("change", async () => {
      const row = checkbox.closest("tr");
      const providerId = row.dataset.providerId;
      try {
        await apiFetch(`/admin/ai/providers/${providerId}`, {
          method: "PATCH",
          body: JSON.stringify({ is_enabled: checkbox.checked }),
        });
      } catch (err) {
        alert(err);
        checkbox.checked = !checkbox.checked;
      }
    });
  });
  document.querySelectorAll(".set-provider-key").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const providerId = row.dataset.providerId;
      const apiKey = prompt("Enter API key (will be stored encrypted)");
      if (!apiKey) {
        return;
      }
      try {
        await apiFetch(`/admin/ai/providers/${providerId}/secret`, {
          method: "POST",
          body: JSON.stringify({ api_key: apiKey }),
        });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
  document.querySelectorAll(".clear-provider-key").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const providerId = row.dataset.providerId;
      if (!confirm("Clear API key?")) {
        return;
      }
      try {
        await apiFetch(`/admin/ai/providers/${providerId}/secret`, { method: "DELETE" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
  document.querySelectorAll(".test-provider").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const providerId = row.dataset.providerId;
      try {
        await apiFetch(`/admin/ai/providers/${providerId}/test`, { method: "POST" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
}
function wireAiModels() {
  const form = document.getElementById("model-form");
  if (!form) {
    return;
  }
  const idField = document.getElementById("model-id");
  const providerField = document.getElementById("model-provider");
  const nameField = document.getElementById("model-name");
  const contextField = document.getElementById("model-context");
  const tagsField = document.getElementById("model-tags");
  const paramsField = document.getElementById("model-params");
  const enabledField = document.getElementById("model-enabled");
  const resetBtn = document.getElementById("model-reset");
  function resetForm() {
    idField.value = "";
    nameField.value = "";
    contextField.value = "";
    tagsField.value = "";
    paramsField.value = "";
    enabledField.checked = true;
  }
  resetBtn.addEventListener("click", resetForm);
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      id: idField.value.trim() || undefined,
      provider_id: providerField.value,
      model_name: nameField.value.trim(),
      max_context: contextField.value ? parseInt(contextField.value, 10) : undefined,
      tags: tagsField.value.trim(),
      default_params: parseJsonField(paramsField.value.trim(), {}),
      is_enabled: enabledField.checked,
    };
    const target = idField.value ? `/admin/ai/models/${idField.value}` : "/admin/ai/models";
    const method = idField.value ? "PATCH" : "POST";
    try {
      await apiFetch(target, { method, body: JSON.stringify(payload) });
      window.location.reload();
    } catch (err) {
      alert(err);
    }
  });
  document.querySelectorAll(".edit-model").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      idField.value = row.dataset.modelId;
      providerField.value = row.dataset.providerId;
      nameField.value = row.querySelector(".model-name").textContent.trim();
      tagsField.value = row.querySelector(".model-tags").textContent.trim();
    });
  });
  document.querySelectorAll(".toggle-model").forEach((checkbox) => {
    checkbox.addEventListener("change", async () => {
      const row = checkbox.closest("tr");
      const modelId = row.dataset.modelId;
      try {
        await apiFetch(`/admin/ai/models/${modelId}`, {
          method: "PATCH",
          body: JSON.stringify({ is_enabled: checkbox.checked }),
        });
      } catch (err) {
        alert(err);
        checkbox.checked = !checkbox.checked;
      }
    });
  });
  document.querySelectorAll(".delete-model").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const modelId = row.dataset.modelId;
      if (!confirm("Delete model?")) {
        return;
      }
      try {
        await apiFetch(`/admin/ai/models/${modelId}`, { method: "DELETE" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
}
function wireAiPrompts() {
  const form = document.getElementById("prompt-form");
  if (!form) {
    return;
  }
  const tableBody = document.getElementById("prompt-table-body");
  const idField = document.getElementById("prompt-id");
  const nameField = document.getElementById("prompt-name");
  const versionField = document.getElementById("prompt-version");
  const systemField = document.getElementById("prompt-system");
  const userField = document.getElementById("prompt-user");
  const notesField = document.getElementById("prompt-notes");
  const resetBtn = document.getElementById("prompt-reset");
  function resetForm() {
    idField.value = "";
    nameField.value = "";
    versionField.value = "v1";
    systemField.value = "";
    userField.value = "";
    notesField.value = "";
  }
  async function refreshPrompts() {
    if (!tableBody) {
      return;
    }
    const prompts = await apiFetch("/admin/ai/prompts");
    tableBody.innerHTML = prompts
      .map(
        (prompt) => `
        <tr data-prompt-id="${esc(prompt.id)}">
          <td class="prompt-name">${esc(prompt.name)}</td>
          <td class="prompt-version">${esc(prompt.version)}</td>
          <td class="actions">
            <button class="btn small edit-prompt" type="button">Edit</button>
            <button class="btn small danger delete-prompt" type="button">Delete</button>
          </td>
        </tr>
      `
      )
      .join("");
  }
  resetBtn.addEventListener("click", resetForm);
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      id: idField.value.trim() || undefined,
      name: nameField.value.trim(),
      version: versionField.value.trim(),
      system_template: systemField.value,
      user_template: userField.value,
      notes: notesField.value.trim(),
    };
    const target = idField.value ? `/admin/ai/prompts/${idField.value}` : "/admin/ai/prompts";
    const method = idField.value ? "PATCH" : "POST";
    try {
      await apiFetch(target, { method, body: JSON.stringify(payload) });
      await refreshPrompts();
      showToast("Prompt saved");
    } catch (err) {
      alert(err);
    }
  });
  if (tableBody) {
    tableBody.addEventListener("click", async (event) => {
      const btn = event.target.closest("button");
      if (!btn) {
        return;
      }
      const row = btn.closest("tr");
      if (!row) {
        return;
      }
      const promptId = row.dataset.promptId;
      if (btn.classList.contains("edit-prompt")) {
        try {
          const prompt = await apiFetch(`/admin/ai/prompts/${promptId}`);
          idField.value = prompt.id || "";
          nameField.value = prompt.name || "";
          versionField.value = prompt.version || "v1";
          systemField.value = prompt.system_template || "";
          userField.value = prompt.user_template || "";
          notesField.value = prompt.notes || "";
        } catch (err) {
          alert(err);
        }
      }
      if (btn.classList.contains("delete-prompt")) {
        if (!confirm("Delete prompt?")) {
          return;
        }
        try {
          await apiFetch(`/admin/ai/prompts/${promptId}`, { method: "DELETE" });
          await refreshPrompts();
          showToast("Prompt deleted");
        } catch (err) {
          alert(err);
        }
      }
    });
  }
  refreshPrompts().catch(() => {});
}
function wireAiSchemas() {
  const form = document.getElementById("schema-form");
  if (!form) {
    return;
  }
  const idField = document.getElementById("schema-id");
  const nameField = document.getElementById("schema-name");
  const versionField = document.getElementById("schema-version");
  const jsonField = document.getElementById("schema-json");
  const resetBtn = document.getElementById("schema-reset");
  function resetForm() {
    idField.value = "";
    nameField.value = "";
    versionField.value = "v1";
    jsonField.value = "";
  }
  resetBtn.addEventListener("click", resetForm);
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      id: idField.value.trim() || undefined,
      name: nameField.value.trim(),
      version: versionField.value.trim(),
      json_schema: parseJsonField(jsonField.value.trim(), {}),
    };
    const target = idField.value ? `/admin/ai/schemas/${idField.value}` : "/admin/ai/schemas";
    const method = idField.value ? "PATCH" : "POST";
    try {
      await apiFetch(target, { method, body: JSON.stringify(payload) });
      window.location.reload();
    } catch (err) {
      alert(err);
    }
  });
  document.querySelectorAll(".edit-schema").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      idField.value = row.dataset.schemaId;
      nameField.value = row.querySelector(".schema-name").textContent.trim();
      versionField.value = row.querySelector(".schema-version").textContent.trim();
    });
  });
  document.querySelectorAll(".delete-schema").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const schemaId = row.dataset.schemaId;
      if (!confirm("Delete schema?")) {
        return;
      }
      try {
        await apiFetch(`/admin/ai/schemas/${schemaId}`, { method: "DELETE" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
}
function wireAiProfiles() {
  const form = document.getElementById("profile-form");
  if (!form) {
    return;
  }
  const idField = document.getElementById("profile-id");
  const nameField = document.getElementById("profile-name");
  const providerField = document.getElementById("profile-provider");
  const modelField = document.getElementById("profile-model");
  const promptField = document.getElementById("profile-prompt");
  const schemaField = document.getElementById("profile-schema");
  const paramsField = document.getElementById("profile-params");
  const fallbackField = document.getElementById("profile-fallback");
  const enabledField = document.getElementById("profile-enabled");
  const resetBtn = document.getElementById("profile-reset");
  function resetForm() {
    idField.value = "";
    nameField.value = "";
    paramsField.value = "";
    fallbackField.value = "";
    enabledField.checked = true;
  }
  resetBtn.addEventListener("click", resetForm);
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      id: idField.value.trim() || undefined,
      name: nameField.value.trim(),
      primary_provider_id: providerField.value,
      primary_model_id: modelField.value,
      prompt_id: promptField.value,
      schema_id: schemaField.value || null,
      params: parseJsonField(paramsField.value.trim(), {}),
      fallback: parseJsonField(fallbackField.value.trim(), []),
      is_enabled: enabledField.checked,
    };
    const target = idField.value ? `/admin/ai/profiles/${idField.value}` : "/admin/ai/profiles";
    const method = idField.value ? "PATCH" : "POST";
    try {
      await apiFetch(target, { method, body: JSON.stringify(payload) });
      window.location.reload();
    } catch (err) {
      alert(err);
    }
  });
  document.querySelectorAll(".edit-profile").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      idField.value = row.dataset.profileId;
      nameField.value = row.querySelector(".profile-name").textContent.trim();
      providerField.value = row.dataset.providerId || "";
      modelField.value = row.dataset.modelId || "";
      promptField.value = row.dataset.promptId || "";
      schemaField.value = row.dataset.schemaId || "";
      paramsField.value = row.dataset.params || "";
      fallbackField.value = row.dataset.fallback || "";
      enabledField.checked = row.dataset.enabled === "True" || row.dataset.enabled === "true";
    });
  });
  document.querySelectorAll(".toggle-profile").forEach((checkbox) => {
    checkbox.addEventListener("change", async () => {
      const row = checkbox.closest("tr");
      const profileId = row.dataset.profileId;
      try {
        await apiFetch(`/admin/ai/profiles/${profileId}`, {
          method: "PATCH",
          body: JSON.stringify({ is_enabled: checkbox.checked }),
        });
      } catch (err) {
        alert(err);
        checkbox.checked = !checkbox.checked;
      }
    });
  });
  document.querySelectorAll(".delete-profile").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const profileId = row.dataset.profileId;
      if (!confirm("Delete profile?")) {
        return;
      }
      try {
        await apiFetch(`/admin/ai/profiles/${profileId}`, { method: "DELETE" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
  document.querySelectorAll(".test-profile").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const profileId = row.dataset.profileId;
      const text = prompt("Enter test input");
      if (!text) {
        return;
      }
      try {
        const result = await apiFetch(`/admin/ai/profiles/${profileId}/test`, {
          method: "POST",
          body: JSON.stringify({ text }),
        });
        alert(
          result.schema_valid
            ? "Schema valid"
            : `Schema invalid: ${result.schema_error || ""}`
        );
      } catch (err) {
        alert(err);
      }
    });
  });
}
function wireAiRouting() {
  document.querySelectorAll(".save-route").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const stage = row.dataset.stageName;
      const select = row.querySelector(".route-profile");
      const profileId = select.value;
      if (!profileId) {
        alert("Select a profile first");
        return;
      }
      try {
        await apiFetch("/admin/ai/pipeline-routing", {
          method: "POST",
          body: JSON.stringify({ stage_name: stage, profile_id: profileId }),
        });
        alert("Saved");
      } catch (err) {
        alert(err);
      }
    });
  });
}
function wireAiStageControls() {
  const clearBtn = document.getElementById("ai-clear-queued");
  const result = document.getElementById("ai-clear-queued-result");
  if (!clearBtn) {
    return;
  }
  clearBtn.addEventListener("click", async () => {
    try {
      const data = await apiFetch("/admin/ai/clear-queued", { method: "POST" });
      if (result) {
        result.textContent = `Canceled ${data.cleared || 0} queued LLM jobs.`;
      }
      showToast(`Canceled ${data.cleared || 0} queued LLM jobs.`);
    } catch (err) {
      if (result) {
        result.textContent = err.message || String(err);
      }
      showToast(err.message || String(err));
    }
  });
}
function wireCveSearch() {
  const form = document.getElementById("cve-search-form");
  const table = document.getElementById("cve-table");
  if (!form || !table) {
    return;
  }
  const tbody = table.querySelector("tbody");
  const pager = document.getElementById("cve-pager");
  const error = document.getElementById("cve-error");
  const pageSize = 50;
  let currentPage = 1;
  let cveSort = { index: null, dir: "asc" };
  function getCveSortValue(cell, index) {
    if (!cell) return "";
    const text = cell.textContent?.trim() || "";
    if (index === 1 || index === 2) {
      const parsed = Date.parse(text);
      return Number.isNaN(parsed) ? text.toLowerCase() : parsed;
    }
    if (index === 4) {
      const num = parseFloat(text);
      return Number.isNaN(num) ? text.toLowerCase() : num;
    }
    return text.toLowerCase();
  }
  function applyCveSort() {
    if (!tbody || cveSort.index === null) return;
    const rows = Array.from(tbody.querySelectorAll("tr"));
    rows.sort((a, b) => {
      const aCell = a.children[cveSort.index];
      const bCell = b.children[cveSort.index];
      const av = getCveSortValue(aCell, cveSort.index);
      const bv = getCveSortValue(bCell, cveSort.index);
      if (av < bv) return cveSort.dir === "asc" ? -1 : 1;
      if (av > bv) return cveSort.dir === "asc" ? 1 : -1;
      return 0;
    });
    rows.forEach((row) => tbody.appendChild(row));
  }
  function setupCveSort() {
    const headers = table.querySelectorAll("thead th");
    headers.forEach((th, index) => {
      th.classList.add("sortable");
      th.addEventListener("click", () => {
        if (cveSort.index === index) {
          cveSort.dir = cveSort.dir === "asc" ? "desc" : "asc";
        } else {
          cveSort.index = index;
          cveSort.dir = "asc";
        }
        headers.forEach((h) => h.classList.remove("sorted-asc", "sorted-desc"));
        th.classList.add(cveSort.dir === "asc" ? "sorted-asc" : "sorted-desc");
        applyCveSort();
      });
    });
  }
  async function load(page) {
    currentPage = page;
    if (error) {
      error.style.display = "none";
      error.textContent = "";
    }
    const query = document.getElementById("cve-query").value.trim();
    const severitySelect = document.getElementById("cve-severity");
    const severities = Array.from(severitySelect.selectedOptions).map((opt) => opt.value);
    const minCvss = document.getElementById("cve-min-cvss").value;
    const after = document.getElementById("cve-after").value;
    const before = document.getElementById("cve-before").value;
    const missingDesc = document.getElementById("cve-missing-description").checked;
    const missingProducts = document.getElementById("cve-missing-products").checked;
    const kevOnly = document.getElementById("cve-kev-only").checked;
    const params = new URLSearchParams();
    if (query) params.set("query", query);
    if (severities.length) params.set("severity", severities.join(","));
    if (after) params.set("after", after);
    if (before) params.set("before", before);
    if (kevOnly) params.set("kev", "true");
    params.set("page", String(page));
    params.set("page_size", String(pageSize));
    const data = await apiFetch(`/admin/api/cves?${params.toString()}`);
    tbody.innerHTML = "";
    if (!data.items || !data.items.length) {
      const row = document.createElement("tr");
      row.innerHTML = `<td colspan="9" class="muted">No CVEs found.</td>`;
      tbody.appendChild(row);
      renderPager(pager, data.total, data.page, data.page_size, load);
      return;
    }
    data.items.forEach((item) => {
      const row = document.createElement("tr");
      const missingDescFlag = !item.summary;
      const hasProducts = (item.affected_products && item.affected_products.length) || (item.product_versions && item.product_versions.length);
      const missingProductsFlag = !hasProducts;
      const kevDue = item.kev_due_date ? formatDateOnly(item.kev_due_date) : "";
      const kevPill = item.kev_cve_id
        ? `<span class="status-pill status-warn" title="${kevDue ? `Due ${esc(kevDue)}` : "Known Exploited Vulnerability"}">KEV</span>`
        : `<span class="status-pill status-muted">-</span>`;
      const statusPills = [
        missingDescFlag ? '<span class="status-pill status-warn">Missing description</span>' : '<span class="status-pill status-ok">Desc ok</span>',
        missingProductsFlag ? '<span class="status-pill status-warn">Missing products</span>' : '<span class="status-pill status-ok">Products ok</span>',
      ].join(" ");
      const actions = `
        <div class="table-actions compact">
          <button class="btn small action-cve-desc" data-cve-id="${item.cve_id}" ${missingDescFlag ? "" : "disabled"}>Fetch desc</button>
          <button class="btn small action-cve-products" data-cve-id="${item.cve_id}" ${missingProductsFlag ? "" : "disabled"}>Enrich products</button>
        </div>
      `;
      row.innerHTML = `
        <td><a href="/ui/cves/${item.cve_id}">${item.cve_id}</a></td>
        <td>${esc(formatDateOnly(item.published_at))}</td>
        <td>${esc(formatDateOnly(item.last_modified_at))}</td>
        <td>${item.preferred_base_severity || ""}</td>
        <td>${item.preferred_base_score || ""}</td>
        <td class="cve-kev">${kevPill}</td>
        <td class="truncate" title="${item.summary || ""}">${item.summary || ""}</td>
        <td class="cve-status">${statusPills}</td>
        <td class="actions">${actions}</td>
      `;
      tbody.appendChild(row);
    });
    applyCveSort();
    renderPager(pager, data.total, data.page, data.page_size, load);
  }
  tbody.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const cveId = target.dataset.cveId;
    if (!cveId) {
      return;
    }
    try {
      if (target.classList.contains("action-cve-desc")) {
        const result = await apiFetch(`/admin/api/cves/${cveId}/refresh`, { method: "POST" });
        toast(`Queued ${renderShortId(result.job_id || cveId)}`, "success");
        load(currentPage);
      }
      if (target.classList.contains("action-cve-products")) {
        const result = await apiFetch(`/admin/api/cves/${cveId}/enrich_products`, { method: "POST" });
        toast(result.status === "already_queued" ? "Already queued" : `Queued ${renderShortId(result.job_id || cveId)}`, "success");
        load(currentPage);
      }
    } catch (err) {
      toast(err.message || String(err), "error");
    }
  });
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    load(1).catch((err) => {
      if (error) {
        error.textContent = err.message || String(err);
        error.style.display = "block";
      }
    });
  });
  load(currentPage).catch((err) => {
    if (error) {
      error.textContent = err.message || String(err);
      error.style.display = "block";
    }
  });
  setupCveSort();
}
function wireCveDetail() {
  const container = document.getElementById("cve-detail");
  if (!container) {
    return;
  }
  const cveId = container.dataset.cveId;
  apiFetch(`/admin/api/cves/${cveId}`)
    .then((item) => {
      const watchlistEnabled = item.watchlist_enabled !== false;
      const scopeBadge =
        item.in_scope === null || item.in_scope === undefined
          ? "Watchlist disabled"
          : item.in_scope
          ? "In Scope"
          : "Out of Scope";
      const scopeClass =
        item.in_scope === true ? "status-ok" : item.in_scope === false ? "status-error" : "status-muted";
      const scopeReasons = (item.scope_reasons || []).join(", ");
      const preferredVersion = item.preferred_cvss_version || "unknown";
      const v31 = item.cvss_v31 || null;
      const v40 = item.cvss_v40 || null;
      const v31List = item.cvss_v31_list || [];
      const v40List = item.cvss_v40_list || [];
      const vendorProducts = item.vendor_products || [];
      const cpes = item.affected_cpes || [];
      const domains = item.reference_domains || [];
      const productVersions = item.product_versions || [];
      const threatActors = item.threat_actors || [];
      const kev = item.kev || null;
      const kevDue = kev && kev.due_date ? kev.due_date : "";
      const kevAdded = kev && kev.added_at ? kev.added_at : "";
      const kevRansom = kev && kev.ransomware_use ? kev.ransomware_use : "";
      const kevName = kev && kev.vulnerability_name ? kev.vulnerability_name : "";
      const kevShort = kev && kev.short_description ? kev.short_description : "";
      const kevRequired = kev && kev.required_action ? kev.required_action : "";
      const kevNotes = kev && kev.notes ? kev.notes : "";
      const kevBadge = kev
        ? `<div class="status-pill status-warn">KEV${kevDue ? `: Due ${esc(kevDue)}` : ""}</div>`
        : `<div class="status-pill status-muted">KEV: Not listed</div>`;
      const otherScores = [...v31List, ...v40List]
        .map((entry) => {
          const version = entry.version || "unknown";
          const type = entry.type || "";
          const source = entry.source || "";
          const score = entry.baseScore ?? "";
          const severity = entry.baseSeverity || "";
          const vector = entry.vectorString || "";
          return `${version} ${type} ${source} ${score} ${severity} ${vector}`.trim();
        })
        .filter(Boolean);
      container.innerHTML = `
        <div class="kv">
          <div><strong>${item.cve_id}</strong></div>
          <div>Published: ${esc(formatTimestamp(item.published_at))}</div>
          <div>Modified: ${esc(formatTimestamp(item.last_modified_at))}</div>
          <div>Last seen: ${esc(formatTimestamp(item.last_seen_at))}</div>
          <div>Preferred CVSS (${preferredVersion}): ${item.preferred_base_score || ""} ${
        item.preferred_base_severity ? `(${item.preferred_base_severity})` : ""
      }</div>
          <div>Preferred Vector: ${item.preferred_vector || ""}</div>
          <div class="status-pill ${scopeClass}">${scopeBadge}</div>
          ${kevBadge}
          ${scopeReasons ? `<div class="muted">Reasons: ${scopeReasons}</div>` : ""}
        </div>
        ${kev ? `<h3>Known Exploited (KEV)</h3>
        <div class="kv">
          ${kevName ? `<div>Name: ${esc(kevName)}</div>` : ""}
          ${kevShort ? `<div>Summary: ${esc(kevShort)}</div>` : ""}
          ${kevAdded ? `<div>Added: ${esc(kevAdded)}</div>` : ""}
          ${kevDue ? `<div>Due: ${esc(kevDue)}</div>` : ""}
          ${kevRansom ? `<div>Ransomware: ${esc(kevRansom)}</div>` : ""}
          ${kevRequired ? `<div>Required Action: ${esc(kevRequired)}</div>` : ""}
          ${kevNotes ? `<div>Notes: ${esc(kevNotes)}</div>` : ""}
        </div>` : ""}
        <h3>CVSS Versions</h3>
        <div class="kv">
          <div>CVSS v3.1: ${
            v31 ? `${v31.baseScore || ""} ${v31.baseSeverity || ""} ${v31.vectorString || ""}` : "None"
          }</div>
          <div>CVSS v4.0: ${
            v40 ? `${v40.baseScore || ""} ${v40.baseSeverity || ""} ${v40.vectorString || ""}` : "None"
          }</div>
        </div>
        <h3>Other Scores</h3>
        <pre class="mono">${otherScores.length ? otherScores.join("\\n") : "None"}</pre>
        <h3>Description</h3>
        <p>${item.description_text || ""}</p>
        <h3>Affected Products</h3>
        ${
          vendorProducts.length
            ? `<ul>${vendorProducts
                .map((vp) => {
                  let vendor = vp.vendor_display || vp.vendor_norm || "";
                  if (vendor && vendor.toLowerCase() === "unknown") {
                    vendor = "";
                  }
                  const product = vp.product_display || vp.product_norm || "";
                  const label = vendor && product ? `${vendor} — ${product}` : product || vendor || "";
                  return `<li>${esc(label)}</li>`;
                })
                .join("")}</ul>`
            : `<p class="muted">No products linked.</p>`
        }
        ${vendorProducts.length && watchlistEnabled ? `
          <div class="actions">
            ${vendorProducts
              .map(
                (vp) =>
                  `<button class="btn small secondary add-watch-vendor" data-vendor="${vp.vendor_display}">Watch Vendor ${vp.vendor_display}</button>
                   <button class="btn small secondary add-watch-product" data-vendor="${vp.vendor_norm}" data-product="${vp.product_display}">Watch Product ${vp.product_display}</button>`
              )
              .join(" ")}
          </div>
        ` : ""}
        <h3>Product Versions</h3>
        <pre class="mono">${productVersions.length ? productVersions.join("\\n") : "None found"}</pre>
        <h3>Threat Actors</h3>
        ${
          threatActors.length
            ? `<ul>${threatActors
                .map((actor) => {
                  const name = actor.display_name || actor.actor_key || "";
                  const kind = actor.actor_type || "";
                  const country = actor.country || "";
                  const confidence =
                    actor.confidence === null || actor.confidence === undefined ? "" : ` (${actor.confidence})`;
                  const meta = [kind, country].filter(Boolean).join(" · ");
                  return `<li>${esc(name)}${meta ? ` <span class="muted">(${esc(meta)}${esc(confidence)})</span>` : ""}</li>`;
                })
                .join("")}</ul>`
            : `<p class="muted">None found.</p>`
        }
<h3>Affected CPEs</h3>
        <pre class="mono">${cpes.length ? cpes.join("\\n") : "None found"}</pre>
        <h3>Reference Domains</h3>
        <pre class="mono">${domains.length ? domains.join("\\n") : "None found"}</pre>
      `;      if (!watchlistEnabled) {
        return;
      }
      container.querySelectorAll(".add-watch-vendor").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const name = btn.dataset.vendor;
          if (!name) {
            return;
          }
          try {
            await apiFetch("/admin/api/watchlist/vendors", {
              method: "POST",
              body: JSON.stringify({ display_name: name, enabled: true }),
            });
            showToast(`Watching vendor ${name}`);
          } catch (err) {
            showToast(err.message || String(err));
          }
        });
      });
      container.querySelectorAll(".add-watch-product").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const name = btn.dataset.product;
          const vendor = btn.dataset.vendor || "";
          if (!name) {
            return;
          }
          try {
            await apiFetch("/admin/api/watchlist/products", {
              method: "POST",
              body: JSON.stringify({
                display_name: name,
                vendor_norm: vendor || null,
                match_mode: "exact",
                enabled: true,
              }),
            });
            showToast(`Watching product ${name}`);
          } catch (err) {
            showToast(err.message || String(err));
          }
        });
      });
    })
    .catch((err) => {
      container.textContent = err.message || String(err);
    });
}
function wireCveSettings() {
  const form = document.getElementById("cve-settings-form");
  if (!form) {
    return;
  }
  const error = document.getElementById("cve-settings-error");
  const note = document.getElementById("cve-settings-note");
  function setSeverities(values) {
    const select = document.getElementById("cve-severities");
    Array.from(select.options).forEach((opt) => {
      opt.selected = values.includes(opt.value);
    });
  }
  async function load() {
    const data = await apiFetch("/admin/api/cves/settings");
    const settings = data.settings || {};
    document.getElementById("cve-enabled").checked = settings.enabled ?? true;
    document.getElementById("cve-schedule").value = settings.schedule_minutes ?? 60;
    document.getElementById("cve-api-base").value =
      settings.nvd?.api_base ?? "https://services.nvd.nist.gov/rest/json/cves/2.0";
    document.getElementById("cve-results").value = settings.nvd?.results_per_page ?? 2000;
    document.getElementById("cve-min").value = settings.filters?.min_cvss ?? "";
    document.getElementById("cve-known-score").checked =
      settings.filters?.require_known_score ?? false;
    setSeverities(settings.filters?.severities || []);
    document.getElementById("cve-vendors").value = (settings.filters?.vendor_keywords || []).join(
      ", "
    );
    document.getElementById("cve-products").value = (
      settings.filters?.product_keywords || []
    ).join(", ");
    document.getElementById("cve-retention").value = settings.retention_days ?? 365;
    if (note) {
      note.textContent = `Last run: ${formatTimestamp(settings.last_run_at) || "unknown"}`;
    }
  }
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    error.style.display = "none";
    const settings = {
      enabled: document.getElementById("cve-enabled").checked,
      schedule_minutes: parseInt(document.getElementById("cve-schedule").value, 10),
      nvd: {
        api_base: document.getElementById("cve-api-base").value.trim(),
        results_per_page: parseInt(document.getElementById("cve-results").value, 10),
      },
      filters: {
        min_cvss: document.getElementById("cve-min").value
          ? parseFloat(document.getElementById("cve-min").value)
          : null,
        severities: Array.from(document.getElementById("cve-severities").selectedOptions).map(
          (opt) => opt.value
        ),
        require_known_score: document.getElementById("cve-known-score").checked,
        vendor_keywords: document
          .getElementById("cve-vendors")
          .value.split(",")
          .map((item) => item.trim())
          .filter(Boolean),
        product_keywords: document
          .getElementById("cve-products")
          .value.split(",")
          .map((item) => item.trim())
          .filter(Boolean),
      },
      retention_days: parseInt(document.getElementById("cve-retention").value, 10),
    };
    try {
      await apiFetch("/admin/api/cves/settings", {
        method: "PUT",
        body: JSON.stringify({ settings }),
      });
      showToast("Settings saved");
    } catch (err) {
      error.textContent = err.message || "Save failed";
      error.style.display = "block";
    }
  });
  const runNow = document.getElementById("cve-run-now");
  const testNow = document.getElementById("cve-test-now");
  const testOutput = document.getElementById("cve-test-output");
  const completenessCards = document.getElementById("cve-completeness-cards");
  const missingTable = document.querySelector("#cve-missing-table tbody");
  if (runNow) {
    runNow.addEventListener("click", async () => {
      try {
        await apiFetch("/admin/api/cves/run", { method: "POST", body: JSON.stringify({}) });
        showToast("CVE sync enqueued");
      } catch (err) {
        error.textContent = err.message || String(err);
        error.style.display = "block";
      }
    });
  }
  if (testNow && testOutput) {
    testNow.addEventListener("click", async () => {
      try {
        testOutput.textContent = "Running test...";
        const hours = parseInt(document.getElementById("cve-test-hours").value, 10) || 24;
        const limit = parseInt(document.getElementById("cve-test-limit").value, 10) || 5;
        const data = await apiFetch("/admin/api/cves/test", {
          method: "POST",
          body: JSON.stringify({ hours, limit }),
        });
        testOutput.textContent = JSON.stringify(data, null, 2);
      } catch (err) {
        testOutput.textContent = err.message || String(err);
      }
    });
  }
  async function loadCompleteness() {
    if (!completenessCards || !missingTable) {
      return;
    }
    const data = await apiFetch("/admin/api/cves/completeness?limit=20");
    const counts = data.counts || {};
    completenessCards.innerHTML = "";
    [
      ["Total", counts.total ?? 0],
      ["With Description", counts.with_description ?? 0],
      ["Good Description", counts.good_description ?? 0],
      ["With Products", counts.with_products ?? 0],
      ["With Domains", counts.with_domains ?? 0],
      ["Has Any CVSS", counts.has_any_cvss ?? 0],
      ["Has v3.1", counts.has_v31 ?? 0],
      ["Has v4.0", counts.has_v40 ?? 0],
    ].forEach(([label, value]) => {
      const card = document.createElement("div");
      card.className = "stat-card";
      card.innerHTML = `<div class="stat-label">${label}</div><div class="stat-value">${value}</div>`;
      completenessCards.appendChild(card);
    });
    const byCategory = data.missing_by_category || {};
    const rows = [];
    ["description", "products", "domains", "cvss"].forEach((key) => {
      (byCategory[key] || []).forEach((cveId) => {
        rows.push({ cve_id: cveId, missing: key });
      });
    });
    missingTable.innerHTML = "";
    rows.slice(0, 20).forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td><a href="/ui/cves/${item.cve_id}">${item.cve_id}</a></td>
        <td>${item.missing}</td>
      `;
      missingTable.appendChild(row);
    });
  }
  Promise.all([load(), loadCompleteness()]).catch((err) => {
    error.textContent = err.message || String(err);
    error.style.display = "block";
  });
}

function wireScheduleSettings() {
  const form = document.getElementById("schedule-settings-form");
  if (!form) {
    return;
  }
  const error = document.getElementById("schedule-settings-error");
  const note = document.getElementById("schedule-settings-note");
  async function load() {
    const data = await apiFetch("/admin/api/schedules/settings");
    const settings = data.settings || {};
    document.getElementById("schedule-timezone").value = settings.timezone || "";
    const tasks = settings.tasks || {};
    const brief = tasks.daily_brief || {};
    const podcast = tasks.podcast || {};
    document.getElementById("schedule-brief-enabled").checked = brief.enabled ?? false;
    document.getElementById("schedule-brief-time").value = brief.time || "07:30";
    document.getElementById("schedule-podcast-enabled").checked = podcast.enabled ?? false;
    document.getElementById("schedule-podcast-time").value = podcast.time || "08:00";
    if (note) {
      const last = brief.last_run ? `Daily brief last run: ${brief.last_run}` : "";
      note.textContent = last;
    }
  }
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    error.style.display = "none";
    const settings = {
      timezone: document.getElementById("schedule-timezone").value.trim() || null,
      tasks: {
        daily_brief: {
          enabled: document.getElementById("schedule-brief-enabled").checked,
          time: document.getElementById("schedule-brief-time").value || "07:30",
        },
        podcast: {
          enabled: document.getElementById("schedule-podcast-enabled").checked,
          time: document.getElementById("schedule-podcast-time").value || "08:00",
        },
      },
    };
    try {
      await apiFetch("/admin/api/schedules/settings", {
        method: "PUT",
        body: JSON.stringify({ settings }),
      });
      showToast("Schedule saved");
      load();
    } catch (err) {
      error.textContent = err.message || "Save failed";
      error.style.display = "block";
    }
  });
  const runBrief = document.getElementById("schedule-brief-run");
  if (runBrief) {
    runBrief.addEventListener("click", async () => {
      try {
        await apiFetch("/admin/api/daily_brief/build", { method: "POST", body: JSON.stringify({}) });
        showToast("Daily brief enqueued");
      } catch (err) {
        showToast(err.message || String(err));
      }
    });
  }
  load().catch((err) => {
    error.textContent = err.message || "Load failed";
    error.style.display = "block";
  });
}
function wireContentSearch() {
  const form = document.getElementById("content-search-form");
  const table = document.getElementById("content-table");
  if (!form || !table) {
    return;
  }
  const tbody = table.querySelector("tbody");
  const pager = document.getElementById("content-pager");
  const error = document.getElementById("content-error");
  const tagList = document.getElementById("content-tag-list");
  const selectedTagsEl = document.getElementById("content-selected-tags");
  const tagsField = document.getElementById("content-tags");
  const missingField = document.getElementById("content-missing");
  const contentStateField = document.getElementById("content-state");
  const contentErrorField = document.getElementById("content-content-error");
  const contentErrorKindField = document.getElementById("content-error-kind");
  const summaryErrorField = document.getElementById("content-summary-error");
  const needsField = document.getElementById("content-needs");
  const watchlistEnabled = form.dataset.watchlistEnabled === "true";
  const watchlistOnlyField = document.getElementById("content-watchlist-only");
  let pageSize = parseInt(document.getElementById("content-page-size").value, 10);
  let currentPage = 1;
  let contentSort = { index: null, dir: "asc" };
  function getSortValue(cell, index) {
    if (!cell) return "";
    const tsEl = cell.querySelector("[data-ts]");
    if (tsEl && tsEl.dataset.ts) {
      const parsed = Date.parse(tsEl.dataset.ts);
      return Number.isNaN(parsed) ? 0 : parsed;
    }
    const text = cell.textContent?.trim() || "";
    if (index === 0) {
      const idVal = parseInt(text.replace(/[^\d]/g, ""), 10);
      return Number.isNaN(idVal) ? text.toLowerCase() : idVal;
    }
    return text.toLowerCase();
  }
  function applyContentSort() {
    if (!tbody || contentSort.index === null) return;
    const rows = Array.from(tbody.querySelectorAll("tr"));
    rows.sort((a, b) => {
      const aCell = a.children[contentSort.index];
      const bCell = b.children[contentSort.index];
      const av = getSortValue(aCell, contentSort.index);
      const bv = getSortValue(bCell, contentSort.index);
      if (av < bv) return contentSort.dir === "asc" ? -1 : 1;
      if (av > bv) return contentSort.dir === "asc" ? 1 : -1;
      return 0;
    });
    rows.forEach((row) => tbody.appendChild(row));
  }
  function setupContentSort() {
    const headers = table.querySelectorAll("thead th");
    headers.forEach((th, index) => {
      th.classList.add("sortable");
      th.addEventListener("click", () => {
        if (contentSort.index === index) {
          contentSort.dir = contentSort.dir === "asc" ? "desc" : "asc";
        } else {
          contentSort.index = index;
          contentSort.dir = "asc";
        }
        headers.forEach((h) => h.classList.remove("sorted-asc", "sorted-desc"));
        th.classList.add(contentSort.dir === "asc" ? "sorted-asc" : "sorted-desc");
        applyContentSort();
      });
    });
  }
  let selectedTags = new Set();
  function setError(message) {
    if (!error) {
      return;
    }
    if (message) {
      error.textContent = message;
      error.style.display = "block";
    } else {
      error.textContent = "";
      error.style.display = "none";
    }
  }
  function syncTagField() {
    tagsField.value = Array.from(selectedTags).join(", ");
  }
  function renderSelectedTags() {
    if (!selectedTagsEl) {
      return;
    }
    selectedTagsEl.innerHTML = "";
    Array.from(selectedTags).forEach((tag) => {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = "tag-chip";
      chip.textContent = tag;
      chip.addEventListener("click", () => {
        selectedTags.delete(tag);
        syncTagField();
        renderSelectedTags();
        load(1).catch((err) => setError(err.message || String(err)));
      });
      selectedTagsEl.appendChild(chip);
    });
  }
  function renderTagList(tags) {
    if (!tagList) {
      return;
    }
    tagList.innerHTML = "";
    tags.forEach((item) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "tag-item";
      btn.textContent = `${item.tag} (${item.count})`;
      btn.addEventListener("click", () => {
        if (selectedTags.has(item.tag)) {
          selectedTags.delete(item.tag);
        } else {
          selectedTags.add(item.tag);
        }
        syncTagField();
        renderSelectedTags();
        load(1).catch((err) => setError(err.message || String(err)));
      });
      tagList.appendChild(btn);
    });
  }
  function parseTagsInput(value) {
    return value
      .split(",")
      .map((tag) => tag.trim())
      .filter(Boolean);
  }
  function buildPageList(current, total) {
    const pages = new Set([1, total, current - 2, current - 1, current, current + 1, current + 2]);
    return Array.from(pages)
      .filter((p) => p >= 1 && p <= total)
      .sort((a, b) => a - b);
  }
  function renderPager(total, page, size) {
    if (!pager) {
      return;
    }
    pager.innerHTML = "";
    const totalPages = Math.max(1, Math.ceil(total / size));
    const controls = document.createElement("div");
    controls.className = "pager-controls";
    const prev = document.createElement("button");
    prev.type = "button";
    prev.className = "btn secondary";
    prev.textContent = "Prev";
    prev.disabled = page <= 1;
    prev.addEventListener("click", () => load(page - 1));
    controls.appendChild(prev);
    const pages = buildPageList(page, totalPages);
    let last = 0;
    pages.forEach((p) => {
      if (p - last > 1) {
        const ellipsis = document.createElement("span");
        ellipsis.className = "pager-ellipsis";
        ellipsis.textContent = "…";
        controls.appendChild(ellipsis);
      }
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "pager-page" + (p === page ? " active" : "");
      btn.textContent = String(p);
      btn.addEventListener("click", () => load(p));
      controls.appendChild(btn);
      last = p;
    });
    const next = document.createElement("button");
    next.type = "button";
    next.className = "btn secondary";
    next.textContent = "Next";
    next.disabled = page >= totalPages;
    next.addEventListener("click", () => load(page + 1));
    controls.appendChild(next);
    const info = document.createElement("div");
    info.className = "pager-info";
    info.textContent = `Page ${page} of ${totalPages}`;
    pager.appendChild(controls);
    pager.appendChild(info);
  }
  function applyQueryParams() {
    const params = new URLSearchParams(window.location.search);
    const setValue = (id, key) => {
      const el = document.getElementById(id);
      if (el && params.has(key)) {
        el.value = params.get(key) || "";
      }
    };
    setValue("content-query", "query");
    setValue("content-type", "type");
    setValue("content-source", "source_id");
    setValue("content-has-summary", "has_summary");
    setValue("content-has-context", "has_context");
    setValue("content-tags", "tags");
    setValue("content-after", "after");
    setValue("content-before", "before");
    setValue("content-missing", "missing");
    setValue("content-state", "content_state");
    setValue("content-needs", "needs");
    setValue("content-error-kind", "content_error_kind");
    if (contentErrorField && params.get("content_error") === "1") {
      contentErrorField.checked = true;
    }
    if (summaryErrorField && params.get("summary_error") === "1") {
      summaryErrorField.checked = true;
    }
    if (watchlistOnlyField && params.get("watchlist_hit") === "true") {
      watchlistOnlyField.checked = true;
    }
    const pageSizeField = document.getElementById("content-page-size");
    if (pageSizeField && params.has("page_size")) {
      pageSize = parseInt(params.get("page_size"), 10) || pageSize;
      pageSizeField.value = String(pageSize);
    }
    if (params.has("page")) {
      currentPage = parseInt(params.get("page"), 10) || currentPage;
    }
  }
  async function load(page) {
    currentPage = page;
    setError("");
    const params = new URLSearchParams();
    const query = document.getElementById("content-query").value.trim();
    const typeField = document.getElementById("content-type");
    const type = typeField ? typeField.value : "article";
    const source = document.getElementById("content-source").value;
    const hasSummary = document.getElementById("content-has-summary").value;
    const hasContext = document.getElementById("content-has-context").value;
    const tags = document.getElementById("content-tags").value.trim();
        const after = document.getElementById("content-after").value;
    const before = document.getElementById("content-before").value;
    const watchlistOnly = watchlistEnabled && watchlistOnlyField && watchlistOnlyField.checked;
    const missing = missingField ? missingField.value : "";
    const contentState = contentStateField ? contentStateField.value : "";
    const needs = needsField ? needsField.value : "";
    const contentError = contentErrorField && contentErrorField.checked;
    const contentErrorKind = contentErrorKindField ? contentErrorKindField.value : "";
    const summaryError = summaryErrorField && summaryErrorField.checked;
    if (query) params.set("query", query);
    if (type) params.set("type", type);
    if (source) params.set("source_id", source);
    if (hasSummary) params.set("has_summary", hasSummary);
    if (hasContext) params.set("has_context", hasContext);
    if (tags) params.set("tags", tags);
    if (after) params.set("after", after);
    if (before) params.set("before", before);
    if (missing) params.set("missing", missing);
    if (contentState) params.set("content_state", contentState);
    if (needs) params.set("needs", needs);
    if (contentError) params.set("content_error", "1");
    if (contentErrorKind) params.set("content_error_kind", contentErrorKind);
    if (summaryError) params.set("summary_error", "1");
    if (watchlistOnly) params.set("watchlist_hit", "true");
    params.set("page", String(page));
    params.set("page_size", String(pageSize));
    const data = await apiFetch(`/admin/api/content/search?${params.toString()}`);
    tbody.innerHTML = "";
    if (!data.items || !data.items.length) {
      const row = document.createElement("tr");
      row.innerHTML = `<td colspan="3" class="muted">No products found. Run “Backfill from CVEs” or enqueue CVE enrichment.</td>`;
      tbody.appendChild(row);
      renderPager(pager, data.total, data.page, data.page_size, load);
      return;
    }
    data.items.forEach((item) => {
      const row = document.createElement("tr");
      const date = item.published_at || item.ingested_at || "";
      const title = item.title || item.summary || "";
      const link = `/ui/content/articles/${item.id}`;
      const idValue = item.id;
      const watchlistCell = watchlistEnabled
        ? `<td>${
            item.watchlist_hit || item.in_scope
              ? '<span class="status-pill status-ok">hit</span>'
              : '<span class="status-pill status-muted">-</span>'
          }</td>`
        : "";
      let actions = "";
      if (item.type === "article") {
        const hasContent = item.has_content;
        const hasSummary = item.has_summary;
        const hasUrl = Boolean(item.url);
        actions = `
          <div class="table-actions">
            <button class="btn small action-pipeline" data-article-id="${item.id}">Run Pipeline</button>
            <div class="action-menu">
              <button class="action-menu-button" type="button" aria-label="More actions">⋮</button>
              <div class="action-menu-list">
                <button type="button" class="action-fetch" data-article-id="${item.id}" ${
                  hasUrl ? "" : "disabled"
                }>Fetch content</button>
                <button type="button" class="action-summarize" data-article-id="${item.id}" ${
                  hasContent ? "" : "disabled"
                }>Generate summary</button>
                <button type="button" class="action-context-pack" data-article-id="${item.id}">Generate context pack</button>
                <button type="button" class="action-publish" data-article-id="${item.id}">Publish markdown</button>
                <button type="button" class="action-derive-event" data-article-id="${item.id}">Derive event</button>
                <button type="button" class="action-suppress" data-article-id="${item.id}" data-suppressed="${item.suppressed ? "1" : "0"}">${item.suppressed ? "Unsuppress" : "Suppress"}</button>
                <button type="button" class="action-delete" data-article-id="${item.id}">Delete article</button>
              </div>
            </div>
          </div>
        `;
      }
      const contentLen = Number(item.content_len || 0);
      const contentPill = item.has_content
        ? null
        : contentLen > 0
          ? '<span class="status-pill status-warn">Partial content</span>'
          : '<span class="status-pill status-warn">No content</span>';
      const statusPills = [
        item.suppressed ? '<span class="status-pill status-muted">Suppressed</span>' : null,
        contentPill,
        item.has_summary ? null : '<span class="status-pill status-warn">No summary</span>',
        item.content_error ? '<span class="status-pill status-error">Content error</span>' : null,
        item.summary_error ? '<span class="status-pill status-error">Summary error</span>' : null,
      ].filter(Boolean).join(" ");
      row.innerHTML = `
        <td>${link ? renderShortId(idValue, link) : renderShortId(idValue)}</td>
        <td><span data-ts="${esc(date)}"></span></td>
        <td class="line-clamp-2 ${item.suppressed ? "is-suppressed" : ""}" title="${esc(title)}">${esc(title)}</td>
        <td class="content-status">${statusPills || '<span class="status-pill status-muted">OK</span>'}</td>
        <td class="truncate" title="${esc(item.source_name || "")}">${esc(item.source_name || "")}</td>
        ${watchlistCell}
        <td class="actions">${actions}</td>
      `;
      tbody.appendChild(row);
    });
    applyTimestampFormatting(tbody);
    applyContentSort();
    renderPager(data.total, data.page, data.page_size);
  }
  document.getElementById("content-page-size").addEventListener("change", () => {
    pageSize = parseInt(document.getElementById("content-page-size").value, 10);
    load(1).catch((err) => setError(err.message || String(err)));
  });
  tbody.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const cveId = target.dataset.cveId;
    if (!cveId) {
      return;
    }
    try {
      if (target.classList.contains("action-cve-desc")) {
        const result = await apiFetch(`/admin/api/cves/${cveId}/refresh`, { method: "POST" });
        toast(`Queued ${renderShortId(result.job_id || cveId)}`, "success");
        load(currentPage);
      }
      if (target.classList.contains("action-cve-products")) {
        const result = await apiFetch(`/admin/api/cves/${cveId}/enrich_products`, { method: "POST" });
        toast(result.status === "already_queued" ? "Already queued" : `Queued ${renderShortId(result.job_id || cveId)}`, "success");
        load(currentPage);
      }
    } catch (err) {
      toast(err.message || String(err), "error");
    }
  });
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    load(1).catch((err) => setError(err.message || String(err)));
  });
  tbody.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    try {
      if (target.classList.contains("action-fetch")) {
        const articleId = target.dataset.articleId;
        const result = await apiFetch(`/admin/api/articles/${articleId}/fetch`, { method: "POST" });
        showToast(`${result.status}: ${result.job_id || ""}`.trim());
      }
      if (target.classList.contains("action-summarize")) {
        const articleId = target.dataset.articleId;
        const result = await apiFetch(`/admin/api/articles/${articleId}/summarize`, { method: "POST" });
        showToast(`${result.status}: ${result.job_id || ""}`.trim());
      }
      if (target.classList.contains("action-context-pack")) {
        const articleId = target.dataset.articleId;
        const result = await apiFetch(`/admin/api/articles/${articleId}/context_pack`, { method: "POST" });
        showToast(`${result.status}: ${result.job_id || ""}`.trim());
      }
      if (target.classList.contains("action-publish")) {
        const articleId = target.dataset.articleId;
        const result = await apiFetch(`/admin/api/articles/${articleId}/publish`, { method: "POST" });
        showToast(`${result.status}: ${result.job_id || ""}`.trim());
      }
      if (target.classList.contains("action-pipeline")) {
        const articleId = target.dataset.articleId;
        const result = await apiFetch(`/admin/api/articles/${articleId}/pipeline`, { method: "POST" });
        const ids = result.job_ids ? result.job_ids.join(",") : result.job_id || "";
        showToast(`${result.status}: ${ids}`.trim());
      }
      if (target.classList.contains("action-refresh-cve")) {
        const cveId = target.dataset.cveId;
        const result = await apiFetch(`/admin/api/cves/${cveId}/refresh`, { method: "POST" });
        showToast(`${result.status}: ${result.job_id || ""}`.trim());
      }
      if (target.classList.contains("action-suppress")) {
        const articleId = target.dataset.articleId;
        const currentlySuppressed = target.dataset.suppressed === "1";
        const result = await apiFetch(`/admin/api/articles/${articleId}/suppress`, {
          method: "POST",
          body: JSON.stringify({ suppressed: !currentlySuppressed }),
        });
        showToast(result.suppressed ? "Article suppressed" : "Article unsuppressed");
        load(currentPage).catch((err) => setError(err.message || String(err)));
      }
      if (target.classList.contains("action-delete")) {
        const articleId = target.dataset.articleId;
        if (!confirm(`Delete article ${articleId}?`)) {
          return;
        }
        await apiFetch(`/admin/api/articles/${articleId}`, { method: "DELETE" });
        showToast("Article deleted");
        const scrollY = window.scrollY;
        await load(page).catch((err) => setError(err.message || String(err)));
        window.scrollTo(0, scrollY);
      }
      if (target.classList.contains("action-derive-event")) {
        const articleId = target.dataset.articleId;
        const result = await apiFetch(`/admin/api/events/derive`, {
          method: "POST",
          body: JSON.stringify({ article_id: parseInt(articleId, 10) }),
        });
        showToast(`${result.status}: ${result.job_id || ""}`.trim());
      }
    } catch (err) {
      showToast(err.message || String(err));
    }
  });
  applyQueryParams();
  if (tagList) {
    apiFetch("/admin/api/content/tags")
      .then((data) => renderTagList(data.tags || []))
      .catch((err) => setError(err.message || String(err)));
  }
  if (tagsField.value.trim()) {
    selectedTags = new Set(parseTagsInput(tagsField.value));
    renderSelectedTags();
  }
  tagsField.addEventListener("change", () => {
    selectedTags = new Set(parseTagsInput(tagsField.value));
    renderSelectedTags();
    load(1).catch((err) => setError(err.message || String(err)));
  });
  load(currentPage).catch((err) => setError(err.message || String(err)));
  setupContentSort();
}
function wireContentArticle() {
  const container = document.getElementById("article-detail");
  if (!container) {
    return;
  }
  const articleId = container.dataset.articleId;
  apiFetch(`/admin/api/content/articles/${articleId}`)
    .then((item) => {
      const summaryRaw = item.summary_llm || "";
      const legacySummary = item.summary || "";
      const content = item.content_text || "";
      const htmlExcerpt = item.content_html_excerpt || "";
      const error = item.content_error || "";
      const contextRaw = item.context_llm || "";
      const contextModel = item.context_model || "";
      const contextGeneratedAt = item.context_generated_at || "";
      const contextError = item.context_error || "";
      const threatActors = item.threat_actors || [];
      let summaryBlock = "";
      let rawJsonBlock = "";
      let contextBlock = "";
      if (summaryRaw) {
        try {
          const parsed = JSON.parse(summaryRaw);
          if (parsed && typeof parsed === "object") {
            if (parsed.summary) {
              summaryBlock += `<p>${esc(parsed.summary)}</p>`;
            }
            const bullets = parsed.bullets || parsed.key_points || parsed.tldr;
            if (Array.isArray(bullets) && bullets.length) {
              summaryBlock += `<ul>${bullets
                .map((bullet) => `<li>${esc(bullet)}</li>`)
                .join("")}</ul>`;
            }
            rawJsonBlock = `
              <details>
                <summary>Raw JSON</summary>
                <pre class="mono wrap-pre">${esc(JSON.stringify(parsed, null, 2))}</pre>
              </details>
            `;
          }
        } catch (err) {
          summaryBlock = `<pre class="mono wrap-pre">${esc(summaryRaw)}</pre>`;
        }
      }
      if (!summaryBlock) {
        const fallback = legacySummary || "No summary available.";
        summaryBlock = `<pre class="mono wrap-pre">${esc(fallback)}</pre>`;
      }
      if (contextRaw) {
        try {
          const parsed = JSON.parse(contextRaw);
          if (parsed && typeof parsed === "object") {
            if (Array.isArray(parsed.facts) && parsed.facts.length) {
              contextBlock += `<h4>Facts</h4><ul>${parsed.facts
                .map((fact) => `<li>${esc(fact)}</li>`)
                .join("")}</ul>`;
            }
            if (parsed.entities && typeof parsed.entities === "object") {
              const ent = parsed.entities;
              const parts = [];
              const pushList = (label, values) => {
                if (Array.isArray(values) && values.length) {
                  parts.push(`<div><strong>${esc(label)}:</strong> ${esc(values.join(", "))}</div>`);
                }
              };
              pushList("Orgs", ent.orgs);
              pushList("People", ent.people);
              pushList("Products", ent.products);
              pushList("Vendors", ent.vendors);
              pushList("Threat Actors", ent.threat_actors);
              pushList("Countries", ent.countries);
              if (parts.length) {
                contextBlock += `<h4>Entities</h4>${parts.join("")}`;
              }
            }
            if (Array.isArray(parsed.cves) && parsed.cves.length) {
              contextBlock += `<h4>CVEs</h4><p>${esc(parsed.cves.join(", "))}</p>`;
            }
            if (Array.isArray(parsed.iocs) && parsed.iocs.length) {
              contextBlock += `<h4>IOCs</h4><p>${esc(parsed.iocs.join(", "))}</p>`;
            }
            contextBlock += `
              <details>
                <summary>Raw Context JSON</summary>
                <pre class="mono wrap-pre">${esc(JSON.stringify(parsed, null, 2))}</pre>
              </details>
            `;
          }
        } catch (err) {
          contextBlock = `<pre class="mono wrap-pre">${esc(contextRaw)}</pre>`;
        }
      }
      container.innerHTML = `
        <div class="kv">
          <div class="${item.suppressed ? "is-suppressed" : ""}"><strong>${esc(item.title || "")}</strong></div>
          <div>Source: ${esc(item.source_id || "")}</div>
          <div>Published: ${esc(formatTimestamp(item.published_at))}</div>
          <div>Ingested: ${esc(formatTimestamp(item.ingested_at))}</div>
          <div><a href="${esc(item.original_url || "")}" target="_blank" rel="noopener">Open URL</a></div>
          <div>Suppressed: ${item.suppressed ? "yes" : "no"}</div>
        <div class="actions">
          <button class="btn small" id="article-suppress-toggle">${item.suppressed ? "Unsuppress" : "Suppress"}</button>
          <button class="btn small secondary" id="article-context-pack">Generate context pack</button>
          <button class="btn small secondary" id="article-publish-markdown">Publish markdown</button>
        </div>
        </div>
        <h3>Products & Vendors</h3>
        ${
          Array.isArray(item.products) && item.products.length
            ? `<ul>${item.products
                .map((prod) => {
                  const label = prod.display_name || `${prod.vendor || ""} ${prod.product || ""}`.trim();
                  const link = prod.product_key ? `/ui/products/${prod.product_key}` : "";
                  return `<li>${link ? `<a href="${link}">${esc(label)}</a>` : esc(label)}</li>`;
                })
                .join("")}</ul>`
            : `<p class="muted">No products linked.</p>`
        }
        <h3>Threat Actors</h3>
        ${
          threatActors.length
            ? `<ul>${threatActors
                .map((actor) => {
                  const name = actor.display_name || actor.actor_key || "";
                  const kind = actor.actor_type || "";
                  const country = actor.country || "";
                  const confidence =
                    actor.confidence === null || actor.confidence === undefined ? "" : ` (${actor.confidence})`;
                  const meta = [kind, country].filter(Boolean).join(" · ");
                  return `<li>${esc(name)}${meta ? ` <span class="muted">(${esc(meta)}${esc(confidence)})</span>` : ""}</li>`;
                })
                .join("")}</ul>`
            : `<p class="muted">None found.</p>`
        }
        <h3>Summary</h3>
        ${summaryBlock}
        ${rawJsonBlock}
        <h3>Context Pack</h3>
        ${
          contextRaw
            ? `<p class="muted">Model: ${esc(contextModel || "unknown")} · Generated: ${esc(
                formatTimestamp(contextGeneratedAt)
              )}${contextError ? ` · Error: ${esc(contextError)}` : ""}</p>${contextBlock}`
            : `<p class="muted">No context pack available.</p>`
        }
        <h3>Content</h3>
        <pre class="mono wrap-pre">${esc(content || "No extracted content available.")}</pre>
        ${htmlExcerpt ? `<h3>HTML Excerpt</h3><pre class="mono wrap-pre">${esc(htmlExcerpt)}</pre>` : ""}
        ${error ? `<p class="error">Content error: ${esc(error)}</p>` : ""}
        <h3>Edit Content</h3>
        <p class="muted">Paste full text here to override the extracted content.</p>
        <textarea id="article-content-edit" class="mono wrap-pre" rows="12">${esc(content || "")}</textarea>
        <div class="form-actions">
          <button class="btn" id="article-content-save" type="button">Save Content</button>
        </div>
      `;
      const saveBtn = document.getElementById("article-content-save");
      const publishBtn = document.getElementById("article-publish-markdown");
      const contextBtn = document.getElementById("article-context-pack");
      if (publishBtn) {
        publishBtn.addEventListener("click", async () => {
          try {
            const resp = await apiFetch(`/admin/api/articles/${articleId}/publish`, { method: "POST" });
            toast(`Publish queued (${resp.job_id || "ok"})`, "success");
          } catch (err) {
            toast(err.message || String(err), "error");
          }
        });
      }
      if (contextBtn) {
        contextBtn.addEventListener("click", async () => {
          try {
            const resp = await apiFetch(`/admin/api/articles/${articleId}/context_pack`, { method: "POST" });
            toast(`Context pack queued (${resp.job_id || "ok"})`, "success");
          } catch (err) {
            toast(err.message || String(err), "error");
          }
        });
      }
      if (saveBtn) {
        saveBtn.addEventListener("click", async () => {
          const textarea = document.getElementById("article-content-edit");
          const value = textarea ? textarea.value : "";
          try {
            const resp = await apiFetch(`/admin/api/articles/${articleId}/content`, {
              method: "PATCH",
              body: JSON.stringify({ content_text: value }),
            });
            toast(`Saved content (${resp.content_len || 0} chars)`, "success");
          } catch (err) {
            toast(err.message || String(err), "error");
          }
        });
      }
    })
    .catch((err) => {
      container.textContent = err.message || String(err);
    });
}

function wireThreatsList() {
  const table = document.getElementById("threats-table");
  const tbody = document.querySelector("#threats-table tbody");
  const pager = document.getElementById("threats-pager");
  const form = document.getElementById("threats-search-form");
  const error = document.getElementById("threats-error");
  const backfillArticles = document.getElementById("threats-backfill-articles");
  const backfillCves = document.getElementById("threats-backfill-cves");
  const backfillLimit = document.getElementById("threats-backfill-limit");
  if (!table || !tbody || !form) {
    return;
  }
  let page = 1;
  let pageSize = 50;
  function renderPager(total, pageSizeValue) {
    if (!pager) {
      return;
    }
    pager.innerHTML = "";
    if (!total) {
      return;
    }
    const totalPages = Math.ceil(total / pageSizeValue);
    if (totalPages <= 1) {
      return;
    }
    const prev = document.createElement("button");
    prev.className = "btn small";
    prev.textContent = "Prev";
    prev.disabled = page <= 1;
    prev.addEventListener("click", () => {
      page = Math.max(1, page - 1);
      load(page);
    });
    pager.appendChild(prev);
    const info = document.createElement("span");
    info.className = "pager-info";
    info.textContent = `Page ${page} of ${totalPages}`;
    pager.appendChild(info);
    const next = document.createElement("button");
    next.className = "btn small";
    next.textContent = "Next";
    next.disabled = page >= totalPages;
    next.addEventListener("click", () => {
      page = Math.min(totalPages, page + 1);
      load(page);
    });
    pager.appendChild(next);
  }
  async function load(nextPage) {
    page = nextPage || 1;
    const query = document.getElementById("threats-query").value.trim();
    pageSize = parseInt(document.getElementById("threats-page-size").value, 10) || 50;
    const params = new URLSearchParams();
    if (query) {
      params.set("query", query);
    }
    params.set("page", String(page));
    params.set("page_size", String(pageSize));
    const data = await apiFetch(`/admin/api/threats?${params.toString()}`);
    tbody.innerHTML = "";
    const items = data.items || [];
    if (!items.length) {
      const row = document.createElement("tr");
      row.innerHTML = `<td colspan="5" class="muted">No threat actors found.</td>`;
      tbody.appendChild(row);
      renderPager(data.total || 0, pageSize);
      return;
    }
    items.forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td><a href="/ui/threats/${encodeURIComponent(item.actor_key)}">${esc(item.display_name || item.actor_key || "")}</a></td>
        <td>${esc(item.actor_type || "unknown")}</td>
        <td>${item.alias_count ?? 0}</td>
        <td>${item.article_count ?? 0}</td>
        <td>${item.cve_count ?? 0}</td>
      `;
      tbody.appendChild(row);
    });
    renderPager(data.total || 0, pageSize);
  }
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    if (error) {
      error.style.display = "none";
    }
    load(1).catch((err) => {
      if (error) {
        error.textContent = err.message || String(err);
        error.style.display = "block";
      }
    });
  });
  load(page).catch((err) => {
    if (error) {
      error.textContent = err.message || String(err);
      error.style.display = "block";
    }
  });
  const limitValue = () => parseInt(backfillLimit?.value || "200", 10) || 200;
  if (backfillArticles) {
    backfillArticles.addEventListener("click", async () => {
      try {
        const payload = await apiFetch("/admin/api/threats/backfill/articles", {
          method: "POST",
          body: JSON.stringify({ limit: limitValue() }),
        });
        toast(`Queued article threat backfill (${payload.job_id})`, "success");
      } catch (err) {
        toast(err.message || String(err), "error");
      }
    });
  }
  if (backfillCves) {
    backfillCves.addEventListener("click", async () => {
      try {
        const payload = await apiFetch("/admin/api/threats/backfill/cves", {
          method: "POST",
          body: JSON.stringify({ limit: limitValue() }),
        });
        toast(`Queued CVE threat backfill (${payload.job_id})`, "success");
      } catch (err) {
        toast(err.message || String(err), "error");
      }
    });
  }
}

function wireBriefsList() {
  const table = document.getElementById("briefs-table");
  const tbody = document.querySelector("#briefs-table tbody");
  const pager = document.getElementById("briefs-pager");
  const form = document.getElementById("briefs-search-form");
  const cancelRunning = document.getElementById("briefs-cancel-running");
  const cancelRestart = document.getElementById("briefs-cancel-restart");
  const error = document.getElementById("briefs-error");
  if (!table || !tbody || !form) {
    return;
  }
  let page = 1;
  let pageSize = 50;
  function renderPager(total, pageSizeValue) {
    if (!pager) {
      return;
    }
    pager.innerHTML = "";
    if (!total) {
      return;
    }
    const totalPages = Math.ceil(total / pageSizeValue);
    if (totalPages <= 1) {
      return;
    }
    const prev = document.createElement("button");
    prev.className = "btn small";
    prev.textContent = "Prev";
    prev.disabled = page <= 1;
    prev.addEventListener("click", () => {
      page = Math.max(1, page - 1);
      load(page);
    });
    pager.appendChild(prev);
    const info = document.createElement("span");
    info.className = "pager-info";
    info.textContent = `Page ${page} of ${totalPages}`;
    pager.appendChild(info);
    const next = document.createElement("button");
    next.className = "btn small";
    next.textContent = "Next";
    next.disabled = page >= totalPages;
    next.addEventListener("click", () => {
      page = Math.min(totalPages, page + 1);
      load(page);
    });
    pager.appendChild(next);
  }
  async function load(nextPage) {
    page = nextPage || 1;
    pageSize = parseInt(document.getElementById("briefs-page-size").value, 10) || 50;
    const params = new URLSearchParams();
    params.set("page", String(page));
    params.set("page_size", String(pageSize));
    const data = await apiFetch(`/admin/api/briefs?${params.toString()}`);
    tbody.innerHTML = "";
    const items = data.items || [];
    if (!items.length) {
      const row = document.createElement("tr");
      row.innerHTML = `<td colspan="9" class="muted">No daily briefs found.</td>`;
      tbody.appendChild(row);
      renderPager(data.total || 0, pageSize);
      return;
    }
    items.forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td><a href="/ui/briefs/${encodeURIComponent(item.brief_day)}">${esc(item.brief_day)}</a></td>
        <td>${esc(formatTimestamp(item.generated_at || item.updated_at))}</td>
        <td>${item.article_count ?? "-"}</td>
        <td>${item.topic_count ?? 0}</td>
        <td>${item.family_count ?? 0}</td>
        <td>${item.url_count ?? 0}</td>
        <td>${item.tldr_count ?? 0}</td>
        <td>${esc(item.profile_id || "")}</td>
        <td>${esc(formatTimestamp(item.updated_at))}</td>
      `;
      tbody.appendChild(row);
    });
    renderPager(data.total || 0, pageSize);
  }
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    if (error) {
      error.style.display = "none";
    }
    load(1).catch((err) => {
      if (error) {
        error.textContent = err.message || String(err);
        error.style.display = "block";
      }
    });
  });
  load(page).catch((err) => {
    if (error) {
      error.textContent = err.message || String(err);
      error.style.display = "block";
    }
  });
  if (cancelRunning) {
    cancelRunning.addEventListener("click", async () => {
      try {
        const data = await apiFetch("/admin/api/briefs/cancel-running", { method: "POST" });
        showToast(`Canceled ${data.canceled || 0} running brief job(s)`);
      } catch (err) {
        showToast(err.message || String(err), "error");
      }
    });
  }
  if (cancelRestart) {
    cancelRestart.addEventListener("click", async () => {
      try {
        const data = await apiFetch("/admin/api/briefs/cancel-running-restart", { method: "POST" });
        const canceled = data.canceled || 0;
        const cmd = data.restart_command || "docker compose restart worker_llm";
        showToast(`Canceled ${canceled} brief job(s). Run: ${cmd}`, "success");
      } catch (err) {
        showToast(err.message || String(err), "error");
      }
    });
  }
}

function wireBriefDetail() {
  const container = document.querySelector("[data-brief-day]");
  if (!container) {
    return;
  }
  const day = container.getAttribute("data-brief-day");
  const error = document.getElementById("brief-detail-error");
  const status = document.getElementById("brief-status");
  const meta = document.getElementById("brief-meta");
  const tldr = document.getElementById("brief-tldr");
  const technicalSynthesis = document.getElementById("brief-technical-synthesis");
  const actions = document.getElementById("brief-actions");
  const dailyCves = document.getElementById("brief-daily-cves");
  const podcastScript = document.getElementById("brief-podcast-script");
  const nistBreakdown = document.getElementById("brief-nist-breakdown");
  const citations = document.getElementById("brief-citations");
  const lowValue = document.getElementById("brief-low-value");
  const metaBlock = document.getElementById("brief-meta-json");
  const clusterRaw = document.getElementById("brief-cluster-raw");
  const summarizeRaw = document.getElementById("brief-summarize-raw");
  const nistRaw = document.getElementById("brief-nist-raw");
  const overallRaw = document.getElementById("brief-overall-raw");
  if (!day) {
    return;
  }

  const renderStatus = (payload) => {
    if (!status) {
      return;
    }
    if (!payload || !payload.pending || !payload.job) {
      status.innerHTML = `<span class="muted">No pending brief job.</span>`;
      return;
    }
    const job = payload.job || {};
    const started = formatAbsolute(job.started_at || "");
    const requested = formatAbsolute(job.requested_at || "");
    const when = started || requested || "";
    status.innerHTML = `
      <span class="badge warn">Pending</span>
      <span class="muted">Status:</span> ${esc(job.status || "unknown")}
      ${when ? `<span class="muted">Since:</span> ${esc(when)}` : ""}
      ${job.id ? `<span class="muted">Job:</span> <a href="/ui/jobs/${esc(job.id)}">${esc(shortId(job.id))}</a>` : ""}
    `;
  };

  const renderList = (items, className = "muted") => {
    if (!items || !items.length) {
      return `<div class="${className}">None</div>`;
    }
    return `<ul>${items.map((item) => `<li>${esc(item)}</li>`).join("")}</ul>`;
  };

  const linkifyCitations = (text) => {
    if (!text) return "";
    return esc(text).replace(/\((\d+)\)/g, '<a href="#cite-$1">($1)</a>');
  };
  apiFetch(`/admin/api/briefs/${encodeURIComponent(day)}/status`)
    .then((payload) => renderStatus(payload))
    .catch(() => {
      if (status) {
        status.innerHTML = `<span class="muted">Status unavailable.</span>`;
      }
    });

  apiFetch(`/admin/api/briefs/${encodeURIComponent(day)}`)
    .then((data) => {
      if (data && data.pending_job) {
        renderStatus({ pending: true, job: data.pending_job });
      }
      if (meta) {
        const generatedAt = formatTimestamp(data.meta?.generated_at || data.updated_at);
        meta.innerHTML = `
          <div class="meta-row">
            <span class="meta-label">Generated:</span> ${esc(generatedAt)}
            <span class="meta-label">Articles:</span> ${data.meta?.article_count ?? "-"}
            <span class="meta-label">Topics:</span> ${data.meta?.topic_count ?? "-"}
            <span class="meta-label">Families:</span> ${data.meta?.family_count ?? "-"}
            <span class="meta-label">Citations:</span> ${data.meta?.citation_count ?? "-"}
          </div>
        `;
      }
      if (tldr) {
        tldr.innerHTML = "";
        const items = Array.isArray(data.tldr) ? data.tldr : [];
        if (!items.length) {
          tldr.innerHTML = `<li class="muted">No TLDR items recorded.</li>`;
        } else {
          items.forEach((item) => {
            const li = document.createElement("li");
            if (typeof item === "string") {
              li.innerHTML = linkifyCitations(item);
            } else {
              li.innerHTML = linkifyCitations(item.text || "");
            }
            tldr.appendChild(li);
          });
        }
      }
      if (technicalSynthesis) {
        const text =
          typeof data.technical_synthesis === "string"
            ? data.technical_synthesis
            : data.technical_synthesis?.text || "";
        if (!text) {
          technicalSynthesis.innerHTML = `<p class="muted">No technical synthesis recorded.</p>`;
        } else {
          technicalSynthesis.innerHTML = text
            .split("\n\n")
            .map((p) => (p ? `<p>${linkifyCitations(p)}</p>` : ""))
            .join("");
        }
      }
      if (actions) {
        actions.innerHTML = "";
        const items = Array.isArray(data.actions) ? data.actions : [];
        if (!items.length) {
          actions.innerHTML = `<li class="muted">No actions recorded.</li>`;
        } else {
          items.forEach((item) => {
            if (typeof item === "string") {
              const li = document.createElement("li");
              li.innerHTML = linkifyCitations(item);
              actions.appendChild(li);
              return;
            }
            const priority = item.priority ? `<strong>${esc(item.priority)}</strong> ` : "";
            const horizon = item.time_horizon ? ` <span class="muted">${esc(item.time_horizon)}</span>` : "";
            const actionText = item.action || "";
            const why = item.why ? ` — ${esc(item.why)}` : "";
            const li = document.createElement("li");
            li.innerHTML = `${priority}${linkifyCitations(actionText)}${why}${horizon}`;
            actions.appendChild(li);
          });
        }
      }
      if (dailyCves) {
        dailyCves.innerHTML = "";
        const items = Array.isArray(data.daily_cves) ? data.daily_cves : [];
        if (!items.length) {
          dailyCves.innerHTML = `<li class="muted">No CVEs recorded.</li>`;
        } else {
          items.forEach((item) => {
            if (typeof item === "string") {
              const li = document.createElement("li");
              li.textContent = item;
              dailyCves.appendChild(li);
              return;
            }
            const vendor = item.vendor || "Unknown vendor";
            const product = item.product || "Unknown product";
            const cveId = item.cve_id || "";
            const nvdUrl = item.nvd_url || (cveId ? `https://nvd.nist.gov/vuln/detail/${encodeURIComponent(cveId)}` : "");
            const kevDue = item.kev_due_date ? ` (due ${esc(item.kev_due_date)})` : "";
            let kevText = "Not KEV";
            if (item.kev) {
              if (item.kev_url) {
                kevText = `<a href="${esc(item.kev_url)}" target="_blank" rel="noopener">KEV${kevDue}</a>`;
              } else {
                kevText = `KEV${kevDue}`;
              }
            }
            const cveLink = cveId
              ? nvdUrl
                ? `<a href="${esc(nvdUrl)}" target="_blank" rel="noopener">${esc(cveId)}</a>`
                : esc(cveId)
              : "CVE";
            const li = document.createElement("li");
            li.innerHTML = `${esc(vendor)} — ${esc(product)} — ${cveLink} — ${kevText}`;
            dailyCves.appendChild(li);
          });
        }
      }
      if (podcastScript) {
        const script = data.podcast_script || "";
        if (!script) {
          podcastScript.innerHTML = `<p class="muted">No podcast script recorded.</p>`;
        } else {
          podcastScript.innerHTML = script
            .split("\n\n")
            .map((p) => (p ? `<p>${esc(p)}</p>` : ""))
            .join("");
        }
      }
      if (nistBreakdown) {
        const nistSection = nistBreakdown.closest("section");
        nistBreakdown.innerHTML = "";
        const items = Array.isArray(data.families) ? data.families : [];
        if (!items.length) {
          if (nistSection) {
            nistSection.style.display = "none";
          }
        } else {
          if (nistSection) {
            nistSection.style.display = "";
          }
          items.forEach((item) => {
            const block = document.createElement("div");
            block.className = "brief-nist-block";
            const title = esc(item.family_title || item.family_id || "");
            const summary = item.summary ? linkifyCitations(item.summary) : "";
            const subtopics = Array.isArray(item.subtopics) ? item.subtopics : [];
            const subtopicsHtml = subtopics
              .map((sub) => {
                const subTitle = esc(sub.title || "");
                const severity = sub.severity ? `<span class="pill">${esc(sub.severity)}</span>` : "";
                const narrative = sub.narrative ? linkifyCitations(sub.narrative) : "";
                return `
                  <div class="brief-nist-subtopic">
                    <div class="brief-nist-subtopic-header">${subTitle} ${severity}</div>
                    ${narrative ? `<div class="brief-nist-subtopic-body">${narrative}</div>` : ""}
                  </div>
                `;
              })
              .join("");
            block.innerHTML = `
              <div class="brief-nist-header">${esc(item.family_id || "")}${title ? ` — ${title}` : ""}</div>
              ${summary ? `<div class="brief-nist-body">${summary}</div>` : ""}
              ${subtopicsHtml}
            `;
            nistBreakdown.appendChild(block);
          });
        }
      }
      if (citations) {
        citations.innerHTML = "";
        const items = Array.isArray(data.citations) ? data.citations : [];
        if (!items.length) {
          citations.innerHTML = `<li class="muted">No citations recorded.</li>`;
        } else {
          items.forEach((item) => {
            const li = document.createElement("li");
            const title = item.title || item.url || "";
            const sourceName = item.source_name || "";
            const summary = item.summary || "";
            li.id = `cite-${item.id}`;
            li.innerHTML = `
              <span class="muted">(${esc(item.id)})</span>
              <a href="${esc(item.url || "")}" target="_blank" rel="noopener">${esc(title)}</a>
              ${sourceName ? ` <span class="muted">— ${esc(sourceName)}</span>` : ""}
              ${summary ? `<details class="inline-details"><summary>Summary</summary><span>${esc(summary)}</span></details>` : ""}
            `;
            citations.appendChild(li);
          });
        }
      }
      if (lowValue) {
        lowValue.innerHTML = "";
        const items = Array.isArray(data.low_value) ? data.low_value : [];
        if (!items.length) {
          lowValue.innerHTML = `<li class="muted">No low-value items recorded.</li>`;
        } else {
          items.forEach((item) => {
            const li = document.createElement("li");
            if (typeof item === "number" || typeof item === "string") {
              li.innerHTML = `Citation ${esc(String(item))}`;
            } else {
              const reason = item.reason ? ` — ${esc(item.reason)}` : "";
              li.innerHTML = `Citation ${esc(String(item.citation_id || ""))}${reason}`;
            }
            lowValue.appendChild(li);
          });
        }
      }
      if (metaBlock) {
        const metaItems = data.meta || {};
        const entries = Object.entries(metaItems).filter(([key]) => !key.endsWith("_raw"));
        if (entries.length) {
          metaBlock.innerHTML = entries
            .map(([key, value]) => {
              if (value && typeof value === "object") {
                return `<div><strong>${esc(key)}:</strong> <pre class="mono">${esc(JSON.stringify(value, null, 2))}</pre></div>`;
              }
              return `<div><strong>${esc(key)}:</strong> ${esc(String(value))}</div>`;
            })
            .join("");
        } else {
          metaBlock.innerHTML = `<p class="muted">No meta recorded.</p>`;
        }
      }
      if (clusterRaw) clusterRaw.textContent = JSON.stringify(data.meta?.cluster_raw || {}, null, 2);
      if (summarizeRaw) summarizeRaw.textContent = JSON.stringify(data.meta?.summarize_raw || {}, null, 2);
      if (nistRaw) nistRaw.textContent = JSON.stringify(data.meta?.nist_raw || {}, null, 2);
      if (overallRaw) overallRaw.textContent = JSON.stringify(data.meta?.overall_raw || {}, null, 2);
    })
    .catch((err) => {
      if (error) {
        error.textContent = err.message || String(err);
        error.style.display = "block";
      }
    });
}

function wireThreatDetail() {
  const container = document.getElementById("threat-detail");
  if (!container) {
    return;
  }
  const actorKey = container.dataset.actorKey;
  apiFetch(`/admin/api/threats/${encodeURIComponent(actorKey)}`)
    .then((item) => {
      const aliases = item.aliases || [];
      const articles = item.articles || [];
      const cves = item.cves || [];
      container.innerHTML = `
        <div class="kv">
          <div><strong>${esc(item.display_name || item.actor_key || "")}</strong></div>
          <div>Type: ${esc(item.actor_type || "unknown")}</div>
          ${item.country ? `<div>Country: ${esc(item.country)}</div>` : ""}
          ${
            item.confidence !== null && item.confidence !== undefined
              ? `<div>Confidence: ${item.confidence}</div>`
              : ""
          }
        </div>
        <h3>Aliases</h3>
        ${aliases.length ? `<ul>${aliases.map((alias) => `<li>${esc(alias)}</li>`).join("")}</ul>` : `<p class="muted">None.</p>`}
        <h3>Linked Articles</h3>
        ${
          articles.length
            ? `<ul>${articles
                .map(
                  (article) =>
                    `<li><a href="/ui/content/articles/${article.id}">${esc(article.title || "")}</a> <span class="muted">${esc(
                      formatTimestamp(article.published_at)
                    )}</span></li>`
                )
                .join("")}</ul>`
            : `<p class="muted">No linked articles.</p>`
        }
        <h3>Linked CVEs</h3>
        ${
          cves.length
            ? `<ul>${cves
                .map(
                  (cve) =>
                    `<li><a href="/ui/cves/${cve.cve_id}">${esc(cve.cve_id || "")}</a> <span class="muted">${esc(
                      cve.severity || ""
                    )}</span></li>`
                )
                .join("")}</ul>`
            : `<p class="muted">No linked CVEs.</p>`
        }
      `;
    })
    .catch((err) => {
      container.textContent = err.message || String(err);
    });
}
function wireEvents() {
  const createBtn = document.getElementById("events-create");
  const createModal = document.getElementById("event-create-modal");
  const createClose = document.getElementById("event-create-close");
  const createSave = document.getElementById("event-create-save");
  const createTitle = document.getElementById("event-create-title");
  const createKind = document.getElementById("event-create-kind");
  const createDate = document.getElementById("event-create-date");
  const createLifecycle = document.getElementById("event-create-lifecycle");
  const createEntity = document.getElementById("event-create-entity");
  const createTier = document.getElementById("event-create-tier");
  const createConfidence = document.getElementById("event-create-confidence");
  const createSummary = document.getElementById("event-create-summary");
  const createEnrichWeb = document.getElementById("event-create-enrich-web");
  const editModal = document.getElementById("event-edit-modal");
  const editClose = document.getElementById("event-edit-close");
  const editSave = document.getElementById("event-edit-save");
  const editDelete = document.getElementById("event-edit-delete");
  const editId = document.getElementById("event-edit-id");
  const editTitle = document.getElementById("event-edit-title");
  const editKind = document.getElementById("event-edit-kind");
  const editStatus = document.getElementById("event-edit-status");
  const editSeverity = document.getElementById("event-edit-severity");
  const editDate = document.getElementById("event-edit-date");
  const editEntity = document.getElementById("event-edit-entity");
  const editTier = document.getElementById("event-edit-tier");
  const editConfidence = document.getElementById("event-edit-confidence");
  const editCandidate = document.getElementById("event-edit-candidate");
  const editSummary = document.getElementById("event-edit-summary");
  const editTags = document.getElementById("event-edit-tags");
  const editIsEvent = document.getElementById("event-edit-is-event");
  const table = document.getElementById("events-table");
  if (!table) {
    return;
  }
  const tbody = table.querySelector("tbody");
  const pager = document.getElementById("events-pager");
  const error = document.getElementById("events-error");
  const form = document.getElementById("events-filters");
  const deriveBtn = document.getElementById("events-derive");
  const normalizeBtn = document.getElementById("events-normalize");
  const rebuildBtn = document.getElementById("events-rebuild");
  const purgeBtn = document.getElementById("events-purge");
  const purgePreviewBtn = document.getElementById("events-purge-preview");
  let pageSize = 50;
  let eventsById = {};
  function setError(message) {
    if (!error) {
      return;
    }
    if (message) {
      error.textContent = message;
      error.style.display = "block";
    } else {
      error.textContent = "";
      error.style.display = "none";
    }
  }
  async function load(page) {
    setError("");
    const params = new URLSearchParams();
    const query = document.getElementById("events-query").value.trim();
    const kind = document.getElementById("events-kind").value;
    const severity = document.getElementById("events-severity").value;
    const status = document.getElementById("events-status").value;
    const candidate = document.getElementById("events-candidate").value;
    const articles = document.getElementById("events-articles").value;
    const after = document.getElementById("events-after").value;
    const before = document.getElementById("events-before").value;
    const includeLegacy = document.getElementById("events-include-legacy");
    if (query) params.set("query", query);
    if (kind) params.set("kind", kind);
    if (status) params.set("status", status);
    if (candidate) params.set("candidate", candidate);
    if (articles) params.set("article_bucket", articles);
    if (after) params.set("after", after);
    if (before) params.set("before", before);
    if (includeLegacy && includeLegacy.checked) params.set("include_legacy", "1");
    params.set("page", String(page));
    params.set("page_size", String(pageSize));
    const data = await apiFetch(`/admin/api/events?${params.toString()}`);
    tbody.innerHTML = "";
    eventsById = {};
    data.items.forEach((event) => {
      eventsById[event.id] = event;
      const row = document.createElement("tr");
      const when = event.last_seen_at || event.last_article_at || event.created_at || "";
      const eventLink = `/ui/events/${event.id}`;
      const published = String(event.publish_state || "").toLowerCase() === "published";
      row.innerHTML = `
        <td class="line-clamp-2" title="${esc(event.title || "")}"><a href="${eventLink}">${esc(event.title || "")}</a></td>
        <td><span class="badge muted">${esc(event.kind || "")}</span></td>
        <td>${statusBadge(event.status || "")}</td>
        <td>${event.lifecycle ? `<span class="badge ${event.lifecycle === "candidate" ? "warn" : "success"}">${esc(event.lifecycle)}</span>` : (event.candidate ? '<span class="badge warn">candidate</span>' : '<span class="badge success">confirmed</span>')}</td>
        <td>${published ? '<span class="badge success">yes</span>' : '<span class="badge muted">no</span>'}</td>
        <td>${event.confidence ? event.confidence.toFixed(2) : ""}</td>
        <td class="truncate" title="${esc(event.entity || "")}">${esc(event.entity || "")}</td>
        <td><span data-ts="${esc(event.incident_date || "")}"></span></td>
        <td><span data-ts="${esc(when)}"></span></td>
        <td><span class="badge muted">📰 ${event.article_count ?? 0}</span></td>
        <td class="table-actions">
          <div class="action-menu">
            <button class="action-menu-button" type="button" aria-label="More actions">☰</button>
            <div class="action-menu-list">
              <button type="button" class="event-edit" data-event-id="${event.id}">Edit</button>
              <button type="button" class="event-delete" data-event-id="${event.id}">Delete</button>
              <button type="button" class="event-search-web" data-event-id="${event.id}">Search Web</button>
              <button type="button" class="event-publish" data-event-id="${event.id}">Publish</button>
            </div>
          </div>
        </td>
      `;
      tbody.appendChild(row);
    });
    applyTimestampFormatting(tbody);
    renderPager(pager, data.total, data.page, data.page_size, load);
  }
  if (createBtn && createModal) {
    createBtn.addEventListener("click", () => {
      createModal.style.display = "block";
    });
  }
  if (createClose && createModal) {
    createClose.addEventListener("click", () => {
      createModal.style.display = "none";
    });
  }
  if (createSave) {
    createSave.addEventListener("click", async () => {
      const payload = {
        title: createTitle?.value || "",
        kind: createKind?.value || "other",
        status: createLifecycle?.value || "confirmed",
        lifecycle: createLifecycle?.value || "confirmed",
        occurred_at: createDate?.value || null,
        entity: createEntity?.value || null,
        confidence_tier: createTier?.value || "watch",
        confidence: createConfidence?.value ? parseFloat(createConfidence.value) : null,
        summary: createSummary?.value || null,
        manual: true,
        candidate: (createLifecycle?.value || "") === "candidate",
        run_web_enrich: !!createEnrichWeb?.checked,
      };
      if (!payload.title.trim()) {
        showToast("Title is required");
        return;
      }
      try {
        const event = await apiFetch("/admin/api/events", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        showToast("Event created");
        if (createModal) createModal.style.display = "none";
        load(1).catch(() => undefined);
      } catch (err) {
        showToast(err.message || String(err));
      }
    });
  }
  function openEditModal(event) {
    if (!editModal) {
      return;
    }
    editId.value = event.id || "";
    editTitle.value = event.title || "";
    editKind.value = event.kind || "other";
    editStatus.value = event.lifecycle || event.status || "candidate";
    editSeverity.value = event.severity || "UNKNOWN";
    editDate.value = event.incident_date || "";
    editEntity.value = event.entity || "";
    editTier.value = event.confidence_tier || "watch";
    editConfidence.value = event.confidence ?? "";
    editCandidate.value = event.candidate === true ? "true" : event.candidate === false ? "false" : "";
    editSummary.value = event.summary || "";
    editTags.value = ((event.meta && event.meta.tags) || event.tags || []).join(",");
    if (editIsEvent) {
      editIsEvent.checked = event.meta && typeof event.meta.is_event === "boolean" ? event.meta.is_event : true;
    }
    editModal.style.display = "block";
  }
  if (tbody) {
    tbody.addEventListener("click", async (event) => {
      const target = event.target.closest("button");
      if (!target) {
        return;
      }
      const eventId = target.dataset.eventId;
      if (!eventId) {
        return;
      }
      if (target.classList.contains("event-edit")) {
        try {
          const detail = await apiFetch(`/admin/api/events/${eventId}`);
          openEditModal(detail);
        } catch (err) {
          setError(err.message || String(err));
        }
      }
      if (target.classList.contains("event-delete")) {
        if (!confirm("Delete this event? This cannot be undone.")) {
          return;
        }
        try {
          await apiFetch(`/admin/api/events/${eventId}`, { method: "DELETE" });
          showToast("Event deleted");
          load(1).catch((err) => setError(err.message || String(err)));
        } catch (err) {
          setError(err.message || String(err));
        }
      }
      if (target.classList.contains("event-search-web")) {
        try {
          const payload = await apiFetch(`/admin/api/events/${eventId}/enrich/web`, {
            method: "POST",
            body: JSON.stringify({}),
          });
          const added = Number(payload && payload.added_count) || 0;
          showToast(`Web search queued${added ? ` (added ${added})` : ""}`);
        } catch (err) {
          setError(err.message || String(err));
        }
      }
      if (target.classList.contains("event-publish")) {
        try {
          const payload = await apiFetch(`/admin/api/events/${eventId}/publish`, {
            method: "POST",
            body: JSON.stringify({ publish: true }),
          });
          if (payload && payload.status === "blocked") {
            const reasons = ((payload.readiness && payload.readiness.reasons) || []).join(", ");
            showToast(`Publish blocked: ${reasons || "not_ready"}`);
            return;
          }
          showToast("Event published");
          load(currentPage).catch((err) => setError(err.message || String(err)));
        } catch (err) {
          setError(err.message || String(err));
        }
      }
    });
  }
  if (editClose && editModal) {
    editClose.addEventListener("click", () => {
      editModal.style.display = "none";
    });
  }
  if (editSave) {
    editSave.addEventListener("click", async () => {
      const eventId = editId.value;
      if (!eventId) {
        showToast("Missing event id");
        return;
      }
      const candidateValue = editCandidate.value;
      const payload = {
        title: editTitle.value || undefined,
        kind: editKind.value || undefined,
        status: editStatus.value || undefined,
        lifecycle: editStatus.value || undefined,
        severity: editSeverity.value || undefined,
        incident_date: editDate.value || undefined,
        entity: editEntity.value || undefined,
        confidence_tier: editTier.value || undefined,
        confidence: editConfidence.value ? parseFloat(editConfidence.value) : undefined,
        candidate: candidateValue === "" ? undefined : candidateValue === "true",
        summary: editSummary.value || undefined,
        tags: editTags.value ? editTags.value.split(",").map((t) => t.trim()).filter(Boolean) : undefined,
        is_event: editIsEvent ? editIsEvent.checked : undefined,
      };
      try {
        await apiFetch(`/admin/api/events/${eventId}`, {
          method: "PUT",
          body: JSON.stringify(payload),
        });
        showToast("Event updated");
        if (editModal) editModal.style.display = "none";
        load(1).catch((err) => setError(err.message || String(err)));
      } catch (err) {
        setError(err.message || String(err));
      }
    });
  }
  if (editDelete) {
    editDelete.addEventListener("click", async () => {
      const eventId = editId.value;
      if (!eventId) {
        return;
      }
      if (!confirm("Delete this event? This cannot be undone.")) {
        return;
      }
      try {
        await apiFetch(`/admin/api/events/${eventId}`, { method: "DELETE" });
        showToast("Event deleted");
        if (editModal) editModal.style.display = "none";
        load(1).catch((err) => setError(err.message || String(err)));
      } catch (err) {
        setError(err.message || String(err));
      }
    });
  }
  if (form) {
    tbody.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const cveId = target.dataset.cveId;
    if (!cveId) {
      return;
    }
    try {
      if (target.classList.contains("action-cve-desc")) {
        const result = await apiFetch(`/admin/api/cves/${cveId}/refresh`, { method: "POST" });
        toast(`Queued ${renderShortId(result.job_id || cveId)}`, "success");
        load(currentPage);
      }
      if (target.classList.contains("action-cve-products")) {
        const result = await apiFetch(`/admin/api/cves/${cveId}/enrich_products`, { method: "POST" });
        toast(result.status === "already_queued" ? "Already queued" : `Queued ${renderShortId(result.job_id || cveId)}`, "success");
        load(currentPage);
      }
    } catch (err) {
      toast(err.message || String(err), "error");
    }
  });
  form.addEventListener("submit", (event) => {
      event.preventDefault();
      load(1).catch((err) => setError(err.message || String(err)));
    });
  }
  if (rebuildBtn) {
    rebuildBtn.addEventListener("click", async () => {
      if (!confirm("Rebuild events from CVEs?")) {
        return;
      }
      try {
        const payload = await apiFetch("/admin/api/events/rebuild", {
          method: "POST",
          body: JSON.stringify({}),
        });
        if (payload && payload.job_id) {
          showToast(`Events rebuild queued (${payload.job_id})`);
        } else {
          showToast("Events rebuild queued");
        }
        load(1).catch((err) => setError(err.message || String(err)));
      } catch (err) {
        setError(err.message || String(err));
      }
    });
  }
  if (normalizeBtn) {
    normalizeBtn.addEventListener("click", async () => {
      try {
        const payload = await apiFetch("/admin/api/events/normalize_cve_keys", {
          method: "POST",
        });
        const stats = payload.stats || {};
        showToast(`Updated ${stats.updated || 0} CVE keys`);
        load(1).catch((err) => setError(err.message || String(err)));
      } catch (err) {
        setError(err.message || String(err));
      }
    });
  }
  if (deriveBtn) {
    deriveBtn.addEventListener("click", async () => {
      try {
        const payload = await apiFetch("/admin/api/events/derive", {
          method: "POST",
          body: JSON.stringify({}),
        });
        if (payload && payload.job_id) {
          showToast(`Event derivation queued (${payload.job_id})`);
        } else {
          showToast("Event derivation queued");
        }
      } catch (err) {
        setError(err.message || String(err));
      }
    });
  }
  async function runPurge(dryRunOverride) {
    const dryRunBox = document.getElementById("events-purge-dry");
    const modeSelect = document.getElementById("events-purge-mode");
    const olderDays = document.getElementById("events-purge-older");
    const kindsCve = document.getElementById("events-purge-cve-only");
    const emptyOnly = document.getElementById("events-purge-empty-only");
    const noVictims = document.getElementById("events-purge-no-victims");
    const noCves = document.getElementById("events-purge-no-cves");
    const noSources = document.getElementById("events-purge-no-sources");
    const research = document.getElementById("events-purge-research");
    const confidence = document.getElementById("events-purge-confidence");
    const dryRun = dryRunOverride !== undefined ? dryRunOverride : dryRunBox ? dryRunBox.checked : true;
    const mode = modeSelect ? modeSelect.value : "suppress";
    const olderDaysValue = olderDays ? parseInt(olderDays.value, 10) : null;
    const includeKinds = kindsCve && kindsCve.checked ? ["cve_cluster"] : null;
    const onlyEmpty = emptyOnly ? emptyOnly.checked : false;
    const confidenceValue = confidence && confidence.value ? parseFloat(confidence.value) : null;
    if (!dryRun && !confirm("Purge events now? Manual events will be kept.")) {
      return;
    }
    try {
      const payload = await apiFetch("/admin/api/events/purge", {
        method: "POST",
        body: JSON.stringify({
          dry_run: dryRun,
          mode: mode,
          older_than_days: Number.isNaN(olderDaysValue) ? null : olderDaysValue,
          kinds: includeKinds,
          require_no_victims: noVictims ? noVictims.checked : false,
          require_no_cves: noCves ? noCves.checked : false,
          require_no_sources: noSources ? noSources.checked : false,
          require_research: research ? research.checked : false,
          confidence_below: confidenceValue,
          only_empty_cve_clusters: onlyEmpty,
        }),
      });
      const stats = payload.stats || {};
      const deleted = stats.deleted ?? 0;
      const candidates = stats.candidates ?? 0;
      const matched = stats.matched ?? 0;
      showToast(
        dryRun
          ? `Preview: ${matched} match (candidates ${candidates})`
          : `${mode === "suppress" ? "Suppressed" : "Deleted"} ${deleted} events (matched ${matched})`
      );
      load(1).catch((err) => setError(err.message || String(err)));
    } catch (err) {
      setError(err.message || String(err));
    }
  }
  if (purgeBtn) {
    purgeBtn.addEventListener("click", async () => {
      const dryRunBox = document.getElementById("events-purge-dry");
      if (dryRunBox && dryRunBox.checked) {
        dryRunBox.checked = false;
      }
      runPurge(false);
    });
  }
  if (purgePreviewBtn) {
    purgePreviewBtn.addEventListener("click", async () => {
      runPurge(true);
    });
  }
  load(1).catch((err) => setError(err.message || String(err)));
}
function wireEventDetail() {
  const container = document.getElementById("event-detail");
  if (!container) {
    return;
  }
  const eventId = container.dataset.eventId;
  const cveTable = document.getElementById("event-cves-table");
  const productsList = document.getElementById("event-products-list");
  const articlesTable = document.getElementById("event-articles-table");
  const webTable = document.getElementById("event-web-sources-table");
  const reportPanel = document.getElementById("event-report-panel");
  const webError = document.getElementById("event-web-error");
  const webSearchBtn = document.getElementById("event-web-search");
  const webRefreshBtn = document.getElementById("event-web-refresh");
  const webQueryInput = document.getElementById("event-web-query");
  const webManualUrlInput = document.getElementById("event-web-manual-url");
  const webManualTitleInput = document.getElementById("event-web-manual-title");
  const webAddUrlBtn = document.getElementById("event-web-add-url");
  const webReplace = document.getElementById("event-web-replace");
  const webKeepLow = document.getElementById("event-web-keep-low");
  const webPromote = document.getElementById("event-web-promote");
  const webShowDiscarded = document.getElementById("event-web-show-discarded");
  const webShowLow = document.getElementById("event-web-show-low");
  function setWebError(message) {
    if (!webError) {
      return;
    }
    if (message) {
      webError.textContent = message;
      webError.style.display = "block";
    } else {
      webError.textContent = "";
      webError.style.display = "none";
    }
  }
  async function loadWebSources() {
    if (!webTable) {
      return;
    }
    setWebError("");
    const includeDiscarded = webShowDiscarded && webShowDiscarded.checked;
    const data = await apiFetch(
      `/admin/api/events/${eventId}/web_sources?include_discarded=${includeDiscarded ? "true" : "false"}`
    );
    const body = webTable.querySelector("tbody");
    body.innerHTML = "";
    const showLow = webShowLow ? webShowLow.checked : true;
    (data.items || []).forEach((item) => {
      if (!showLow && (item.score ?? 0) < 10) {
        return;
      }
      const row = document.createElement("tr");
      const published = item.published_at || "";
      const status = item.status || "new";
      const title = item.title || item.url || "";
      const snippet = item.snippet || "";
      row.innerHTML = `
        <td><span class="badge muted">${item.score ?? 0}</span></td>
        <td class="truncate" title="${esc(item.domain || "")}">${esc(item.domain || "")}</td>
        <td><span data-ts="${esc(published)}"></span></td>
        <td class="line-clamp-2" title="${esc(title)}"><a href="${esc(item.url)}" target="_blank" rel="noopener">${esc(title)}</a></td>
        <td>
          <div class="line-clamp-2" title="${esc(snippet)}">${esc(snippet)}</div>
          ${snippet ? `<details class="snippet-expand"><summary>Expand</summary><div class="mono wrap-pre">${esc(snippet)}</div></details>` : ""}
        </td>
        <td>${statusBadge(status)}</td>
        <td class="table-actions">
          <button class="btn small secondary web-promote" data-source-id="${item.id}" ${
            status === "promoted" ? "disabled" : ""
          }>Fetch</button>
          <button class="btn small secondary web-discard" data-source-id="${item.id}" ${
            status === "discarded" ? "disabled" : ""
          }>Discard</button>
        </td>
      `;
      body.appendChild(row);
    });
    applyTimestampFormatting(webTable);
  }
  apiFetch(`/admin/api/events/${eventId}`)
    .then((event) => {
      const summaryBtn = document.getElementById("event-summary-refresh");
      const reportBtn = document.getElementById("event-report-refresh");
      const rederiveBtn = document.getElementById("event-rederive");
      const publishBtn = document.getElementById("event-publish");
      const unpublishBtn = document.getElementById("event-unpublish");
      const attachBtn = document.getElementById("event-attach-article");
      const attachInput = document.getElementById("event-attach-article-id");
      const meta = `
        ${event.visibility && event.visibility !== "active"
          ? `<div class="banner warning">Visibility: ${esc(event.visibility)}</div>`
          : ""}
        <div class="meta-grid">
          <div><strong>ID:</strong> ${event.id}</div>
          <div><strong>Kind:</strong> ${event.kind}</div>
          <div><strong>Status:</strong> ${event.status}</div>
          <div><strong>Lifecycle:</strong> ${esc(event.lifecycle || event.status || "")}</div>
          <div><strong>Publish:</strong> ${esc(event.publish_state || "draft")}</div>
          <div><strong>Published at:</strong> ${esc(formatTimestamp(event.published_at))}</div>
          <div><strong>Site slug:</strong> ${esc(event.site_slug || event.id || "")}</div>
          <div><strong>Severity:</strong> ${event.severity || "UNKNOWN"}</div>
          <div><strong>Confidence:</strong> ${event.confidence_tier || "watch"} ${event.confidence ? `(${event.confidence.toFixed(2)})` : ""}</div>
          <div><strong>Narrative bullets:</strong> ${event.narrative && Array.isArray(event.narrative.bullets) ? event.narrative.bullets.length : 0}</div>
          <div><strong>Timeline entries:</strong> ${Array.isArray(event.timeline) ? event.timeline.length : 0}</div>
          <div><strong>Candidate:</strong> ${event.candidate ? "yes" : "no"}</div>
          <div><strong>Entity:</strong> ${esc(event.entity || "")}</div>
          <div><strong>Incident date:</strong> ${esc(event.incident_date || "")}</div>
          <div><strong>First seen:</strong> ${esc(formatTimestamp(event.first_seen_at))}</div>
          <div><strong>Last seen:</strong> ${esc(formatTimestamp(event.last_seen_at))}</div>
        </div>
        ${event.evidence && event.evidence.length ? `<div class="muted">Why: ${event.evidence.slice(0,5).map(esc).join(", ")}</div>` : ""}
        ${event.summary ? `<p class="summary">${event.summary}</p>` : ""}
      `;
      container.innerHTML = meta;
      if (summaryBtn) {
        summaryBtn.addEventListener("click", async () => {
          try {
            const payload = await apiFetch(`/admin/api/events/${eventId}/summary`, {
              method: "POST",
            });
            if (payload.summary) {
              showToast(
                `Narrative rebuilt (bullets: ${payload.narrative_bullet_count || 0}, timeline: ${payload.timeline_count || 0})`
              );
            } else {
              showToast("No narrative generated");
            }
            wireEventDetail();
          } catch (err) {
            showToast(err.message || String(err));
          }
        });
      }
      if (reportBtn) {
        reportBtn.addEventListener("click", async () => {
          try {
            const payload = await apiFetch(`/admin/api/events/${eventId}/report`, {
              method: "POST",
            });
            const jobLabel = payload.job_id ? shortId(payload.job_id) : "";
            showToast(`Report queued ${jobLabel ? `(${jobLabel})` : ""}`.trim());
          } catch (err) {
            showToast(err.message || String(err));
          }
        });
      }
      if (rederiveBtn) {
        rederiveBtn.addEventListener("click", async () => {
          try {
            const payload = await apiFetch(`/admin/api/events/${eventId}/rederive`, {
              method: "POST",
            });
            showToast(`Re-derive queued (${payload.queued || 0})`);
            wireEventDetail();
          } catch (err) {
            showToast(err.message || String(err));
          }
        });
      }
      if (attachBtn && attachInput) {
        attachBtn.addEventListener("click", async () => {
          const articleId = parseInt(attachInput.value, 10);
          if (!articleId) {
            showToast("Enter a valid article id");
            return;
          }
          try {
            await apiFetch(`/admin/api/events/${eventId}/articles`, {
              method: "POST",
              body: JSON.stringify({ article_id: articleId }),
            });
            showToast("Article attached");
            attachInput.value = "";
            wireEventDetail();
          } catch (err) {
            showToast(err.message || String(err));
          }
        });
      }
      if (publishBtn) {
        publishBtn.addEventListener("click", async () => {
          try {
            const payload = await apiFetch(`/admin/api/events/${eventId}/publish`, {
              method: "POST",
              body: JSON.stringify({ publish: true }),
            });
            if (payload.status === "blocked") {
              const reasons = ((payload.readiness && payload.readiness.reasons) || []).join(", ");
              showToast(`Publish blocked: ${reasons || "not_ready"}`);
              return;
            }
            showToast("Event published");
            wireEventDetail();
          } catch (err) {
            showToast(err.message || String(err));
          }
        });
      }
      if (unpublishBtn) {
        unpublishBtn.addEventListener("click", async () => {
          try {
            await apiFetch(`/admin/api/events/${eventId}/publish`, {
              method: "POST",
              body: JSON.stringify({ publish: false }),
            });
            showToast("Event moved to draft");
            wireEventDetail();
          } catch (err) {
            showToast(err.message || String(err));
          }
        });
      }
      const cves = (event.items && event.items.cves) || [];
      if (cveTable) {
        const body = cveTable.querySelector("tbody");
        body.innerHTML = "";
        cves.forEach((cve) => {
          const row = document.createElement("tr");
          row.innerHTML = `
            <td><a href="/ui/cves/${cve.cve_id}">${cve.cve_id}</a></td>
            <td>${cve.preferred_base_severity || ""}</td>
            <td>${cve.preferred_base_score ?? ""}</td>
            <td>${esc(formatTimestamp(cve.published_at))}</td>
            <td class="truncate" title="${cve.summary || ""}">${cve.summary || ""}</td>
          `;
          body.appendChild(row);
        });
      }
      const products = (event.items && event.items.products) || [];
      if (productsList) {
        productsList.innerHTML = "";
        if (!products.length) {
          productsList.innerHTML = "<li>None found</li>";
        } else {
          products.forEach((product) => {
            const li = document.createElement("li");
            const label = `${product.vendor_name || ""} ${product.product_name || ""}`.trim();
            li.innerHTML = `<a href="/ui/products/${product.product_key}">${label}</a>`;
            productsList.appendChild(li);
          });
        }
      }
      const articles = (event.items && event.items.articles) || [];
      if (articlesTable) {
        const body = articlesTable.querySelector("tbody");
        body.innerHTML = "";
        articles.forEach((article) => {
          const row = document.createElement("tr");
          const link = article.article_id
            ? `/ui/content/articles/${article.article_id}`
            : "";
          const detachButton = article.article_id
            ? `<button class="btn small secondary event-detach-article" data-article-id="${article.article_id}">Remove</button>`
            : "";
          row.innerHTML = `
            <td>${link ? `<a href="${link}">${article.title || ""}</a>` : (article.title || "")}</td>
            <td>${esc(formatTimestamp(article.published_at))}</td>
            <td>${article.url ? `<a href="${article.url}" target="_blank" rel="noopener">Source</a>` : ""}</td>
            <td>${detachButton}</td>
          `;
          body.appendChild(row);
        });
      }
      if (reportPanel) {
        const report = event.report && typeof event.report === "object" ? event.report : null;
        const renderList = (items) => {
          if (!Array.isArray(items) || !items.length) return "";
          const lis = items.map((item) => `<li>${esc(String(item || ""))}</li>`).join("");
          return `<ul>${lis}</ul>`;
        };
        const renderTimeline = (items) => {
          if (!Array.isArray(items) || !items.length) return "";
          const rows = items
            .map((item) => {
              const obj = item && typeof item === "object" ? item : {};
              const date = esc(String(obj.date || obj.published_at || "Unknown date"));
              const title = esc(String(obj.event || obj.title || obj.summary || ""));
              const evidence = Array.isArray(obj.evidence) ? obj.evidence : [];
              const evHtml = evidence.length
                ? `<ul>${evidence.map((e) => `<li>${esc(String(e || ""))}</li>`).join("")}</ul>`
                : "";
              return `<li><strong>${date}</strong>${title ? `: ${title}` : ""}${evHtml}</li>`;
            })
            .join("");
          return `<ul>${rows}</ul>`;
        };
        const narrative = event.narrative && typeof event.narrative === "object" ? event.narrative : {};
        const narrativeSections =
          narrative.sections && typeof narrative.sections === "object" ? narrative.sections : {};
        const reportOverview =
          report && report.overview ? String(report.overview) : String(narrative.summary || "");
        const reportTimeline =
          report && Array.isArray(report.timeline) && report.timeline.length
            ? report.timeline
            : Array.isArray(event.timeline)
              ? event.timeline
              : [];
        const attribution = report && report.attribution && typeof report.attribution === "object" ? report.attribution : null;
        const attributionHtml = attribution
          ? `
            <h3>Attribution</h3>
            <ul>
              <li><strong>Responsible Actor:</strong> ${esc(String(attribution.responsible_actor || "unknown"))}</li>
              <li><strong>Actor Type:</strong> ${esc(String(attribution.actor_type || "unknown"))}</li>
              <li><strong>Attribution Confidence:</strong> ${esc(String(attribution.confidence || "unknown"))}</li>
            </ul>
            ${renderList(attribution.rationale) ? `<h4>Rationale</h4>${renderList(attribution.rationale)}` : ""}
            ${renderList(attribution.disputed_claims) ? `<h4>Disputed Claims</h4>${renderList(attribution.disputed_claims)}` : ""}
          `
          : "";
        const sections = [
          ["breach_compromise", "Breach and Compromise", report ? report.compromise_path : []],
          ["impact", "Impact", report ? report.impact : []],
          ["response_recovery", "Response and Recovery", report ? report.response_recovery : []],
          ["lessons_learned", "Lessons Learned", report ? report.lessons_learned : []],
          ["compromise_path", "Compromise Path", report ? report.compromise_path : []],
          ["investigation_findings", "Investigation Findings", report ? report.investigation_findings : []],
          ["legal_regulatory_outcomes", "Legal and Regulatory Outcomes", report ? report.legal_regulatory_outcomes : []],
          ["confidence_notes", "Confidence Notes", report ? report.confidence_notes : []],
        ];
        const sectionHtml = sections
          .map(([nKey, title, reportItems]) => {
            const reportList = renderList(reportItems);
            const narrativeList = renderList(narrativeSections[nKey]);
            const body = reportList || narrativeList;
            return body ? `<h3>${title}</h3>${body}` : "";
          })
          .join("");
        const bulletsHtml = renderList(Array.isArray(narrative.bullets) ? narrative.bullets : []);
        const timelineHtml = renderTimeline(reportTimeline);
        const hasAnyContent = Boolean(
          reportOverview || attributionHtml || bulletsHtml || sectionHtml || timelineHtml
        );
        reportPanel.innerHTML = hasAnyContent
          ? `
            ${report && report.generated_at ? `<div class="muted"><strong>Generated:</strong> ${esc(formatTimestamp(report.generated_at))}</div>` : ""}
            ${reportOverview ? `<p class="summary">${esc(reportOverview)}</p>` : ""}
            ${bulletsHtml ? `<h3>Key Points</h3>${bulletsHtml}` : ""}
            ${attributionHtml}
            ${sectionHtml}
            ${timelineHtml ? `<h3>Timeline</h3>${timelineHtml}` : ""}
          `
          : `<div class="muted">No report generated yet.</div>`;
      }
      if (webShowDiscarded) {
        webShowDiscarded.addEventListener("change", () => {
          loadWebSources().catch((err) => setWebError(err.message || String(err)));
        });
      }
      if (webShowLow) {
        webShowLow.addEventListener("change", () => {
          loadWebSources().catch((err) => setWebError(err.message || String(err)));
        });
      }
      if (articlesTable) {
        articlesTable.addEventListener("click", async (evt) => {
          const target = evt.target;
          if (!(target instanceof HTMLElement)) return;
          if (target.classList.contains("event-detach-article")) {
            const articleId = parseInt(target.dataset.articleId || "", 10);
            if (!articleId) return;
            try {
              await apiFetch(`/admin/api/events/${eventId}/articles/detach`, {
                method: "POST",
                body: JSON.stringify({ article_id: articleId }),
              });
              showToast("Article detached");
              wireEventDetail();
            } catch (err) {
              showToast(err.message || String(err));
            }
          }
        });
      }
      if (webSearchBtn) {
        webSearchBtn.addEventListener("click", async () => {
          try {
            const payload = {
              query: webQueryInput ? webQueryInput.value.trim() : "",
              replace_existing: webReplace ? webReplace.checked : true,
              keep_low: webKeepLow ? webKeepLow.checked : false,
              promote_on_enrich: webPromote ? webPromote.checked : false,
            };
            const result = await apiFetch(`/admin/api/events/${eventId}/enrich/web`, {
              method: "POST",
              body: JSON.stringify(payload),
            });
            const jobLabel = result.job_id ? shortId(result.job_id) : "";
            showToast(`Enrichment queued ${jobLabel ? `(${jobLabel})` : ""}`.trim());
          } catch (err) {
            showToast(err.message || String(err));
            setWebError(err.message || String(err));
          }
        });
      }
      if (webRefreshBtn) {
        webRefreshBtn.addEventListener("click", () => {
          loadWebSources().catch((err) => setWebError(err.message || String(err)));
        });
      }
      if (webAddUrlBtn) {
        webAddUrlBtn.addEventListener("click", async () => {
          const manualUrl = webManualUrlInput ? webManualUrlInput.value.trim() : "";
          const manualTitle = webManualTitleInput ? webManualTitleInput.value.trim() : "";
          if (!manualUrl) {
            setWebError("Enter a URL to add.");
            return;
          }
          try {
            const result = await apiFetch(`/admin/api/events/${eventId}/web_sources/manual`, {
              method: "POST",
              body: JSON.stringify({
                url: manualUrl,
                title: manualTitle || null,
              }),
            });
            const jobLabel = result.job_id ? shortId(result.job_id) : "";
            showToast(`URL queued ${jobLabel ? `(${jobLabel})` : ""}`.trim());
            if (webManualUrlInput) webManualUrlInput.value = "";
            if (webManualTitleInput) webManualTitleInput.value = "";
            loadWebSources().catch((err) => setWebError(err.message || String(err)));
          } catch (err) {
            setWebError(err.message || String(err));
          }
        });
      }
      if (webTable) {
        webTable.addEventListener("click", async (event) => {
          const target = event.target;
          if (!(target instanceof HTMLElement)) {
            return;
          }
          if (target.classList.contains("web-promote")) {
            const sourceId = target.dataset.sourceId;
            try {
              const result = await apiFetch(
                `/admin/api/events/${eventId}/web_sources/${sourceId}/promote`,
                { method: "POST" }
              );
              const jobLabel = result.job_id ? shortId(result.job_id) : "";
              showToast(`Fetch queued ${jobLabel ? `(${jobLabel})` : ""}`.trim());
              loadWebSources().catch((err) => setWebError(err.message || String(err)));
            } catch (err) {
              setWebError(err.message || String(err));
            }
          }
          if (target.classList.contains("web-discard")) {
            const sourceId = target.dataset.sourceId;
            try {
              await apiFetch(
                `/admin/api/events/${eventId}/web_sources/${sourceId}/discard`,
                { method: "POST" }
              );
              showToast("Discarded");
              loadWebSources().catch((err) => setWebError(err.message || String(err)));
            } catch (err) {
              setWebError(err.message || String(err));
            }
          }
        });
      }
      loadWebSources().catch((err) => setWebError(err.message || String(err)));
    })
    .catch((err) => {
      container.innerHTML = `<div class="error-banner">${err.message || String(err)}</div>`;
    });
}
function wireProducts() {
  const table = document.getElementById("products-table");
  if (!table) {
    return;
  }
  const tbody = table.querySelector("tbody");
  const pager = document.getElementById("products-pager");
  const error = document.getElementById("products-error");
  const form = document.getElementById("products-filters");
  const backfillBtn = document.getElementById("products-backfill");
  let pageSize = 50;
  function setError(message) {
    if (!error) {
      return;
    }
    if (message) {
      error.textContent = message;
      error.style.display = "block";
    } else {
      error.textContent = "";
      error.style.display = "none";
    }
  }
  async function load(page) {
    setError("");
    const params = new URLSearchParams();
    const query = document.getElementById("products-query").value.trim();
    const vendor = document.getElementById("products-vendor").value.trim();
    if (query) params.set("query", query);
    if (vendor) params.set("vendor", vendor);
    params.set("page", String(page));
    params.set("page_size", String(pageSize));
    const data = await apiFetch(`/admin/api/products?${params.toString()}`);
    tbody.innerHTML = "";
    if (!data.items || !data.items.length) {
      const row = document.createElement("tr");
      row.innerHTML = `<td colspan="3" class="muted">No products found. Run “Backfill from CVEs” or enqueue CVE enrichment.</td>`;
      tbody.appendChild(row);
      renderPager(pager, data.total, data.page, data.page_size, load);
      return;
    }
    data.items.forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${item.vendor_name || ""}</td>
        <td><a href="/ui/products/${item.product_key}">${item.product_name || ""}</a></td>
        <td class="mono">${item.product_key || ""}</td>
        <td class="mono">${item.link_count ?? 0}</td>
      `;
      tbody.appendChild(row);
    });
    renderPager(pager, data.total, data.page, data.page_size, load);
  }
  if (form) {
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      load(1).catch((err) => setError(err.message || String(err)));
    });
  }
  if (backfillBtn) {
    backfillBtn.addEventListener("click", async () => {
      if (!confirm("Backfill products from existing CVEs?")) {
        return;
      }
      try {
        await apiFetch("/admin/api/products/backfill", {
          method: "POST",
          body: JSON.stringify({}),
        });
        showToast("Backfill complete");
        load(1).catch((err) => setError(err.message || String(err)));
      } catch (err) {
        setError(err.message || String(err));
      }
    });
  }
  load(1).catch((err) => setError(err.message || String(err)));
}
function wireProductDetail() {
  const container = document.getElementById("product-detail");
  if (!container) {
    return;
  }
  const productKey = container.dataset.productKey;
  const facetsEl = document.createElement("div");
  facetsEl.className = "facet-list";
  container.appendChild(facetsEl);
  const cveTable = document.getElementById("product-cves-table");
  const cvePager = document.getElementById("product-cves-pager");
  const eventsTable = document.getElementById("product-events-table");
  const eventsPager = document.getElementById("product-events-pager");
  const articlesTable = document.getElementById("product-articles-table");
  const articlesPager = document.getElementById("product-articles-pager");
  const cveFilters = document.getElementById("product-cve-filters");
  let cvePageSize = 50;
  let eventsPageSize = 25;
  function renderFacets(facets) {
    facetsEl.innerHTML = "";
    const entries = Object.entries(facets || {});
    if (!entries.length) {
      facetsEl.textContent = "No CVE facets.";
      return;
    }
    entries.forEach(([severity, count]) => {
      const chip = document.createElement("span");
      chip.className = "facet-chip";
      chip.textContent = `${severity}: ${count}`;
      facetsEl.appendChild(chip);
    });
  }
  async function loadProduct() {
    const data = await apiFetch(`/admin/api/products/${productKey}`);
    container.innerHTML = `
      <h2>${data.product.vendor_name} ${data.product.product_name}</h2>
      <div class="mono">${data.product.product_key}</div>
    `;
    container.appendChild(facetsEl);
    renderFacets(data.facets);
  }
  function selectedSeverities() {
    if (!cveFilters) {
      return "";
    }
    const values = [];
    cveFilters.querySelectorAll("input[type='checkbox']").forEach((box) => {
      if (box.checked) {
        values.push(box.value);
      }
    });
    return values.join(",");
  }
  async function loadCves(page) {
    const params = new URLSearchParams();
    const severity = selectedSeverities();
    if (severity) {
      params.set("severity", severity);
    }
    params.set("page", String(page));
    params.set("page_size", String(cvePageSize));
    const data = await apiFetch(
      `/admin/api/products/${productKey}/cves?${params.toString()}`
    );
    if (cveTable) {
      const body = cveTable.querySelector("tbody");
      body.innerHTML = "";
      data.items.forEach((cve) => {
        const row = document.createElement("tr");
        row.innerHTML = `
          <td><a href="/ui/cves/${cve.cve_id}">${cve.cve_id}</a></td>
          <td>${cve.preferred_base_severity || ""}</td>
          <td>${cve.preferred_base_score ?? ""}</td>
          <td>${esc(formatTimestamp(cve.published_at))}</td>
          <td class="truncate" title="${cve.summary || ""}">${cve.summary || ""}</td>
        `;
        body.appendChild(row);
      });
      renderPager(cvePager, data.total, data.page, data.page_size, loadCves);
    }
  }
  async function loadArticles(page) {
    const params = new URLSearchParams();
    params.set("page", String(page));
    params.set("page_size", String(25));
    const data = await apiFetch(
      `/admin/api/products/${productKey}/articles?${params.toString()}`
    );
    if (articlesTable) {
      const body = articlesTable.querySelector("tbody");
      body.innerHTML = "";
      data.items.forEach((item) => {
        const row = document.createElement("tr");
        row.innerHTML = `
          <td>${esc(formatTimestamp(item.published_at || item.ingested_at))}</td>
          <td>${esc(item.source_name || "")}</td>
          <td><a href="/ui/content/${item.id}">${esc(item.title || "")}</a></td>
          <td>${esc(item.tags || "")}</td>
        `;
        body.appendChild(row);
      });
      renderPager(articlesPager, data.total, data.page, data.page_size, loadArticles);
    }
  }

  async function loadEvents(page) {
    const params = new URLSearchParams();
    params.set("page", String(page));
    params.set("page_size", String(eventsPageSize));
    const data = await apiFetch(
      `/admin/api/products/${productKey}/events?${params.toString()}`
    );
    if (eventsTable) {
      const body = eventsTable.querySelector("tbody");
      body.innerHTML = "";
      data.items.forEach((event) => {
        const row = document.createElement("tr");
        row.innerHTML = `
          <td><a href="/ui/events/${event.id}">${event.title}</a></td>
          <td>${event.kind || ""}</td>
          <td>${event.severity || ""}</td>
          <td>${event.status || ""}</td>
          <td>${esc(formatTimestamp(event.last_seen_at))}</td>
        `;
        body.appendChild(row);
      });
      renderPager(eventsPager, data.total, data.page, data.page_size, loadEvents);
    }
  }
  if (cveFilters) {
    cveFilters.addEventListener("change", () => {
      loadCves(1).catch((err) => showToast(err.message || String(err)));
    });
  }
  loadProduct()
    .then(() => loadCves(1))
    .then(() => loadEvents(1))
    .then(() => loadArticles(1))
    .catch((err) => {
      container.innerHTML = `<div class="error-banner">${err.message || String(err)}</div>`;
    });
}
function wireDangerZone() {
  const section = document.querySelector(".danger-zone");
  if (!section) {
    return;
  }
  function setup(panelId, confirmToken, endpoint, allowFiles) {
    const panel = document.getElementById(panelId);
    if (!panel) {
      return;
    }
    const ack = panel.querySelector(".danger-ack");
    const confirmInput = panel.querySelector(".danger-confirm");
    const btn = panel.querySelector(".danger-btn");
    const result = panel.querySelector(".danger-result");
    const deleteFiles = panel.querySelector(".danger-delete-files");
    function updateState() {
      const ok = ack.checked && confirmInput.value.trim() === confirmToken;
      btn.disabled = !ok;
    }
    ack.addEventListener("change", updateState);
    confirmInput.addEventListener("input", updateState);
    btn.addEventListener("click", async () => {
      result.textContent = "";
      try {
        const payload = { confirm: confirmToken };
        if (allowFiles && deleteFiles) {
          payload.delete_files = deleteFiles.checked;
        }
        const data = await apiFetch(endpoint, {
          method: "POST",
          body: JSON.stringify(payload),
        });
        result.textContent = JSON.stringify(data.stats, null, 2);
        showToast("Deletion complete");
      } catch (err) {
        result.textContent = err.message || String(err);
      }
    });
    updateState();
  }
  setup("danger-articles", "DELETE_ALL_ARTICLES", "/admin/api/admin/clear/articles", true);
  setup("danger-cves", "DELETE_ALL_CVES", "/admin/api/admin/clear/cves", false);
  setup("danger-events", "DELETE_ALL_EVENTS", "/admin/api/admin/clear/events", false);
  setup("danger-all", "DELETE_ALL_CONTENT", "/admin/api/admin/clear/all", true);
  setup("danger-site-data", "REBUILD_SITE_DATA", "/admin/api/admin/rebuild/site-data", false);
}
function wireDebug() {
  const cards = document.getElementById("debug-cards");
  if (!cards) {
    return;
  }
  const error = document.getElementById("debug-error");
  const refresh = document.getElementById("debug-refresh");
  const smoke = document.getElementById("debug-smoke");
  const productsSmoke = document.getElementById("debug-products-smoke");
  const buildNow = document.getElementById("debug-build");
  const statusGrid = document.getElementById("debug-status");
  const jobsBody = document.querySelector("#debug-jobs-table tbody");
  const buildEl = document.getElementById("debug-build");
  const cveEl = document.getElementById("debug-cve-sync");
  const ingestEl = document.getElementById("debug-ingest");
  const llmBody = document.querySelector("#debug-llm-table tbody");
  function renderCards(data) {
    cards.innerHTML = "";
    const items = [
      ["Schema", data.db_schema_version || "unknown"],
      ["Articles", data.counts?.articles ?? 0],
      ["Article Tags", data.counts?.article_tags ?? 0],
      ["CVEs", data.counts?.cves ?? 0],
      ["Vendors", data.counts?.vendors ?? 0],
      ["Products", data.counts?.products ?? 0],
      ["CVE Products", data.counts?.cve_products ?? 0],
      ["CVE Product Versions", data.counts?.cve_product_versions ?? 0],
      ["Events", data.counts?.events ?? 0],
      ["Event Items", data.counts?.event_items ?? 0],
      ["Jobs", data.counts?.jobs ?? 0],
      ["Health Runs", data.counts?.source_health_history ?? 0],
      ["LLM Runs", data.counts?.llm_runs ?? 0],
    ];
    items.forEach(([label, value]) => {
      const card = document.createElement("div");
      card.className = "stat-card";
      card.innerHTML = `<div class="stat-label">${label}</div><div class="stat-value">${value}</div>`;
      cards.appendChild(card);
    });
  }
  function renderStatus(data) {
    if (!statusGrid) {
      return;
    }
    statusGrid.innerHTML = "";
    const status = data.status_metrics || {};
    const items = [
      {
        label: "Articles with content error",
        value: status.articles_with_content_error_count ?? 0,
        link: "/ui/content?type=article&content_error=1&content_error_kind=other",
      },
      {
        label: "Articles 404/410",
        value: status.articles_404_count ?? 0,
        link: "/ui/content?type=article&content_error=1&content_error_kind=404",
      },
      {
        label: "Articles stale (>1 week)",
        value: status.articles_stale_count ?? 0,
        link: "/ui/content?type=article&content_error=1&content_error_kind=stale",
      },
      {
        label: "Articles max retries exceeded",
        value: status.articles_max_retries_count ?? 0,
        link: "/ui/content?type=article&content_error=1&content_error_kind=max_retries",
      },
      {
        label: "Articles pending publish",
        value: status.articles_pending_publish ?? 0,
        link: "/ui/content?type=article&needs=publish",
      },
      {
        label: "CVEs missing description",
        value: status.cves_missing_description_count ?? 0,
        link: "/ui/cves",
      },
      {
        label: "Event candidates",
        value: status.events_candidate_count ?? 0,
        link: "/ui/events",
      },
      {
        label: "LLM configured",
        value: status.llm_configured ? "yes" : "no",
        link: "/ui/ai",
      },
      {
        label: "LLM stages active",
        value: `${status.llm_stage_active || 0}/${status.llm_stage_total || 0}`,
        link: "/ui/ai",
      },
    ];
    items.forEach((item) => {
      const row = document.createElement("div");
      row.className = "status-row";
      row.innerHTML = `
        <span>${item.label}</span>
        <span>${item.link ? `<a href="${item.link}">${item.value}</a>` : item.value}</span>
      `;
      statusGrid.appendChild(row);
    });
  }
  function renderJobs(rows) {
    if (!jobsBody) {
      return;
    }
    jobsBody.innerHTML = "";
    rows.forEach((job) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td class="mono">${job.id}</td>
        <td>${job.job_type}</td>
        <td>${job.status}</td>
        <td>${esc(formatTimestamp(job.requested_at))}</td>
        <td>${esc(formatTimestamp(job.started_at))}</td>
        <td>${esc(formatTimestamp(job.finished_at))}</td>
        <td class="truncate" title="${job.error || ""}">${job.error || ""}</td>
      `;
      jobsBody.appendChild(row);
    });
  }
  function renderLlmRuns(rows) {
    if (!llmBody) {
      return;
    }
    llmBody.innerHTML = "";
    rows.forEach((run) => {
      const providerLabel = run.provider_name || run.provider_id || "";
      const modelLabel = run.model_name || run.model_id || "";
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${esc(formatTimestamp(run.ts))}</td>
        <td>${esc(providerLabel)}</td>
        <td>${esc(modelLabel)}</td>
        <td>${esc(run.prompt_name || "")}</td>
        <td>${run.latency_ms || ""}</td>
        <td>${run.ok ? "ok" : "error"}</td>
        <td class="truncate" title="${run.error || ""}">${run.error || ""}</td>
      `;
      llmBody.appendChild(row);
    });
  }
  async function loadOverview() {
    if (error) {
      error.style.display = "none";
      error.textContent = "";
    }
    const data = await apiFetch("/admin/api/debug/overview");
    renderCards(data);
    renderStatus(data);
    renderJobs(data.last_jobs || []);
    renderLlmRuns(data.last_llm_runs || []);
    if (buildEl) {
      buildEl.textContent = JSON.stringify(data.last_build_job || {}, null, 2);
    }
    if (cveEl) {
      cveEl.textContent = JSON.stringify(data.last_cve_sync || {}, null, 2);
    }
    if (ingestEl) {
      ingestEl.textContent = JSON.stringify(data.last_article_ingest || {}, null, 2);
    }
  }
  if (refresh) {
    refresh.addEventListener("click", () => {
      loadOverview().catch((err) => {
        if (error) {
          error.textContent = err.message || String(err);
          error.style.display = "block";
        }
      });
    });
  }
  if (smoke) {
    smoke.addEventListener("click", async () => {
      try {
        const data = await apiFetch("/admin/api/debug/smoke", {
          method: "POST",
          body: JSON.stringify({}),
        });
        showToast(`Smoke test enqueued: ${data.job_id}`);
      } catch (err) {
        if (error) {
          error.textContent = err.message || String(err);
          error.style.display = "block";
        }
      }
    });
  }
  if (productsSmoke) {
    productsSmoke.addEventListener("click", async () => {
      try {
        const data = await apiFetch("/admin/api/debug/products-smoke", {
          method: "POST",
          body: JSON.stringify({}),
        });
        const status = data.status || "ok";
        showToast(`Product smoke: ${status}`);
      } catch (err) {
        if (error) {
          error.textContent = err.message || String(err);
          error.style.display = "block";
        }
      }
    });
  }
  if (buildNow) {
    buildNow.addEventListener("click", async () => {
      try {
        const data = await apiFetch("/jobs/enqueue", {
          method: "POST",
          body: JSON.stringify({ job_type: "build_site" }),
        });
        showToast(`Build enqueued: ${data.job_id}`);
      } catch (err) {
        if (error) {
          error.textContent = err.message || String(err);
          error.style.display = "block";
        }
      }
    });
  }
  loadOverview().catch((err) => {
    if (error) {
      error.textContent = err.message || String(err);
      error.style.display = "block";
    }
  });
}
async function wireAnalytics() {
  const chartEl = document.getElementById("articles-chart");
  const error = document.getElementById("analytics-error");
  if (!chartEl || !window.Chart) {
    return;
  }
  try {
    const data = await apiFetch("/admin/analytics/articles_per_day?days=30");
    if (data.error) {
      if (error) {
        error.textContent = data.error;
        error.style.display = "block";
      }
      return;
    }
    const labels = data.data.map((row) => row.day);
    const counts = data.data.map((row) => row.count);
    new Chart(chartEl, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Articles",
            data: counts,
            borderColor: "#2563eb",
            backgroundColor: "rgba(37,99,235,0.1)",
            fill: true,
            tension: 0.2,
          },
        ],
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
      },
    });
    const stats = await apiFetch("/admin/analytics/source_stats?days=7&runs=20");
    if (stats.error) {
      if (error) {
        error.textContent = stats.error;
        error.style.display = "block";
      }
      return;
    }
    const table = document.querySelector("#source-stats tbody");
    if (table) {
      table.innerHTML = stats.data
        .map(
          (row) =>
            `<tr>
              <td>${row.source_name}</td>
              <td>${row.articles_per_day_avg}</td>
              <td>${esc(formatTimestamp(row.last_ok_at))}</td>
              <td class="truncate" title="${row.last_error || ""}">${row.last_error || ""}</td>
              <td>${row.ok_rate}%</td>
              <td>${row.total_articles}</td>
              <td>${row.pct_full_content}%</td>
              <td>${row.pct_summaries}%</td>
            </tr>`
        )
        .join("");
    }
    const dateBtn = document.getElementById("brief-date-run");
    const dateField = document.getElementById("brief-date");
    if (dateBtn && dateField) {
      dateBtn.addEventListener("click", async () => {
        if (!dateField.value) {
          alert("Select a date");
          return;
        }
        try {
          await apiFetch("/admin/api/daily_brief/build", {
            method: "POST",
            body: JSON.stringify({ date: dateField.value }),
          });
          showToast("Brief job enqueued");
        } catch (err) {
          if (error) {
            error.textContent = err.message || String(err);
            error.style.display = "block";
          }
        }
      });
    }
    if (error) {
      error.style.display = "none";
      error.textContent = "";
    }
  } catch (err) {
    if (error) {
      error.textContent = err.message || String(err);
      error.style.display = "block";
    }
  }
}
function wireAiTest() {
  const form = document.getElementById("ai-test-form");
  if (!form) {
    return;
  }
  const providerField = document.getElementById("ai-test-provider");
  const modelField = document.getElementById("ai-test-model");
  const promptField = document.getElementById("ai-test-prompt");
  const output = document.getElementById("ai-test-output");
  const runsBody = document.querySelector("#ai-runs-table tbody");
  async function loadRuns() {
    if (!runsBody) {
      return;
    }
    const data = await apiFetch("/admin/api/ai/runs?limit=10");
    runsBody.innerHTML = "";
    (data.items || []).forEach((run) => {
      const providerLabel = run.provider_name || run.provider_id || "";
      const modelLabel = run.model_name || run.model_id || "";
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${esc(formatTimestamp(run.ts))}</td>
        <td>${esc(providerLabel)}</td>
        <td>${esc(modelLabel)}</td>
        <td>${esc(run.prompt_name || "")}</td>
        <td>${run.latency_ms || ""}</td>
        <td>${run.ok ? "ok" : "error"}</td>
        <td class="truncate" title="${run.error || ""}">${run.error || ""}</td>
      `;
      runsBody.appendChild(row);
    });
  }
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (output) {
      output.textContent = "Running...";
    }
    try {
      const payload = await apiFetch("/admin/api/ai/test", {
        method: "POST",
        body: JSON.stringify({
          provider_id: providerField.value,
          model_id: modelField.value,
          prompt: promptField.value,
        }),
      });
      if (output) {
        output.textContent = JSON.stringify(payload, null, 2);
      }
      loadRuns().catch((err) => console.error(err));
    } catch (err) {
      if (output) {
        output.textContent = err.message || String(err);
      }
    }
  });
  loadRuns().catch((err) => console.error(err));
}
function wireUtilities() {
  const checkBtn = document.getElementById("utilities-pipeline-check");
  const refreshBtn = document.getElementById("utilities-metrics-refresh");
  const rebuildVendorBtn = document.getElementById("utilities-rebuild-vendor-products");
  const resetCountersBtn = document.getElementById("utilities-reset-counters");
  const cancelBriefBtn = document.getElementById("utilities-cancel-daily-brief");
  const cancelRestartBriefBtn = document.getElementById("utilities-cancel-daily-brief-restart");
  if (checkBtn) {
    checkBtn.addEventListener("click", async () => {
      try {
        await apiFetch("/admin/api/dashboard/metrics");
        showToast("Pipeline check complete", "success");
      } catch (err) {
        showToast(err.message || String(err), "error");
      }
    });
  }
  if (refreshBtn) {
    refreshBtn.addEventListener("click", async () => {
      try {
        await apiFetch("/admin/api/dashboard/metrics");
        showToast("Pipeline metrics refreshed", "success");
      } catch (err) {
        showToast(err.message || String(err), "error");
      }
    });
  }
  if (rebuildVendorBtn) {
    rebuildVendorBtn.addEventListener("click", async () => {
      try {
        const payload = await apiFetch("/admin/api/dashboard/rebuild_vendor_products", { method: "POST" });
        if (payload && payload.status === "queued") {
          showToast(`Vendor/product rebuild queued (${payload.job_id})`, "success");
        } else {
          showToast("Vendor/product rebuild queued", "success");
        }
      } catch (err) {
        showToast(err.message || String(err), "error");
      }
    });
  }
  if (resetCountersBtn) {
    resetCountersBtn.addEventListener("click", async () => {
      try {
        const payload = await apiFetch("/admin/api/dashboard/reset_failures", { method: "POST" });
        const since = payload && payload.counts_since ? formatTimestamp(payload.counts_since) : null;
        showToast(since ? `Counters reset (${since})` : "Counters reset", "success");
      } catch (err) {
        showToast(err.message || String(err), "error");
      }
    });
  }
  if (cancelBriefBtn) {
    cancelBriefBtn.addEventListener("click", async () => {
      try {
        await apiFetch("/admin/api/briefs/cancel-running", { method: "POST" });
        showToast("Running daily brief canceled", "success");
      } catch (err) {
        showToast(err.message || String(err), "error");
      }
    });
  }
  if (cancelRestartBriefBtn) {
    cancelRestartBriefBtn.addEventListener("click", async () => {
      try {
        await apiFetch("/admin/api/briefs/cancel-running-restart", { method: "POST" });
        showToast("Daily brief cancel/restart queued", "success");
      } catch (err) {
        showToast(err.message || String(err), "error");
      }
    });
  }
}
document.addEventListener("DOMContentLoaded", () => {
  wireNavDropdowns();
  wireEnqueueButtons();
  wireDashboard();
  wireUtilities();
  wireSources();
  wireJobs();
  wireLogin();
  wireRuntimeConfig();
  wirePersonalization();
  wireAnalytics();
  wireAiProviders();
  wireAiModels();
  wireAiPrompts();
  wireAiSchemas();
  wireAiProfiles();
  wireAiRouting();
  wireAiStageControls();
  wireAiTest();
  wireCveSearch();
  wireCveDetail();
  wireCveSettings();
  wireScheduleSettings();
  wireContentSearch();
  wireContentArticle();
  wireThreatsList();
  wireThreatDetail();
  wireBriefsList();
  wireBriefDetail();
  wireEvents();
  wireEventDetail();
  wireProducts();
  wireProductDetail();
  wireDangerZone();
  wireDebug();
  wireLogs();
  wireWatchlist();
  wireCopyButtons();
  wireActionMenus();
  applyTimestampFormatting();
  applyShortIds();
});
