async function apiFetch(url, options = {}) {
  const headers = Object.assign(
    { "Content-Type": "application/json" },
    options.headers || {}
  );
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
  document.querySelectorAll(".brief-today").forEach((btn) => {
    btn.addEventListener("click", async () => {
      try {
        await apiFetch("/admin/briefs/build", {
          method: "POST",
          body: JSON.stringify({}),
        });
        showToast("Brief job enqueued");
      } catch (err) {
        alert(err);
      }
    });
  });
}

function wireSources() {
  const form = document.getElementById("source-form");
  if (!form) {
    return;
  }
  const table = document.getElementById("sources-table");
  const tbody = table ? table.querySelector("tbody") : null;
  const idField = document.getElementById("source-id");
  const nameField = document.getElementById("source-name");
  const kindField = document.getElementById("source-kind");
  const urlField = document.getElementById("source-url");
  const intervalField = document.getElementById("source-interval");
  const tagsField = document.getElementById("source-tags");
  const enabledField = document.getElementById("source-enabled");
  const resetBtn = document.getElementById("source-reset");

  function resetForm() {
    idField.value = "";
    nameField.value = "";
    kindField.value = "rss";
    urlField.value = "";
    intervalField.value = "60";
    tagsField.value = "";
    enabledField.checked = true;
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
      row.innerHTML = `
        <td><input type="checkbox" class="toggle-enabled" ${source.enabled ? "checked" : ""}></td>
        <td class="source-name">${source.name}</td>
        <td class="source-kind">${source.kind || ""}</td>
        <td class="source-url mono wrap">${source.url || ""}</td>
        <td class="source-interval">${source.interval_minutes}</td>
        <td>
          ${source.last_error ? '<span class="status-pill status-error">Error</span>' : source.last_ok_at ? '<span class="status-pill status-ok">OK</span>' : '<span class="status-pill">Unknown</span>'}
        </td>
        <td>${source.articles_24h || 0}</td>
        <td>${source.accepted_last_run || 0}</td>
        <td class="source-tags">${(source.tags || []).join(", ")}</td>
        <td>${source.last_ok_at || ""}</td>
        <td class="truncate" title="${source.last_error || ""}">${source.last_error || ""}</td>
        <td class="table-actions">
          <button class="btn small test-source" type="button">Test</button>
          <button class="btn small secondary history-source" type="button">History</button>
          <button class="btn small secondary edit-source" type="button">Edit</button>
          <button class="btn small danger delete-source" type="button">Delete</button>
        </td>
      `;
      const testRow = document.createElement("tr");
      testRow.className = "test-result";
      testRow.dataset.sourceId = source.id;
      testRow.style.display = "none";
      testRow.innerHTML = `
        <td colspan="12">
          <div class="test-summary"></div>
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
      historyRow.innerHTML = `<td colspan="12"><div class="history-table"></div></td>`;
      tbody.appendChild(row);
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
      id: idField.value.trim() || undefined,
      name: nameField.value.trim(),
      kind: kindField.value,
      url: urlField.value.trim(),
      interval_minutes: parseInt(intervalField.value, 10),
      tags: tagsField.value.trim(),
      enabled: enabledField.checked,
    };
    const hasId = Boolean(idField.value);
    const target = hasId ? `/sources/${idField.value}` : "/sources";
    const method = hasId ? "PUT" : "POST";
    try {
      await apiFetch(target, {
        method,
        body: JSON.stringify(payload),
      });
      if (!hasId) {
        resetForm();
      }
      showToast("Source saved");
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
        idField.value = sourceId;
        nameField.value = row.querySelector(".source-name").textContent.trim();
        kindField.value = row.querySelector(".source-kind").textContent.trim() || "rss";
        urlField.value = row.querySelector(".source-url").textContent.trim();
        intervalField.value = row.querySelector(".source-interval").textContent.trim() || "60";
        tagsField.value = row.querySelector(".source-tags").textContent.trim();
        enabledField.checked = row.querySelector(".toggle-enabled").checked;
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

      if (target.classList.contains("test-source")) {
        const outputRow = document.querySelector(
          `tr.test-result[data-source-id="${sourceId}"]`
        );
        try {
          const result = await apiFetch(`/sources/${sourceId}/test`, { method: "POST" });
          const summary = `status=${result.status} http=${result.http_status || ""} found=${result.found_count} accepted=${result.accepted_count}`;
          const items = (result.items || [])
            .map((item) => `- ${item.title || ""} ${item.url || ""}`)
            .join("\n");
          outputRow.querySelector(".test-summary").textContent = summary;
          outputRow.querySelector(".test-raw").textContent = items || "No items";
          outputRow.style.display = "table-row";
        } catch (err) {
          alert(err);
        }
        return;
      }

      if (target.classList.contains("history-source")) {
        const outputRow = document.querySelector(
          `tr.history-result[data-source-id="${sourceId}"]`
        );
        try {
          const result = await apiFetch(`/sources/${sourceId}/health?limit=20`);
          const rows = result
            .map(
              (item) =>
                `<tr>
                  <td>${item.ts}</td>
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

    tbody.addEventListener("change", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement)) {
        return;
      }
      if (!target.classList.contains("toggle-enabled")) {
        return;
      }
      const row = target.closest("tr");
      const sourceId = row.dataset.sourceId;
      try {
        await apiFetch(`/sources/${sourceId}`, {
          method: "PATCH",
          body: JSON.stringify({ enabled: target.checked }),
        });
        showToast("Source updated");
      } catch (err) {
        alert(err);
        target.checked = !target.checked;
      }
    });
  }
}

function wireJobs() {
  const refresh = document.getElementById("jobs-refresh");
  const table = document.getElementById("jobs-table");
  const tbody = document.getElementById("jobs-table-body");
  if (!refresh || !table || !tbody) {
    return;
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
      const row = document.createElement("tr");
      row.innerHTML = `
        <td class="mono">${job.id}</td>
        <td>${job.job_type}</td>
        <td>${job.status}</td>
        <td>${job.requested_at || ""}</td>
        <td>${job.started_at || ""}</td>
        <td>${job.finished_at || ""}</td>
        <td class="truncate" title="${formatResult(job)}">${formatResult(job)}</td>
      `;
      tbody.appendChild(row);
    });
  }

  async function refreshJobs() {
    const jobs = await apiFetch("/jobs");
    renderRows(jobs);
    return jobs;
  }

  refresh.addEventListener("click", () => {
    refreshJobs().catch((err) => alert(err));
  });

  let polling = false;
  async function poll() {
    if (polling) {
      return;
    }
    polling = true;
    try {
      const jobs = await refreshJobs();
      const running = jobs.some(
        (job) =>
          job.job_type === "build_site" && (job.status === "queued" || job.status === "running")
      );
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
    setValue("paths-state-db", cfg.paths?.state_db);
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
    nextConfig.paths.state_db = document.getElementById("paths-state-db").value.trim();
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
      window.location.reload();
    } catch (err) {
      alert(err);
    }
  });

  document.querySelectorAll(".edit-prompt").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      idField.value = row.dataset.promptId;
      nameField.value = row.querySelector(".prompt-name").textContent.trim();
      versionField.value = row.querySelector(".prompt-version").textContent.trim();
    });
  });

  document.querySelectorAll(".delete-prompt").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const promptId = row.dataset.promptId;
      if (!confirm("Delete prompt?")) {
        return;
      }
      try {
        await apiFetch(`/admin/ai/prompts/${promptId}`, { method: "DELETE" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
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
    const inScope = document.getElementById("cve-in-scope").checked;

    const params = new URLSearchParams();
    if (query) params.set("query", query);
    if (severities.length) params.set("severity", severities.join(","));
    if (minCvss) params.set("min_cvss", minCvss);
    if (after) params.set("after", after);
    if (before) params.set("before", before);
    if (inScope) params.set("in_scope", "true");
    params.set("page", String(page));
    params.set("page_size", String(pageSize));

    const data = await apiFetch(`/admin/api/cves?${params.toString()}`);
    tbody.innerHTML = "";
    data.items.forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td><a href="/ui/cves/${item.cve_id}">${item.cve_id}</a></td>
        <td>${item.published_at || ""}</td>
        <td>${item.last_modified_at || ""}</td>
        <td>${item.preferred_base_severity || ""}</td>
        <td>${item.preferred_base_score || ""}</td>
        <td class="truncate" title="${item.summary || ""}">${item.summary || ""}</td>
        <td>${item.in_scope ? "yes" : "no"}</td>
      `;
      tbody.appendChild(row);
    });
    pager.textContent = `Page ${data.page} of ${Math.max(
      1,
      Math.ceil(data.total / data.page_size)
    )}`;
  }

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
}

function wireCveDetail() {
  const container = document.getElementById("cve-detail");
  if (!container) {
    return;
  }
  const cveId = container.dataset.cveId;
  apiFetch(`/admin/api/cves/${cveId}`)
    .then((item) => {
      const preferredVersion = item.preferred_cvss_version || "unknown";
      const v31 = item.cvss_v31 || null;
      const v40 = item.cvss_v40 || null;
      const products = item.affected_products || [];
      const cpes = item.affected_cpes || [];
      const domains = item.reference_domains || [];
      container.innerHTML = `
        <div class="kv">
          <div><strong>${item.cve_id}</strong></div>
          <div>Published: ${item.published_at || ""}</div>
          <div>Modified: ${item.last_modified_at || ""}</div>
          <div>Last seen: ${item.last_seen_at || ""}</div>
          <div>Preferred CVSS (${preferredVersion}): ${item.preferred_base_score || ""} ${
        item.preferred_base_severity ? `(${item.preferred_base_severity})` : ""
      }</div>
          <div>Preferred Vector: ${item.preferred_vector || ""}</div>
        </div>
        <h3>CVSS Versions</h3>
        <div class="kv">
          <div>CVSS v3.1: ${
            v31 ? `${v31.baseScore || ""} ${v31.baseSeverity || ""} ${v31.vectorString || ""}` : "None"
          }</div>
          <div>CVSS v4.0: ${
            v40 ? `${v40.baseScore || ""} ${v40.baseSeverity || ""} ${v40.vectorString || ""}` : "None"
          }</div>
        </div>
        <h3>Description</h3>
        <p>${item.description_text || ""}</p>
        <h3>Affected Products</h3>
        <pre class="mono">${products.length ? products.join("\\n") : "None found"}</pre>
        <h3>Affected CPEs</h3>
        <pre class="mono">${cpes.length ? cpes.join("\\n") : "None found"}</pre>
        <h3>Reference Domains</h3>
        <pre class="mono">${domains.length ? domains.join("\\n") : "None found"}</pre>
      `;
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
      note.textContent = `Last run: ${settings.last_run_at || "unknown"}`;
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

  load().catch((err) => {
    error.textContent = err.message || String(err);
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
  let pageSize = parseInt(document.getElementById("content-page-size").value, 10);
  let currentPage = 1;
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

  async function load(page) {
    currentPage = page;
    setError("");
    const params = new URLSearchParams();
    const query = document.getElementById("content-query").value.trim();
    const type = document.getElementById("content-type").value;
    const source = document.getElementById("content-source").value;
    const hasSummary = document.getElementById("content-has-summary").value;
    const tags = document.getElementById("content-tags").value.trim();
    const severity = document.getElementById("content-severity").value;
    const minCvss = document.getElementById("content-min-cvss").value;
    const after = document.getElementById("content-after").value;
    const before = document.getElementById("content-before").value;

    if (query) params.set("query", query);
    if (type) params.set("type", type);
    if (source) params.set("source_id", source);
    if (hasSummary) params.set("has_summary", hasSummary);
    if (tags) params.set("tags", tags);
    if (severity) params.set("severity", severity);
    if (minCvss) params.set("min_cvss", minCvss);
    if (after) params.set("after", after);
    if (before) params.set("before", before);
    params.set("page", String(page));
    params.set("page_size", String(pageSize));

    const data = await apiFetch(`/admin/api/content/search?${params.toString()}`);
    tbody.innerHTML = "";
    data.items.forEach((item) => {
      const row = document.createElement("tr");
      const date = item.published_at || item.last_modified_at || item.ingested_at || "";
      const title = item.title || item.summary || "";
      let link = "";
      if (item.type === "article") {
        link = `/ui/content/articles/${item.id}`;
      } else if (item.type === "cve") {
        link = `/ui/cves/${item.cve_id}`;
      }
      row.innerHTML = `
        <td>${item.type}</td>
        <td>${link ? `<a href="${link}">${item.type === "cve" ? item.cve_id : item.id}</a>` : ""}</td>
        <td>${date}</td>
        <td class="truncate" title="${title}">${title}</td>
        <td>${item.source_name || ""}</td>
      `;
      tbody.appendChild(row);
    });
    renderPager(data.total, data.page, data.page_size);
  }

  document.getElementById("content-page-size").addEventListener("change", () => {
    pageSize = parseInt(document.getElementById("content-page-size").value, 10);
    load(1).catch((err) => setError(err.message || String(err)));
  });

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    load(1).catch((err) => setError(err.message || String(err)));
  });

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
}

function wireContentArticle() {
  const container = document.getElementById("article-detail");
  if (!container) {
    return;
  }
  const articleId = container.dataset.articleId;
  apiFetch(`/admin/api/content/articles/${articleId}`)
    .then((item) => {
      const summary = item.summary_llm || item.summary || "";
      const content = item.content_text || "";
      const htmlExcerpt = item.content_html_excerpt || "";
      const error = item.content_error || "";
      container.innerHTML = `
        <div class="kv">
          <div><strong>${item.title || ""}</strong></div>
          <div>Source: ${item.source_id || ""}</div>
          <div>Published: ${item.published_at || ""}</div>
          <div>Ingested: ${item.ingested_at || ""}</div>
          <div><a href="${item.original_url}" target="_blank" rel="noopener">Open URL</a></div>
        </div>
        <h3>Summary</h3>
        <pre class="mono wrap-pre">${summary || "No summary available."}</pre>
        <h3>Content</h3>
        <pre class="mono wrap-pre">${content || "No extracted content available."}</pre>
        ${htmlExcerpt ? `<h3>HTML Excerpt</h3><pre class="mono wrap-pre">${htmlExcerpt}</pre>` : ""}
        ${error ? `<p class="error">Content error: ${error}</p>` : ""}
      `;
    })
    .catch((err) => {
      container.textContent = err.message || String(err);
    });
}

function wireEvents() {
  const table = document.getElementById("events-table");
  if (!table) {
    return;
  }
  const tbody = table.querySelector("tbody");
  const pager = document.getElementById("events-pager");
  const error = document.getElementById("events-error");
  const form = document.getElementById("events-filters");
  const rebuildBtn = document.getElementById("events-rebuild");
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
    const query = document.getElementById("events-query").value.trim();
    const kind = document.getElementById("events-kind").value;
    const severity = document.getElementById("events-severity").value;
    const status = document.getElementById("events-status").value;
    const after = document.getElementById("events-after").value;
    const before = document.getElementById("events-before").value;
    if (query) params.set("query", query);
    if (kind) params.set("kind", kind);
    if (severity) params.set("severity", severity);
    if (status) params.set("status", status);
    if (after) params.set("after", after);
    if (before) params.set("before", before);
    params.set("page", String(page));
    params.set("page_size", String(pageSize));
    const data = await apiFetch(`/admin/api/events?${params.toString()}`);
    tbody.innerHTML = "";
    data.items.forEach((event) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td><a href="/ui/events/${event.id}">${event.id}</a></td>
        <td class="truncate" title="${event.title || ""}">${event.title || ""}</td>
        <td>${event.kind || ""}</td>
        <td>${event.severity || ""}</td>
        <td>${event.status || ""}</td>
        <td>${event.last_seen_at || ""}</td>
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

  apiFetch(`/admin/api/events/${eventId}`)
    .then((event) => {
      const meta = `
        <div class="meta-grid">
          <div><strong>ID:</strong> ${event.id}</div>
          <div><strong>Kind:</strong> ${event.kind}</div>
          <div><strong>Status:</strong> ${event.status}</div>
          <div><strong>Severity:</strong> ${event.severity || "UNKNOWN"}</div>
          <div><strong>First seen:</strong> ${event.first_seen_at || ""}</div>
          <div><strong>Last seen:</strong> ${event.last_seen_at || ""}</div>
        </div>
        ${event.summary ? `<p class="summary">${event.summary}</p>` : ""}
      `;
      container.innerHTML = meta;
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
            <td>${cve.published_at || ""}</td>
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
          row.innerHTML = `
            <td>${link ? `<a href="${link}">${article.title || ""}</a>` : (article.title || "")}</td>
            <td>${article.published_at || ""}</td>
            <td>${article.url ? `<a href="${article.url}" target="_blank" rel="noopener">Source</a>` : ""}</td>
          `;
          body.appendChild(row);
        });
      }
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
    data.items.forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${item.vendor_name || ""}</td>
        <td><a href="/ui/products/${item.product_key}">${item.product_name || ""}</a></td>
        <td class="mono">${item.product_key || ""}</td>
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
          <td>${cve.published_at || ""}</td>
          <td class="truncate" title="${cve.summary || ""}">${cve.summary || ""}</td>
        `;
        body.appendChild(row);
      });
      renderPager(cvePager, data.total, data.page, data.page_size, loadCves);
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
          <td>${event.last_seen_at || ""}</td>
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
}
async function wireAnalytics() {
  const chartEl = document.getElementById("articles-chart");
  const error = document.getElementById("analytics-error");
  if (!chartEl || !window.Chart) {
    return;
  }
  try {
    const data = await apiFetch("/admin/analytics/articles_per_day?days=30");
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
    const table = document.querySelector("#source-stats tbody");
    if (table) {
      table.innerHTML = stats.data
        .map(
          (row) =>
            `<tr>
              <td>${row.source_name}</td>
              <td>${row.articles_per_day_avg}</td>
              <td>${row.last_ok_at || ""}</td>
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
          await apiFetch("/admin/briefs/build", {
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
    } catch (err) {
      if (output) {
        output.textContent = err.message || String(err);
      }
    }
  });
}

document.addEventListener("DOMContentLoaded", () => {
  wireNavDropdowns();
  wireEnqueueButtons();
  wireSources();
  wireJobs();
  wireLogin();
  wireRuntimeConfig();
  wireAnalytics();
  wireAiProviders();
  wireAiModels();
  wireAiPrompts();
  wireAiSchemas();
  wireAiProfiles();
  wireAiRouting();
  wireAiTest();
  wireCveSearch();
  wireCveDetail();
  wireCveSettings();
  wireContentSearch();
  wireContentArticle();
  wireEvents();
  wireEventDetail();
  wireProducts();
  wireProductDetail();
  wireDangerZone();
});
