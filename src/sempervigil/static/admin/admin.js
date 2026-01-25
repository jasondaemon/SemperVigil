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
  if (!refresh) {
    return;
  }
  refresh.addEventListener("click", () => {
    window.location.reload();
  });
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
  const field = document.getElementById("runtime-config-json");
  const error = document.getElementById("runtime-config-error");
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    error.style.display = "none";
    let payload;
    try {
      payload = JSON.parse(field.value);
    } catch (err) {
      error.textContent = "Invalid JSON";
      error.style.display = "block";
      return;
    }
    try {
      await apiFetch("/admin/config/runtime", {
        method: "PUT",
        body: JSON.stringify({ config: payload }),
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
  const pageSize = 50;
  let currentPage = 1;

  async function load(page) {
    currentPage = page;
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
    load(1).catch((err) => alert(err));
  });

  load(currentPage).catch((err) => alert(err));
}

function wireCveDetail() {
  const container = document.getElementById("cve-detail");
  if (!container) {
    return;
  }
  const cveId = container.dataset.cveId;
  apiFetch(`/admin/api/cves/${cveId}`)
    .then((item) => {
      container.innerHTML = `
        <div class="kv">
          <div><strong>${item.cve_id}</strong></div>
          <div>Published: ${item.published_at || ""}</div>
          <div>Modified: ${item.last_modified_at || ""}</div>
          <div>Last seen: ${item.last_seen_at || ""}</div>
          <div>Severity: ${item.preferred_base_severity || ""}</div>
          <div>CVSS: ${item.preferred_base_score || ""}</div>
          <div>Vector: ${item.preferred_vector || ""}</div>
        </div>
        <h3>Description</h3>
        <p>${item.description_text || ""}</p>
        <h3>Affected Products</h3>
        <pre class="mono">${(item.affected_products || []).join("\\n")}</pre>
        <h3>Affected CPEs</h3>
        <pre class="mono">${(item.affected_cpes || []).join("\\n")}</pre>
        <h3>Reference Domains</h3>
        <pre class="mono">${(item.reference_domains || []).join("\\n")}</pre>
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
        alert(err);
      }
    });
  }

  load().catch((err) => alert(err));
}

function wireContentSearch() {
  const form = document.getElementById("content-search-form");
  const table = document.getElementById("content-table");
  if (!form || !table) {
    return;
  }
  const tbody = table.querySelector("tbody");
  const pager = document.getElementById("content-pager");
  const pageSize = 50;
  let currentPage = 1;

  async function load(page) {
    currentPage = page;
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
    pager.textContent = `Page ${data.page} of ${Math.max(
      1,
      Math.ceil(data.total / data.page_size)
    )}`;
  }

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    load(1).catch((err) => alert(err));
  });

  load(currentPage).catch((err) => alert(err));
}

function wireContentArticle() {
  const container = document.getElementById("article-detail");
  if (!container) {
    return;
  }
  const articleId = container.dataset.articleId;
  apiFetch(`/admin/api/content/articles/${articleId}`)
    .then((item) => {
      container.innerHTML = `
        <div class="kv">
          <div><strong>${item.title || ""}</strong></div>
          <div>Source: ${item.source_id || ""}</div>
          <div>Published: ${item.published_at || ""}</div>
          <div>Ingested: ${item.ingested_at || ""}</div>
          <div><a href="${item.original_url}" target="_blank" rel="noopener">Open URL</a></div>
        </div>
        <h3>Summary</h3>
        <pre class="mono">${item.summary_llm || ""}</pre>
        <h3>Content</h3>
        <pre class="mono">${item.content_text || ""}</pre>
      `;
    })
    .catch((err) => {
      container.textContent = err.message || String(err);
    });
}
async function wireAnalytics() {
  const chartEl = document.getElementById("articles-chart");
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
          alert(err);
        }
      });
    }
  } catch (err) {
    alert(err);
  }
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
  wireCveSearch();
  wireCveDetail();
  wireCveSettings();
  wireContentSearch();
  wireContentArticle();
});
